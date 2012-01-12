/*-------------------------------------------------------------------------
 *
 * parallel.c
 *
 *	Parallel support for the pg_dump archiver
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	The author is not responsible for loss or damages that may
 *	result from its use.
 *
 * IDENTIFICATION
 *		src/bin/pg_dump/parallel.c
 *
 *-------------------------------------------------------------------------
 */

#include "pg_backup_db.h"

#include "dumpmem.h"
#include "dumputils.h"
#include "parallel.h"

#ifndef WIN32
#include <sys/types.h>
#include <sys/wait.h>
#include "signal.h"
#include <unistd.h>
#include <fcntl.h>
#endif

#define PIPE_READ							0
#define PIPE_WRITE							1
#define SHUTDOWN_GRACE_PERIOD				(500)

/* file-scope variables */
#ifdef WIN32
static unsigned int	tMasterThreadId = 0;
static HANDLE		termEvent = INVALID_HANDLE_VALUE;
#else
static volatile sig_atomic_t wantAbort = 0;
static bool aborting = false;
#endif

/*
 * The parallel error handler is called for any die_horribly() in a child or master process.
 * It then takes control over shutting down the rest of the gang.
 */
void (*volatile vparallel_error_handler)(ArchiveHandle *AH, const char *modulename,
								const char *fmt, va_list ap)
						__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 0))) = NULL;

/* The actual implementation of the error handler function */
static void vparallel_error_handler_imp(ArchiveHandle *AH, const char *modulename,
										const char *fmt, va_list ap)
								__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 0)));

static const char *modulename = gettext_noop("parallel archiver");

static int ShutdownConnection(PGconn **conn);

static void WaitForTerminatingWorkers(ArchiveHandle *AH, ParallelState *pstate);
static void ShutdownWorkersHard(ArchiveHandle *AH, ParallelState *pstate);
static void ShutdownWorkersSoft(ArchiveHandle *AH, ParallelState *pstate, bool do_wait);
static void PrintStatus(ParallelState *pstate);
static bool HasEveryWorkerTerminated(ParallelState *pstate);

static void lockTableNoWait(ArchiveHandle *AH, TocEntry *te);
static void WaitForCommands(ArchiveHandle *AH, int pipefd[2]);
static char *getMessageFromMaster(ArchiveHandle *AH, int pipefd[2]);
static void sendMessageToMaster(ArchiveHandle *AH, int pipefd[2], const char *str);
static char *getMessageFromWorker(ArchiveHandle *AH, ParallelState *pstate,
								  bool do_wait, int *worker);
static void sendMessageToWorker(ArchiveHandle *AH, ParallelState *pstate,
							    int worker, const char *str);
static char *readMessageFromPipe(int fd, bool do_wait);

static void SetupWorker(ArchiveHandle *AH, int pipefd[2], int worker,
						RestoreOptions *ropt);

#define messageStartsWith(msg, prefix) \
	(strncmp(msg, prefix, strlen(prefix)) == 0)
#define messageEquals(msg, pattern) \
	(strcmp(msg, pattern) == 0)

/* architecture dependent #defines */
#ifdef WIN32
	/* WIN32 */
	/* pgpipe implemented in src/backend/port/pipe.c */
	#define setnonblocking(fd) \
		do { u_long mode = 1; \
			 ioctlsocket((fd), FIONBIO, &mode); \
		} while(0);
	#define setblocking(fd) \
		do { u_long mode = 0; \
			 ioctlsocket((fd), FIONBIO, &mode); \
		} while(0);
#else /* UNIX */
	#define setnonblocking(fd) \
		do { long flags = (long) fcntl((fd), F_GETFL, 0); \
			fcntl(fd, F_SETFL, flags | O_NONBLOCK); \
		} while(0);
	#define setblocking(fd) \
		do { long flags = (long) fcntl((fd), F_GETFL, 0); \
			fcntl(fd, F_SETFL, flags & ~O_NONBLOCK); \
		} while(0);
#endif

#ifdef WIN32
/*
 * On Windows, source in the pgpipe implementation from the backend and provide
 * an own error reporting routine, the backend usually uses ereport() for that.
 */
static void pgdump_pgpipe_ereport(const char* fmt, ...);
#define PGPIPE_EREPORT pgdump_pgpipe_ereport
#include "../../backend/port/pipe.c"
static void
pgdump_pgpipe_ereport(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vwrite_msg("pgpipe", fmt, args);
	va_end(args);
}
#endif

static int
#ifdef WIN32
GetSlotOfThread(ParallelState *pstate, unsigned int threadId)
#else
GetSlotOfProcess(ParallelState *pstate, pid_t pid)
#endif
{
	int i;

	for (i = 0; i < pstate->numWorkers; i++)
#ifdef WIN32
		if (pstate->parallelSlot[i].threadId == threadId)
#else
		if (pstate->parallelSlot[i].pid == pid)
#endif
			return i;

	Assert(false);
	return NO_SLOT;
}

enum escrow_action { GET, SET };
static void
parallel_error_handler_escrow_data(enum escrow_action act, ParallelState *pstate)
{
	static ParallelState *s_pstate = NULL;

	if (act == SET)
		s_pstate = pstate;
	else
		*pstate = *s_pstate;
}

static void
vparallel_error_handler_imp(ArchiveHandle *AH,
							const char *modulename,
							const char *fmt, va_list ap)
{
	ParallelState pstate;
	char		buf[512];
	int			pipefd[2];
	int			i;

	if (AH->is_clone)
	{
		/* we are the child, get the message out to the parent */
		parallel_error_handler_escrow_data(GET, &pstate);
#ifdef WIN32
		i = GetSlotOfThread(&pstate, GetCurrentThreadId());
#else
		i = GetSlotOfProcess(&pstate, getpid());
#endif
		if (pstate.parallelSlot[i].inErrorHandling)
			return;

		pstate.parallelSlot[i].inErrorHandling = true;

		pipefd[PIPE_READ] = pstate.parallelSlot[i].pipeRevRead;
		pipefd[PIPE_WRITE] = pstate.parallelSlot[i].pipeRevWrite;

		strcpy(buf, "ERROR ");
		vsnprintf(buf + strlen("ERROR "), sizeof(buf) - strlen("ERROR "), fmt, ap);

		sendMessageToMaster(AH, pipefd, buf);
		if (AH->connection)
			ShutdownConnection(&(AH->connection));
#ifdef WIN32
		ExitThread(1);
#else
		exit(1);
#endif
	}
	else
	{
#ifndef WIN32
		/*
		 * We are the parent. We need the handling variable to see if we're
		 * already handling an error.
		 */
		if (aborting)
			return;
		aborting = 1;

		signal(SIGPIPE, SIG_IGN);
#endif
		/*
		 * Note that technically we're using a new pstate here, the old one
		 * is copied over and then the old one isn't updated anymore. Only
		 * the new one is, which is okay because control will never return
		 * from this function.
		 */
		parallel_error_handler_escrow_data(GET, &pstate);
		ShutdownWorkersHard(AH, &pstate);
		/* Terminate own connection */
		if (AH->connection)
			ShutdownConnection(&(AH->connection));
		vwrite_msg(NULL, fmt, ap);
		exit(1);
	}
	Assert(false);
}

/*
 * If we have one worker that terminates for some reason, we'd like the other
 * threads to terminate as well (and not finish with their 70 GB table dump
 * first...). Now in UNIX we can just kill these processes, and let the signal
 * handler set wantAbort to 1 or more. In Windows we set a termEvent and this
 * serves as the signal for everyone to terminate.
 */
void
checkAborting(ArchiveHandle *AH)
{
#ifdef WIN32
	if (WaitForSingleObject(termEvent, 0) == WAIT_OBJECT_0)
#else
	if (wantAbort)
#endif
		/*
		 * Terminate, this error will actually never show up somewhere
		 * because if termEvent/wantAbort is set, then we are already in the
		 * process of going down and already have a reason why we're
		 * terminating.
		 */
		die_horribly(AH, modulename, "worker is terminating");
}

/*
 * A select loop that repeats calling select until a descriptor in the read set
 * becomes readable. On Windows we have to check for the termination event from
 * time to time, on Unix we can just block forever.
 */
#ifdef WIN32
static int
select_loop(int maxFd, fd_set *workerset)
{
	int			i;
	fd_set		saveSet = *workerset;

	/* should always be the master */
	Assert(tMasterThreadId == GetCurrentThreadId());

	for (;;)
	{
		/*
		 * sleep a quarter of a second before checking if we should
		 * terminate.
		 */
		struct timeval tv = { 0, 250000 };
		*workerset = saveSet;
		i = select(maxFd + 1, workerset, NULL, NULL, &tv);

		if (i == SOCKET_ERROR && WSAGetLastError() == WSAEINTR)
			continue;
		if (i)
			break;
	}

	return i;
}
#else /* UNIX */
static int
select_loop(int maxFd, fd_set *workerset)
{
	int		i;

	fd_set saveSet = *workerset;
	for (;;)
	{
		*workerset = saveSet;
		i = select(maxFd + 1, workerset, NULL, NULL, NULL);
		Assert(i != 0);
		if (wantAbort && !aborting) {
			return NO_SLOT;
		}
		if (i < 0 && errno == EINTR)
			continue;
		break;
	}

	return i;
}
#endif

/*
 * Shut down any remaining workers, this has an implicit do_wait == true
 */
static void
ShutdownWorkersHard(ArchiveHandle *AH, ParallelState *pstate)
{
#ifdef WIN32
	/* The workers monitor this event via checkAborting(). */
	SetEvent(termEvent);
#endif
	/*
	 * The fastest way we can make them terminate is when they are listening
	 * for new commands and we just tell them to terminate.
	 */
	ShutdownWorkersSoft(AH, pstate, false);

#ifndef WIN32
	{
		int i;
		for (i = 0; i < pstate->numWorkers; i++)
			kill(pstate->parallelSlot[i].pid, SIGTERM);

		/* Reset our signal handler, if we get signaled again, terminate normally */
		signal(SIGINT, SIG_DFL);
		signal(SIGTERM, SIG_DFL);
		signal(SIGQUIT, SIG_DFL);
	}
#endif

	WaitForTerminatingWorkers(AH, pstate);
}

static void
WaitForTerminatingWorkers(ArchiveHandle *AH, ParallelState *pstate)
{
	while (!HasEveryWorkerTerminated(pstate))
	{
		int			worker;
		char	   *msg;

		PrintStatus(pstate);

		msg = getMessageFromWorker(AH, pstate, true, &worker);
		if (!msg || messageStartsWith(msg, "ERROR "))
			pstate->parallelSlot[worker].workerStatus = WRKR_TERMINATED;
		if (msg)
			free(msg);
	}
	Assert(HasEveryWorkerTerminated(pstate));
}

#ifndef WIN32
/* Signal handling (UNIX only) */
static void
sigTermHandler(int signum)
{
	wantAbort++;
}
#endif

/*
 * This function is called by both UNIX and Windows variants to set up a
 * worker process.
 */
static void
SetupWorker(ArchiveHandle *AH, int pipefd[2], int worker,
			RestoreOptions *ropt)
{
	/*
	 * In dump mode (pg_dump) this calls _SetupWorker() as defined in
	 * pg_dump.c, while in restore mode (pg_restore) it calls _SetupWorker() as
	 * defined in pg_restore.c.
     *
	 * We get the raw connection only for the reason that we can close it
	 * properly when we shut down. This happens only that way when it is
	 * brought down because of an error.
	 */
	_SetupWorker((Archive *) AH, ropt);

	Assert(AH->connection != NULL);

	WaitForCommands(AH, pipefd);

	closesocket(pipefd[PIPE_READ]);
	closesocket(pipefd[PIPE_WRITE]);
}

#ifdef WIN32
/*
 * On Windows the _beginthreadex() function allows us to pass one parameter.
 * Since we need to pass a few values however, we define a structure here
 * and then pass a pointer to such a structure in _beginthreadex().
 */
typedef struct {
	ArchiveHandle  *AH;
	RestoreOptions *ropt;
	int				worker;
	int				pipeRead;
	int				pipeWrite;
} WorkerInfo;

static unsigned __stdcall
init_spawned_worker_win32(WorkerInfo *wi)
{
	ArchiveHandle *AH;
	int pipefd[2] = { wi->pipeRead, wi->pipeWrite };
	int worker = wi->worker;
	RestoreOptions *ropt = wi->ropt;

	AH = CloneArchive(wi->AH);

	free(wi);
	SetupWorker(AH, pipefd, worker, ropt);

	DeCloneArchive(AH);
	_endthreadex(0);
	return 0;
}
#endif

/*
 * This function starts the parallel dump or restore by spawning off the worker
 * processes in both Unix and Windows. For Windows, it creates a number of
 * threads while it does a fork() on Unix.
 */
ParallelState *
ParallelBackupStart(ArchiveHandle *AH, RestoreOptions *ropt)
{
	ParallelState  *pstate;
	int				i;
	const size_t	slotSize = AH->public.numWorkers * sizeof(ParallelSlot);

	Assert(AH->public.numWorkers > 0);

	/* Ensure stdio state is quiesced before forking */
	fflush(NULL);

	pstate = (ParallelState *) pg_malloc(sizeof(ParallelState));

	pstate->numWorkers = AH->public.numWorkers;
	pstate->parallelSlot = NULL;

	if (AH->public.numWorkers == 1)
		return pstate;

	pstate->parallelSlot = (ParallelSlot *) pg_malloc(slotSize);
	memset((void *) pstate->parallelSlot, 0, slotSize);

	parallel_error_handler_escrow_data(SET, pstate);
	vparallel_error_handler = vparallel_error_handler_imp;

#ifdef WIN32
	tMasterThreadId = GetCurrentThreadId();
	termEvent = CreateEvent(NULL, true, false, "Terminate");
#else
	signal(SIGTERM, sigTermHandler);
	signal(SIGINT, sigTermHandler);
	signal(SIGQUIT, sigTermHandler);
#endif

	for (i = 0; i < pstate->numWorkers; i++)
	{
#ifdef WIN32
		WorkerInfo *wi;
		uintptr_t	handle;
#else
		pid_t		pid;
#endif
		int			pipeMW[2], pipeWM[2];

		if (pgpipe(pipeMW) < 0 || pgpipe(pipeWM) < 0)
			die_horribly(AH, modulename, "Cannot create communication channels: %s",
						 strerror(errno));

		pstate->parallelSlot[i].workerStatus = WRKR_IDLE;
#ifdef WIN32
		/* Allocate a new structure for every worker */
		wi = (WorkerInfo *) pg_malloc(sizeof(WorkerInfo));

		wi->ropt = ropt;
		wi->worker = i;
		wi->AH = AH;
		wi->pipeRead = pstate->parallelSlot[i].pipeRevRead = pipeMW[PIPE_READ];
		wi->pipeWrite = pstate->parallelSlot[i].pipeRevWrite = pipeWM[PIPE_WRITE];

		handle = _beginthreadex(NULL, 0, &init_spawned_worker_win32,
								wi, 0, &(pstate->parallelSlot[i].threadId));
		pstate->parallelSlot[i].hThread = handle;
#else
		pid = fork();
		if (pid == 0)
		{
			/* we are the worker */
			int j;
			int pipefd[2] = { pipeMW[PIPE_READ], pipeWM[PIPE_WRITE] };

			/*
			 * Store the fds for the reverse communication in pstate. Actually
			 * we only use this in case of an error and don't use pstate
			 * otherwise in the worker process. On Windows we write to the
			 * global pstate, in Unix we write to our process-local copy but
			 * that's also where we'd retrieve this information back from.
			 */
			pstate->parallelSlot[i].pipeRevRead = pipefd[PIPE_READ];
			pstate->parallelSlot[i].pipeRevWrite = pipefd[PIPE_WRITE];
			pstate->parallelSlot[i].pid = getpid();

			/*
			 * Call CloneArchive on Unix as well even though technically we
			 * don't need to because fork() gives us a copy in our own address space
			 * already. But CloneArchive resets the state information, sets is_clone
			 * and also clones the database connection (for parallel dump)
			 * which all seems kinda helpful.
			 */
			AH = CloneArchive(AH);

#ifdef HAVE_SETSID
			/*
			 * If we can, we try to make each process the leader of its own
			 * process group. The reason is that if you hit Ctrl-C and they are
			 * all in the same process group, any termination sequence is
			 * possible, because every process will receive the signal. What
			 * often happens is that a worker receives the signal, terminates
			 * and the master detects that one of the workers had a problem,
			 * even before acting on its own signal. That's still okay because
			 * everyone still terminates but it looks a bit weird.
			 *
			 * With setsid() however, a Ctrl-C is only sent to the master and
			 * he can then cascade it to the worker processes.
			 */
			setsid();
#endif

			closesocket(pipeWM[PIPE_READ]);		/* close read end of Worker -> Master */
			closesocket(pipeMW[PIPE_WRITE]);	/* close write end of Master -> Worker */

			/*
			 * Close all inherited fds for communication of the master with
			 * the other workers.
			 */
			for (j = 0; j < i; j++)
			{
				closesocket(pstate->parallelSlot[j].pipeRead);
				closesocket(pstate->parallelSlot[j].pipeWrite);
			}

			SetupWorker(AH, pipefd, i, ropt);

			exit(0);
		}
		else if (pid < 0)
			/* fork failed */
			die_horribly(AH, modulename,
						 "could not create worker process: %s\n",
						 strerror(errno));

		/* we are the Master, pid > 0 here */
		Assert(pid > 0);
		closesocket(pipeMW[PIPE_READ]);		/* close read end of Master -> Worker */
		closesocket(pipeWM[PIPE_WRITE]);	/* close write end of Worker -> Master */

		pstate->parallelSlot[i].pid = pid;
#endif

		pstate->parallelSlot[i].pipeRead = pipeWM[PIPE_READ];
		pstate->parallelSlot[i].pipeWrite = pipeMW[PIPE_WRITE];

		pstate->parallelSlot[i].args = (ParallelArgs *) pg_malloc(sizeof(ParallelArgs));
		pstate->parallelSlot[i].args->AH = AH;
		pstate->parallelSlot[i].args->te = NULL;
		pstate->parallelSlot[i].workerStatus = WRKR_IDLE;
	}

	return pstate;
}

/*
 * Tell all of our workers to terminate.
 *
 * Pretty straightforward routine, first we tell everyone to terminate, then we
 * listen to the workers' replies and finally close the sockets that we have
 * used for communication.
 */
void
ParallelBackupEnd(ArchiveHandle *AH, ParallelState *pstate)
{
	int i;

	if (pstate->numWorkers == 1)
		return;

	PrintStatus(pstate);
	Assert(IsEveryWorkerIdle(pstate));

	/* no hard shutdown, let workers exit by themselves and wait for them */
	ShutdownWorkersSoft(AH, pstate, true);

	PrintStatus(pstate);

	for (i = 0; i < pstate->numWorkers; i++)
	{
		closesocket(pstate->parallelSlot[i].pipeRead);
		closesocket(pstate->parallelSlot[i].pipeWrite);
	}

	vparallel_error_handler = NULL;

	free(pstate->parallelSlot);
	free(pstate);
}


/*
 * The sequence is the following (for dump, similar for restore):
 *
 * Master                                   Worker
 *
 *                                          enters WaitForCommands()
 * DispatchJobForTocEntry(...te...)
 *
 * [ Worker is IDLE ]
 *
 * arg = (MasterStartParallelItemPtr)()
 * send: DUMP arg
 *                                          receive: DUMP arg
 *                                          str = (WorkerJobDumpPtr)(arg)
 * [ Worker is WORKING ]                    ... gets te from arg ...
 *                                          ... dump te ...
 *                                          send: OK DUMP info
 *
 * In ListenToWorkers():
 *
 * [ Worker is FINISHED ]
 * receive: OK DUMP info
 * status = (MasterEndParallelItemPtr)(info)
 *
 * In ReapWorkerStatus(&ptr):
 * *ptr = status;
 * [ Worker is IDLE ]
 */
void
DispatchJobForTocEntry(ArchiveHandle *AH, ParallelState *pstate, TocEntry *te,
					   T_Action act)
{
	int		worker;
	char   *arg;

	/* our caller makes sure that at least one worker is idle */
	Assert(GetIdleWorker(pstate) != NO_SLOT);
	worker = GetIdleWorker(pstate);
	Assert(worker != NO_SLOT);

	arg = (AH->MasterStartParallelItemPtr)(AH, te, act);

	sendMessageToWorker(AH, pstate, worker, arg);

	pstate->parallelSlot[worker].workerStatus = WRKR_WORKING;
	pstate->parallelSlot[worker].args->te = te;
	PrintStatus(pstate);
}

static void
PrintStatus(ParallelState *pstate)
{
	int			i;
	printf("------Status------\n");
	for (i = 0; i < pstate->numWorkers; i++)
	{
		printf("Status of worker %d: ", i);
		switch (pstate->parallelSlot[i].workerStatus)
		{
			case WRKR_IDLE:
				printf("IDLE");
				break;
			case WRKR_WORKING:
				printf("WORKING");
				break;
			case WRKR_FINISHED:
				printf("FINISHED");
				break;
			case WRKR_TERMINATED:
				printf("TERMINATED");
				break;
		}
		printf("\n");
	}
	printf("------------\n");
}


/*
 * Find the first free parallel slot (if any).
 */
int
GetIdleWorker(ParallelState *pstate)
{
	int			i;
	for (i = 0; i < pstate->numWorkers; i++)
		if (pstate->parallelSlot[i].workerStatus == WRKR_IDLE)
			return i;
	return NO_SLOT;
}

/*
 * Return true iff every worker process is in the WRKR_TERMINATED state.
 */
static bool
HasEveryWorkerTerminated(ParallelState *pstate)
{
	int			i;
	for (i = 0; i < pstate->numWorkers; i++)
		if (pstate->parallelSlot[i].workerStatus != WRKR_TERMINATED)
			return false;
	return true;
}

/*
 * Return true iff every worker is in the WRKR_IDLE state.
 */
bool
IsEveryWorkerIdle(ParallelState *pstate)
{
	int			i;
	for (i = 0; i < pstate->numWorkers; i++)
		if (pstate->parallelSlot[i].workerStatus != WRKR_IDLE)
			return false;
	return true;
}

/*
 * Performs a soft shutdown and optionally waits for every worker to terminate.
 * A soft shutdown sends a "TERMINATE" message to every worker only.
 */
static void
ShutdownWorkersSoft(ArchiveHandle *AH, ParallelState *pstate, bool do_wait)
{
	int			i;

	/* soft shutdown */
	for (i = 0; i < pstate->numWorkers; i++)
	{
		if (pstate->parallelSlot[i].workerStatus != WRKR_TERMINATED)
		{
			sendMessageToWorker(AH, pstate, i, "TERMINATE");
			pstate->parallelSlot[i].workerStatus = WRKR_WORKING;
		}
	}

	if (!do_wait)
		return;

	WaitForTerminatingWorkers(AH, pstate);
}

/*
 * This routine does some effort to gracefully shut down the database
 * connection, but not too much, since the parent is waiting for the workers to
 * terminate. The cancellation of the database connection is done in an
 * asynchronous way, so we need to wait a bit after sending PQcancel().
 * Calling PQcancel() first and then and PQfinish() immediately afterwards
 * would still cancel an active connection because most likely the PQfinish()
 * has not yet been processed.
 *
 * On Windows, when the master process terminates the childrens' database
 * connections it forks off new threads that do nothing else than close the
 * connection. These threads only live as long as they are in this function.
 * And since a thread needs to return a value this function needs to as well.
 * Hence this function returns an (unsigned) int.
 */
static int
ShutdownConnection(PGconn **conn)
{
	PGcancel   *cancel;
	char		errbuf[1];
	int			i;

	Assert(conn != NULL);
	Assert(*conn != NULL);

	if ((cancel = PQgetCancel(*conn)))
	{
		PQcancel(cancel, errbuf, sizeof(errbuf));
		PQfreeCancel(cancel);
	}

	/* give the server a little while */
	for (i = 0; i < 10; i++)
	{
		PQconsumeInput(*conn);
		if (!PQisBusy(*conn))
			break;
		pg_usleep((SHUTDOWN_GRACE_PERIOD / 10) * 1000);
	}

	PQfinish(*conn);
	*conn = NULL;
	return 0;
}

/*
 * One danger of the parallel backup is a possible deadlock:
 *
 * 1) Master dumps the schema and locks all tables in ACCESS SHARE mode.
 * 2) Another process requests an ACCESS EXCLUSIVE lock (which is not granted
 *    because the master holds a conflicting ACCESS SHARE lock).
 * 3) The worker process also requests an ACCESS SHARE lock to read the table.
 *    The worker's not granted that lock but is enqueued behind the ACCESS
 *    EXCLUSIVE lock request.
 *
 * Now what we do here is to just request a lock in ACCESS SHARE but with
 * NOWAIT in the worker prior to touching the table. If we don't get the lock,
 * then we know that somebody else has requested an ACCESS EXCLUSIVE lock and
 * are good to just fail the whole backup because we have detected a deadlock.
 */
static void
lockTableNoWait(ArchiveHandle *AH, TocEntry *te)
{
	const char *qualId;
	PQExpBuffer query = createPQExpBuffer();
	PGresult   *res;

	Assert(AH->format == archDirectory);
	Assert(strcmp(te->desc, "BLOBS") != 0);

	/*
	 * We are only locking tables and thus we can peek at the DROP command
	 * which contains the fully qualified name.
	 *
	 * Additionally, strlen("DROP") == strlen("LOCK").
	 */
	appendPQExpBuffer(query, "SELECT pg_namespace.nspname,"
							 "       pg_class.relname "
							 "  FROM pg_class "
							 "  JOIN pg_namespace on pg_namespace.oid = relnamespace "
							 " WHERE pg_class.oid = %d", te->catalogId.oid);

	res = PQexec(AH->connection, query->data);

	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
		die_horribly(AH, modulename, "could not get relation name for oid %d: %s",
					 te->catalogId.oid, PQerrorMessage(AH->connection));

	resetPQExpBuffer(query);

	qualId = fmtQualifiedId(PQgetvalue(res, 0, 0),
							PQgetvalue(res, 0, 1),
							AH->public.remoteVersion);

	appendPQExpBuffer(query, "LOCK TABLE %s IN ACCESS SHARE MODE NOWAIT", qualId);
	PQclear(res);

	res = PQexec(AH->connection, query->data);

	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
		die_horribly(AH, modulename, "could not obtain lock on relation \"%s\". This "
					 "usually means that someone requested an ACCESS EXCLUSIVE lock "
					 "on the table after the pg_dump parent process has gotten the "
					 "initial ACCESS SHARE lock on the table.", qualId);

	PQclear(res);
	destroyPQExpBuffer(query);
}

/*
 * That's the main routine for the worker.
 * When it starts up it enters this routine and waits for commands from the
 * master process. After having processed a command it comes back to here to
 * wait for the next command. Finally it will receive a TERMINATE command and
 * exit.
 */
static void
WaitForCommands(ArchiveHandle *AH, int pipefd[2])
{
	char	   *command;
	DumpId		dumpId;
	int			nBytes;
	char	   *str = NULL;
	TocEntry   *te;

	for(;;)
	{
		command = getMessageFromMaster(AH, pipefd);

		if (messageStartsWith(command, "DUMP "))
		{
			Assert(AH->format == archDirectory);
			sscanf(command + strlen("DUMP "), "%d%n", &dumpId, &nBytes);
			Assert(nBytes == strlen(command) - strlen("DUMP "));

			te = getTocEntryByDumpId(AH, dumpId);
			Assert(te != NULL);

			/*
			 * Lock the table but with NOWAIT. Note that the parent is already
			 * holding a lock. If we cannot acquire another ACCESS SHARE MODE
			 * lock, then somebody else has requested an exclusive lock in the
			 * meantime.  lockTableNoWait dies in this case to prevent a
			 * deadlock.
			 */
			if (strcmp(te->desc, "BLOBS") != 0)
				lockTableNoWait(AH, te);

			/*
			 * The message we return here has been pg_malloc()ed and we are
			 * responsible for free()ing it.
			 */
			str = (AH->WorkerJobDumpPtr)(AH, te);
			Assert(AH->connection != NULL);
			sendMessageToMaster(AH, pipefd, str);
			free(str);
		}
		else if (messageStartsWith(command, "RESTORE "))
		{
			Assert(AH->format == archDirectory || AH->format == archCustom);
			Assert(AH->connection != NULL);

			sscanf(command + strlen("RESTORE "), "%d%n", &dumpId, &nBytes);
			Assert(nBytes == strlen(command) - strlen("RESTORE "));

			te = getTocEntryByDumpId(AH, dumpId);
			Assert(te != NULL);
			/*
			 * The message we return here has been pg_malloc()ed and we are
			 * responsible for free()ing it.
			 */
			str = (AH->WorkerJobRestorePtr)(AH, te);
			Assert(AH->connection != NULL);
			sendMessageToMaster(AH, pipefd, str);
			free(str);
		}
		else if (messageEquals(command, "TERMINATE"))
		{
			PQfinish(AH->connection);
			AH->connection = NULL;
			return;
		}
		else
		{
			die_horribly(AH, modulename,
						 "Unknown command on communication channel: %s", command);
		}
	}
}

/*
 * Note the status change:
 *
 * DispatchJobForTocEntry		WRKR_IDLE -> WRKR_WORKING
 * ListenToWorkers				WRKR_WORKING -> WRKR_FINISHED / WRKR_TERMINATED
 * ReapWorkerStatus				WRKR_FINISHED -> WRKR_IDLE
 *
 * Just calling ReapWorkerStatus() when all workers are working might or might
 * not give you an idle worker because you need to call ListenToWorkers() in
 * between and only thereafter ReapWorkerStatus(). This is necessary in order to
 * get and deal with the status (=result) of the worker's execution.
 */
void
ListenToWorkers(ArchiveHandle *AH, ParallelState *pstate, bool do_wait)
{
	int			worker;
	char	   *msg;

	msg = getMessageFromWorker(AH, pstate, do_wait, &worker);

	if (!msg)
	{
		Assert(!do_wait);
		return;
	}

	if (messageStartsWith(msg, "OK "))
	{
		char	   *statusString;
		TocEntry   *te;

		pstate->parallelSlot[worker].workerStatus = WRKR_FINISHED;
		te = pstate->parallelSlot[worker].args->te;
		if (messageStartsWith(msg, "OK RESTORE "))
		{
			statusString = msg + strlen("OK RESTORE ");
			pstate->parallelSlot[worker].status =
				(AH->MasterEndParallelItemPtr)
					(AH, te, statusString, ACT_RESTORE);
		}
		else if (messageStartsWith(msg, "OK DUMP "))
		{
			statusString = msg + strlen("OK DUMP ");
			pstate->parallelSlot[worker].status =
				(AH->MasterEndParallelItemPtr)
					(AH, te, statusString, ACT_DUMP);
		}
		else
			die_horribly(AH, modulename, "Invalid message received from worker: %s", msg);
	}
	else if (messageStartsWith(msg, "ERROR "))
	{
		Assert(AH->format == archDirectory || AH->format == archCustom);
		pstate->parallelSlot[worker].workerStatus = WRKR_TERMINATED;
		die_horribly(AH, modulename, "%s", msg + strlen("ERROR "));
	}
	else
		die_horribly(AH, modulename, "Invalid message received from worker: %s", msg);

	PrintStatus(pstate);

	/* both Unix and Win32 return pg_malloc()ed space, so we free it */
	free(msg);
}

/*
 * This function is executed in the master process.
 *
 * This function is used to get the return value of a terminated worker
 * process. If a process has terminated, its status is stored in *status and
 * the id of the worker is returned.
 */
int
ReapWorkerStatus(ParallelState *pstate, int *status)
{
	int			i;

	for (i = 0; i < pstate->numWorkers; i++)
	{
		if (pstate->parallelSlot[i].workerStatus == WRKR_FINISHED)
		{
			*status = pstate->parallelSlot[i].status;
			pstate->parallelSlot[i].status = 0;
			pstate->parallelSlot[i].workerStatus = WRKR_IDLE;
			PrintStatus(pstate);
			return i;
		}
	}
	return NO_SLOT;
}

/*
 * This function is executed in the master process.
 *
 * It looks for an idle worker process and only returns if there is one.
 */
void
EnsureIdleWorker(ArchiveHandle *AH, ParallelState *pstate)
{
	int		ret_worker;
	int		work_status;

	for (;;)
	{
		int nTerm = 0;
		while ((ret_worker = ReapWorkerStatus(pstate, &work_status)) != NO_SLOT)
		{
			if (work_status != 0)
				die_horribly(AH, modulename, "Error processing a parallel work item.\n");

			nTerm++;
		}

		/* We need to make sure that we have an idle worker before dispatching
		 * the next item. If nTerm > 0 we already have that (quick check). */
		if (nTerm > 0)
			return;

		/* explicit check for an idle worker */
		if (GetIdleWorker(pstate) != NO_SLOT)
			return;

		/*
		 * If we have no idle worker, read the result of one or more
		 * workers and loop the loop to call ReapWorkerStatus() on them
		 */
		ListenToWorkers(AH, pstate, true);
	}
}

/*
 * This function is executed in the master process.
 *
 * It waits for all workers to terminate.
 */
void
EnsureWorkersFinished(ArchiveHandle *AH, ParallelState *pstate)
{
	int			work_status;

	if (!pstate || pstate->numWorkers == 1)
		return;

	/* Waiting for the remaining worker processes to finish */
	while (!IsEveryWorkerIdle(pstate))
	{
		if (ReapWorkerStatus(pstate, &work_status) == NO_SLOT)
			ListenToWorkers(AH, pstate, true);
		else if (work_status != 0)
			die_horribly(AH, modulename, "Error processing a parallel work item");
	}
}

/*
 * This function is executed in the worker process.
 *
 * It returns the next message on the communication channel, blocking until it
 * becomes available.
 */
static char *
getMessageFromMaster(ArchiveHandle *AH, int pipefd[2])
{
	return readMessageFromPipe(pipefd[PIPE_READ], true);
}

/*
 * This function is executed in the worker process.
 *
 * It sends a message to the master on the communication channel.
 */
static void
sendMessageToMaster(ArchiveHandle *AH, int pipefd[2], const char *str)
{
	int			len = strlen(str) + 1;

	if (pipewrite(pipefd[PIPE_WRITE], str, len) != len)
		die_horribly(AH, modulename,
					 "Error writing to the communication channel: %s",
					 strerror(errno));
}

/*
 * This function is executed in the master process.
 *
 * It returns the next message from the worker on the communication channel,
 * optionally blocking (do_wait) until it becomes available.
 *
 * The id of the worker is returned in *worker.
 */
static char *
getMessageFromWorker(ArchiveHandle *AH, ParallelState *pstate, bool do_wait, int *worker)
{
	int			i;
	fd_set		workerset;
	int			maxFd = -1;
	struct		timeval nowait = { 0, 0 };

	FD_ZERO(&workerset);

	for (i = 0; i < pstate->numWorkers; i++)
	{
		if (pstate->parallelSlot[i].workerStatus == WRKR_TERMINATED)
			continue;
		FD_SET(pstate->parallelSlot[i].pipeRead, &workerset);
		/* actually WIN32 ignores the first parameter to select()... */
		if (pstate->parallelSlot[i].pipeRead > maxFd)
			maxFd = pstate->parallelSlot[i].pipeRead;
	}

	if (do_wait)
	{
		i = select_loop(maxFd, &workerset);
		Assert(i != 0);
	}
	else
	{
		if ((i = select(maxFd + 1, &workerset, NULL, NULL, &nowait)) == 0)
			return NULL;
	}

#ifndef WIN32
	if (wantAbort && !aborting)
		die_horribly(AH, modulename, "terminated by user\n");
#endif

	if (i < 0)
	{
		write_msg(NULL, "Error in ListenToWorkers(): %s", strerror(errno));
		exit(1);
	}

	for (i = 0; i < pstate->numWorkers; i++)
	{
		char	   *msg;

		if (!FD_ISSET(pstate->parallelSlot[i].pipeRead, &workerset))
			continue;

		msg = readMessageFromPipe(pstate->parallelSlot[i].pipeRead, false);
		*worker = i;
		return msg;
	}
	Assert(false);
	return NULL;
}

/*
 * This function is executed in the master process.
 *
 * It sends a message to a certain worker on the communication channel.
 */
static void
sendMessageToWorker(ArchiveHandle *AH, ParallelState *pstate, int worker, const char *str)
{
	int			len = strlen(str) + 1;

	if (pipewrite(pstate->parallelSlot[worker].pipeWrite, str, len) != len)
		die_horribly(AH, modulename,
					 "Error writing to the communication channel: %s",
					 strerror(errno));
}

/*
 * The underlying function to read a message from the communication channel (fd)
 * with optional blocking (do_wait).
 */
static char *
readMessageFromPipe(int fd, bool do_wait)
{
	char	   *msg;
	int			msgsize, bufsize;
	int			ret;

	/*
	 * The problem here is that we need to deal with several possibilites:
	 * we could receive only a partial message or several messages at once.
	 * The caller expects us to return exactly one message however.
	 *
	 * We could either read in as much as we can and keep track of what we
	 * delivered back to the caller or we just read byte by byte. Once we see
	 * (char) 0, we know that it's the message's end. This would be quite
	 * inefficient for more data but since we are reading only on the command
	 * channel, the performance loss does not seem worth the trouble of keeping
	 * internal states for different file descriptors.
	 */

	bufsize = 64;  /* could be any number */
	msg = (char *) pg_malloc(bufsize);

	msgsize = 0;
	for (;;)
	{
		Assert(msgsize <= bufsize);
		/*
		 * If we do non-blocking read, only set the channel non-blocking for
		 * the very first character. We trust in our messages to be
		 * \0-terminated, so if there is any character in the beginning, then
		 * we read the message until we find a \0 somewhere, which indicates
		 * the end of the message.
		 */
		if (msgsize == 0 && !do_wait) {
			setnonblocking(fd);
		}

		ret = piperead(fd, msg + msgsize, 1);

		if (msgsize == 0 && !do_wait)
		{
			int		saved_errno = errno;
			setblocking(fd);
			/* no data has been available */
			if (ret < 0 && saved_errno == EAGAIN)
				return NULL;
		}

		/* worker has closed the connection or another error happened */
		if (ret <= 0)
			return NULL;

		Assert(ret == 1);

		if (msg[msgsize] == '\0') {
			return msg;
		}

		msgsize++;
		if (msgsize == bufsize)
		{
			/* could be any number */
			bufsize += 16;
			msg = (char *) realloc(msg, bufsize);
		}
	}
}

