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

typedef struct ShutdownInformation
{
    ParallelState *pstate;
    Archive       *AHX;
} ShutdownInformation;

static ShutdownInformation shutdown_info;

static const char *modulename = gettext_noop("parallel archiver");

static void ShutdownConnection(PGconn **conn);

static void WaitForTerminatingWorkers(ParallelState *pstate);
static void ShutdownWorkersHard(ParallelState *pstate);
static void ShutdownWorkersSoft(ParallelState *pstate, bool do_wait);
static void PrintStatus(ParallelState *pstate);
static bool HasEveryWorkerTerminated(ParallelState *pstate);

static void lockTableNoWait(ArchiveHandle *AH, TocEntry *te);
static void WaitForCommands(ArchiveHandle *AH, int pipefd[2]);
static char *getMessageFromMaster(int pipefd[2]);
static void sendMessageToMaster(int pipefd[2], const char *str);
static char *getMessageFromWorker(ParallelState *pstate,
								  bool do_wait, int *worker);
static void sendMessageToWorker(ParallelState *pstate,
							    int worker, const char *str);
static char *readMessageFromPipe(int fd);

static void SetupWorker(ArchiveHandle *AH, int pipefd[2], int worker,
						RestoreOptions *ropt);

static void archive_close_connection(int code, void *arg);

static ParallelSlot *GetMyPSlot(ParallelState *pstate);
static void parallel_exit_msg_func(const char *modulename,
								   const char *fmt, va_list ap)
			__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 0)));
static void parallel_msg_master(ParallelSlot *slot, const char *modulename,
								const char *fmt, va_list ap)
			__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 0)));

#define messageStartsWith(msg, prefix) \
	(strncmp(msg, prefix, strlen(prefix)) == 0)
#define messageEquals(msg, pattern) \
	(strcmp(msg, pattern) == 0)

/*
 * This is the function that will be called from exit_horribly() to print the
 * error message. If the worker process does exit_horribly(), we forward its
 * last words to the master process. The master process then does exit_horribly()
 * with this error message itself and prints it normally. After printing the
 * message, exit_horribly() on the master will shut down the remaining worker
 * processes.
 */
static void
parallel_exit_msg_func(const char *modulename, const char *fmt, va_list ap)
{
	ParallelState *pstate = shutdown_info.pstate;
	ParallelSlot *slot;

	Assert(pstate);

	slot = GetMyPSlot(pstate);

	if (!slot)
		/* We're the parent, just write the message out */
		vwrite_msg(modulename, fmt, ap);
	else
		/* If we're a worker process, send the msg to the master process */
		parallel_msg_master(slot, modulename, fmt, ap);
}

/* Sends the error message from the worker to the master process */
static void
parallel_msg_master(ParallelSlot *slot, const char *modulename,
					const char *fmt, va_list ap)
{
	char		buf[512];
	int			pipefd[2];

	pipefd[PIPE_READ] = slot->pipeRevRead;
	pipefd[PIPE_WRITE] = slot->pipeRevWrite;

	strcpy(buf, "ERROR ");
	vsnprintf(buf + strlen("ERROR "),
			  sizeof(buf) - strlen("ERROR "), fmt, ap);

	sendMessageToMaster(pipefd, buf);
}

/*
 * pg_dump and pg_restore register the Archive pointer for the exit handler
 * (called from exit_horribly). This function mainly exists so that we can keep
 * shutdown_info in file scope only.
 */
void
on_exit_close_archive(Archive *AHX)
{
	shutdown_info.AHX = AHX;
	on_exit_nicely(archive_close_connection, &shutdown_info);
}

static ParallelSlot *
GetMyPSlot(ParallelState *pstate)
{
	int i;

	for (i = 0; i < pstate->numWorkers; i++)
#ifdef WIN32
		if (pstate->parallelSlot[i].threadId == GetCurrentThreadId())
#else
		if (pstate->parallelSlot[i].pid == getpid())
#endif
			return &(pstate->parallelSlot[i]);

	return NULL;
}

/* This function can close archives in both the parallel and non-parallel case. */
static void
archive_close_connection(int code, void *arg)
{
	ShutdownInformation *si = (ShutdownInformation *) arg;

	if (si->pstate)
	{
		ParallelSlot *slot = GetMyPSlot(si->pstate);

		if (!slot) {
			/*
			 * We're the master: We have already printed out the message passed
			 * to exit_horribly() either from the master itself or from a
			 * worker process. Now we need to close our own database connection
			 * (only open during parallel dump but not restore) and shut down
			 * the remaining workers.
			 */
			DisconnectDatabase(si->AHX);
#ifndef WIN32
			/*
			 * Setting aborting to true switches to best-effort-mode
			 * (send/receive but ignore errors) in communicating with our
			 * workers.
			 */
			aborting = true;
#endif
			ShutdownWorkersHard(si->pstate);
		}
		else if (slot->args->AH)
			DisconnectDatabase(&(slot->args->AH->public));
	}
	else if (si->AHX)
		DisconnectDatabase(si->AHX);
}

/*
 * If we have one worker that terminates for some reason, we'd like the other
 * threads to terminate as well (and not finish with their 70 GB table dump
 * first...). Now in UNIX we can just kill these processes, and let the signal
 * handler set wantAbort to 1. In Windows we set a termEvent and this serves as
 * the signal for everyone to terminate.
 */
void
checkAborting(ArchiveHandle *AH)
{
#ifdef WIN32
	if (WaitForSingleObject(termEvent, 0) == WAIT_OBJECT_0)
#else
	if (wantAbort)
#endif
		exit_horribly(modulename, "worker is terminating\n");
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

		/*
		 * If we Ctrl-C the master process , it's likely that we interrupt
		 * select() here. The signal handler will set wantAbort == true and the
		 * shutdown journey starts from here. Note that we'll come back here
		 * later when we tell all workers to terminate and read their
		 * responses. But then we have aborting set to true.
		 */
		if (wantAbort && !aborting)
			exit_horribly(modulename, "terminated by user\n");

		if (i < 0 && errno == EINTR)
			continue;
		break;
	}

	return i;
}
#endif

/*
 * Shut down any remaining workers, this has an implicit do_wait == true.
 *
 * The fastest way we can make the workers terminate gracefully is when
 * they are listening for new commands and we just tell them to terminate.
 */
static void
ShutdownWorkersHard(ParallelState *pstate)
{
#ifndef WIN32
	int i;
	signal(SIGPIPE, SIG_IGN);
	ShutdownWorkersSoft(pstate, false);
	for (i = 0; i < pstate->numWorkers; i++)
		kill(pstate->parallelSlot[i].pid, SIGTERM);
	WaitForTerminatingWorkers(pstate);
#else
	/* The workers monitor this event via checkAborting(). */
	SetEvent(termEvent);
	ShutdownWorkersSoft(pstate, false);
	WaitForTerminatingWorkers(pstate);
#endif
}

static void
WaitForTerminatingWorkers(ParallelState *pstate)
{
	while (!HasEveryWorkerTerminated(pstate))
	{
		int			worker;
		char	   *msg;

		PrintStatus(pstate);

		msg = getMessageFromWorker(pstate, true, &worker);
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
	wantAbort = 1;
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

	/*
	 * Set the pstate in the shutdown_info. The exit handler uses pstate if
	 * set and falls back to AHX otherwise.
	 */
	shutdown_info.pstate = pstate;
	on_exit_msg_func = parallel_exit_msg_func;

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
			exit_horribly(modulename, "Cannot create communication channels: %s\n",
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
			 * already. But CloneArchive resets the state information and also
			 * clones the database connection (for parallel dump) which both
			 * seem kinda helpful.
			 */
			AH = CloneArchive(AH);

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
			exit_horribly(modulename,
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
	ShutdownWorkersSoft(pstate, true);

	PrintStatus(pstate);

	for (i = 0; i < pstate->numWorkers; i++)
	{
		closesocket(pstate->parallelSlot[i].pipeRead);
		closesocket(pstate->parallelSlot[i].pipeWrite);
	}

	/*
	 * Remove the pstate again, so the exit handler in the parent will now
	 * again fall back to closing AH->connection (if connected).
	 */
	shutdown_info.pstate = NULL;

	free(pstate->parallelSlot);
	free(pstate);
}


/*
 * The sequence is the following (for dump, similar for restore):
 *
 * The master process starts the parallel backup in ParllelBackupStart, this
 * forks the worker processes which enter WaitForCommand().
 *
 * The master process dispatches an individual work item to one of the worker
 * processes in DispatchJobForTocEntry(). It calls
 * AH->MasterStartParallelItemPtr, a routine of the output format. This
 * function's arguments are the parents archive handle AH (containing the full
 * catalog information), the TocEntry that the worker should work on and a
 * T_Action act indicating whether this is a backup or a restore item.  The
 * function then converts the TocEntry assignment into a string that is then
 * sent over to the worker process. In the simplest case that would be
 * something like "DUMP 1234", with 1234 being the TocEntry id.
 *
 * The worker receives the message in the routine pointed to by
 * WorkerJobDumpPtr or WorkerJobRestorePtr. These are also pointers to
 * corresponding routines of the respective output format, e.g.
 * _WorkerJobDumpDirectory().
 *
 * Remember that we have forked off the workers only after we have read in the
 * catalog. That's why our worker processes can also access the catalog
 * information. Now they re-translate the textual representation to a TocEntry
 * on their side and do the required action (restore or dump).
 *
 * The result is again a textual string that is sent back to the master and is
 * interpreted by AH->MasterEndParallelItemPtr. This function can update state
 * or catalog information on the master's side, depending on the reply from the
 * worker process. In the end it returns status which is 0 for successful
 * execution.
 *
 * ---------------------------------------------------------------------
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
 * ---------------------------------------------------------------------
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

	sendMessageToWorker(pstate, worker, arg);

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
ShutdownWorkersSoft(ParallelState *pstate, bool do_wait)
{
	int			i;

	/* soft shutdown */
	for (i = 0; i < pstate->numWorkers; i++)
	{
		if (pstate->parallelSlot[i].workerStatus != WRKR_TERMINATED)
		{
			sendMessageToWorker(pstate, i, "TERMINATE");
			pstate->parallelSlot[i].workerStatus = WRKR_WORKING;
		}
	}

	if (!do_wait)
		return;

	WaitForTerminatingWorkers(pstate);
}

/*
 * This routine does some effort to gracefully shut down the database
 * connection, but not too much, since the parent is waiting for the workers to
 * terminate. The cancellation of the database connection is done in an
 * asynchronous way, so we need to wait a bit after sending PQcancel().
 * Calling PQcancel() first and then PQfinish() immediately afterwards
 * would still cancel an active connection because most likely the PQfinish()
 * has not yet been processed.
 */
static void
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
}

/*
 * ---------------------------------------------------------------------
 * One danger of the parallel backup is a possible deadlock:
 *
 * 1) Master dumps the schema and locks all tables in ACCESS SHARE mode.
 * 2) Another process requests an ACCESS EXCLUSIVE lock (which is not granted
 *    because the master holds a conflicting ACCESS SHARE lock).
 * 3) The worker process also requests an ACCESS SHARE lock to read the table.
 *    The worker's not granted that lock but is enqueued behind the ACCESS
 *    EXCLUSIVE lock request.
 * ---------------------------------------------------------------------
 *
 * Now what we do here is to just request a lock in ACCESS SHARE but with
 * NOWAIT in the worker prior to touching the table. If we don't get the lock,
 * then we know that somebody else has requested an ACCESS EXCLUSIVE lock and
 * are good to just fail the whole backup because we have detected a deadlock.
 */
static void
lockTableNoWait(ArchiveHandle *AH, TocEntry *te)
{
	Archive *AHX = (Archive *) AH;
	const char *qualId;
	PQExpBuffer query = createPQExpBuffer();
	PGresult   *res;

	Assert(AH->format == archDirectory);
	Assert(strcmp(te->desc, "BLOBS") != 0);

	appendPQExpBuffer(query, "SELECT pg_namespace.nspname,"
							 "       pg_class.relname "
							 "  FROM pg_class "
							 "  JOIN pg_namespace on pg_namespace.oid = relnamespace "
							 " WHERE pg_class.oid = %d", te->catalogId.oid);

	res = PQexec(AH->connection, query->data);

	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
		exit_horribly(modulename, "could not get relation name for oid %d: %s\n",
					  te->catalogId.oid, PQerrorMessage(AH->connection));

	resetPQExpBuffer(query);

	qualId = fmtQualifiedId(AHX->remoteVersion, PQgetvalue(res, 0, 0), PQgetvalue(res, 0, 1));

	appendPQExpBuffer(query, "LOCK TABLE %s IN ACCESS SHARE MODE NOWAIT", qualId);
	PQclear(res);

	res = PQexec(AH->connection, query->data);

	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
		exit_horribly(modulename, "could not obtain lock on relation \"%s\". This "
					  "usually means that someone requested an ACCESS EXCLUSIVE lock "
					  "on the table after the pg_dump parent process has gotten the "
					  "initial ACCESS SHARE lock on the table.\n", qualId);

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
		command = getMessageFromMaster(pipefd);

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
			sendMessageToMaster(pipefd, str);
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
			sendMessageToMaster(pipefd, str);
			free(str);
		}
		else if (messageEquals(command, "TERMINATE"))
		{
			PQfinish(AH->connection);
			AH->connection = NULL;
			return;
		}
		else
			exit_horribly(modulename,
						  "Unknown command on communication channel: %s\n",
						  command);
	}
}

/*
 * ---------------------------------------------------------------------
 * Note the status change:
 *
 * DispatchJobForTocEntry		WRKR_IDLE -> WRKR_WORKING
 * ListenToWorkers				WRKR_WORKING -> WRKR_FINISHED / WRKR_TERMINATED
 * ReapWorkerStatus				WRKR_FINISHED -> WRKR_IDLE
 * ---------------------------------------------------------------------
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

	msg = getMessageFromWorker(pstate, do_wait, &worker);

	if (!msg)
	{
		if (do_wait)
			exit_horribly(modulename, "A worker process died unexpectedly\n");
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
			exit_horribly(modulename,
						  "Invalid message received from worker: %s\n", msg);
	}
	else if (messageStartsWith(msg, "ERROR "))
	{
		Assert(AH->format == archDirectory || AH->format == archCustom);
		pstate->parallelSlot[worker].workerStatus = WRKR_TERMINATED;
		exit_horribly(modulename, "%s", msg + strlen("ERROR "));
	}
	else
		exit_horribly(modulename, "Invalid message received from worker: %s\n", msg);

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
				exit_horribly(modulename, "Error processing a parallel work item.\n");

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
			exit_horribly(modulename, "Error processing a parallel work item\n");
	}
}

/*
 * This function is executed in the worker process.
 *
 * It returns the next message on the communication channel, blocking until it
 * becomes available.
 */
static char *
getMessageFromMaster(int pipefd[2])
{
	return readMessageFromPipe(pipefd[PIPE_READ]);
}

/*
 * This function is executed in the worker process.
 *
 * It sends a message to the master on the communication channel.
 */
static void
sendMessageToMaster(int pipefd[2], const char *str)
{
	int			len = strlen(str) + 1;

	if (pipewrite(pipefd[PIPE_WRITE], str, len) != len)
		exit_horribly(modulename,
					  "Error writing to the communication channel: %s\n",
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
getMessageFromWorker(ParallelState *pstate, bool do_wait, int *worker)
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

	if (i < 0)
		exit_horribly(modulename, "Error in ListenToWorkers(): %s", strerror(errno));

	for (i = 0; i < pstate->numWorkers; i++)
	{
		char	   *msg;

		if (!FD_ISSET(pstate->parallelSlot[i].pipeRead, &workerset))
			continue;

		msg = readMessageFromPipe(pstate->parallelSlot[i].pipeRead);
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
sendMessageToWorker(ParallelState *pstate, int worker, const char *str)
{
	int			len = strlen(str) + 1;

	if (pipewrite(pstate->parallelSlot[worker].pipeWrite, str, len) != len)
	{
		/*
		 * If we're already aborting anyway, don't care if we succeed or not.
		 * The child might have gone already.
		 */
		if (!aborting)
			exit_horribly(modulename,
						  "Error writing to the communication channel: %s\n",
						  strerror(errno));
	}
}

/*
 * The underlying function to read a message from the communication channel (fd)
 * with optional blocking (do_wait).
 */
static char *
readMessageFromPipe(int fd)
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
		ret = piperead(fd, msg + msgsize, 1);

		/* worker has closed the connection or another error happened */
		if (ret <= 0)
			return NULL;

		Assert(ret == 1);

		if (msg[msgsize] == '\0')
			return msg;

		msgsize++;
		if (msgsize == bufsize)
		{
			/* could be any number */
			bufsize += 16;
			msg = (char *) realloc(msg, bufsize);
		}
	}
}

#ifdef WIN32
/*
 *	This is a replacement version of pipe for Win32 which allows returned
 *	handles to be used in select(). Note that read/write calls must be replaced
 *	with recv/send.
 */

int
pgpipe(int handles[2])
{
	SOCKET		s;
	struct sockaddr_in serv_addr;
	int			len = sizeof(serv_addr);

	handles[0] = handles[1] = INVALID_SOCKET;

	if ((s = socket(AF_INET, SOCK_STREAM, 0)) == INVALID_SOCKET)
	{
		write_msg(modulename, "pgpipe could not create socket: %ui",
				  WSAGetLastError());
		return -1;
	}

	memset((void *) &serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(0);
	serv_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	if (bind(s, (SOCKADDR *) &serv_addr, len) == SOCKET_ERROR)
	{
		write_msg(modulename, "pgpipe could not bind: %ui",
				  WSAGetLastError());
		closesocket(s);
		return -1;
	}
	if (listen(s, 1) == SOCKET_ERROR)
	{
		write_msg(modulename, "pgpipe could not listen: %ui",
				  WSAGetLastError());
		closesocket(s);
		return -1;
	}
	if (getsockname(s, (SOCKADDR *) &serv_addr, &len) == SOCKET_ERROR)
	{
		write_msg(modulename, "pgpipe could not getsockname: %ui",
				  WSAGetLastError());
		closesocket(s);
		return -1;
	}
	if ((handles[1] = socket(PF_INET, SOCK_STREAM, 0)) == INVALID_SOCKET)
	{
		write_msg(modulename, "pgpipe could not create socket 2: %ui",
				  WSAGetLastError());
		closesocket(s);
		return -1;
	}

	if (connect(handles[1], (SOCKADDR *) &serv_addr, len) == SOCKET_ERROR)
	{
		write_msg(modulename, "pgpipe could not connect socket: %ui",
				  WSAGetLastError());
		closesocket(s);
		return -1;
	}
	if ((handles[0] = accept(s, (SOCKADDR *) &serv_addr, &len)) == INVALID_SOCKET)
	{
		write_msg(modulename, "pgpipe could not accept socket: %ui",
				  WSAGetLastError());
		closesocket(handles[1]);
		handles[1] = INVALID_SOCKET;
		closesocket(s);
		return -1;
	}
	closesocket(s);
	return 0;
}

int
piperead(int s, char *buf, int len)
{
	int			ret = recv(s, buf, len, 0);

	if (ret < 0 && WSAGetLastError() == WSAECONNRESET)
		/* EOF on the pipe! (win32 socket based implementation) */
		ret = 0;
	return ret;
}

#endif
