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
static DWORD		tMasterThreadId = 0;
static HANDLE		termEvent = INVALID_HANDLE_VALUE;
#else /* UNIX */
static PGconn     **worker_conn = NULL;
#endif

/*
 * The parallel error handler is called for any die_horribly() in a child or master process.
 * It then takes control over shutting down the rest of the gang.
 */
void (*vparallel_error_handler)(ArchiveHandle *AH, const char *modulename,
								const char *fmt, va_list ap) = NULL;

/* The actual implementation of the error handler function */
static void vparallel_error_handler_imp(ArchiveHandle *AH, const char *modulename,
										const char *fmt, va_list ap);

static volatile sig_atomic_t wantExit = 0;
static const char *modulename = gettext_noop("parallel archiver");

#ifdef WIN32
static unsigned int __stdcall ShutdownConnection(PGconn **conn);
#else
static int ShutdownConnection(PGconn **conn);
#endif

static void ShutdownWorkersHard(ArchiveHandle *AH, ParallelState *pstate);
static void ShutdownWorkersSoft(ArchiveHandle *AH, ParallelState *pstate, bool do_wait);
static void PrintStatus(ParallelState *pstate);
static bool HasEveryWorkerTerminated(ParallelState *pstate);

static void lockTableNoWait(ArchiveHandle *AH, TocEntry *te);
static void WaitForCommands(ArchiveHandle *AH, int pipefd[2], int worker);
static char *getMessageFromMaster(ArchiveHandle *AH, int pipefd[2]);
static void sendMessageToMaster(ArchiveHandle *AH, int pipefd[2], const char *str);
static char *getMessageFromWorker(ArchiveHandle *AH, ParallelState *pstate,
								  bool do_wait, int *worker);
static void sendMessageToWorker(ArchiveHandle *AH, ParallelState *pstate,
							    int worker, const char *str);
static char *readMessageFromPipe(int fd, bool do_wait);

static void SetupWorker(ArchiveHandle *AH, int pipefd[2], int worker,
						RestoreOptions *ropt);


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
	/*
	 * Given that we could error out for memory problems, it seems safest to
	 * have buf allocated on the stack already.
	 */
	static char buf[512];
	ParallelState pstate;
	int			pipefd[2];
	static int	handling = 0;
	int			i;

	if (AH->is_clone)
	{
		/* we are the child, get the message out to the parent */
		strcpy(buf, "ERROR ");
		vsnprintf(buf + strlen("ERROR "), sizeof(buf) - strlen("ERROR "), fmt, ap);
		parallel_error_handler_escrow_data(GET, &pstate);
#ifdef WIN32
		i = GetSlotOfThread(&pstate, GetCurrentThreadId());
#else
		i = GetSlotOfProcess(&pstate, getpid());
#endif
		pipefd[PIPE_READ] = pstate.parallelSlot[i].pipeRevRead;
		pipefd[PIPE_WRITE] = pstate.parallelSlot[i].pipeRevWrite;
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
		/*
		 * We are the parent. We need the handling variable to see if we're
		 * already handling an error.
		 */
		if (!handling)
		{
			handling = 1;
#ifndef WIN32
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
	}
	Assert(false);
}







#ifdef WIN32
/*
 * If we have one worker that terminates for some reason, we'd like the other
 * threads to terminate as well (and not finish with their 70 GB table dump
 * first...). Now in UNIX we can just kill these processes, whereas in Windows,
 * the only way to safely terminate a thread is from within the thread itself.
 * So we need to do active polling here to detect that from within the thread.
 */
void
checkWorkerTerm(void)
{
	if (WaitForSingleObject(termEvent, 0) == WAIT_OBJECT_0)
		/* terminate */
		ExitThread(1);
}

#else /* UNIX */
void
checkWorkerTerm(void)
{
	/* nothing to do in UNIX */
}
#endif

#ifdef WIN32
/*
 * Returns the index of the worker process that terminated or NO_SLOT if no
 * worker process has terminated at all. If do_wait == true, we will wait until
 * at least one worker process terminates.
 */
static int
CheckForWorkerTermination(ParallelState *pstate, bool do_wait)
{
	static HANDLE *lpHandles = NULL;
	int i, ret, j = 0;

	if (!lpHandles)
		lpHandles = (HANDLE *) pg_malloc(sizeof(HANDLE) * pstate->numWorkers);

	for(i = 0; i < pstate->numWorkers; i++)
		if (pstate->parallelSlot[i].workerStatus != WRKR_TERMINATED)
			lpHandles[j++] = (HANDLE) pstate->parallelSlot[i].hThread;

	printf("Have %d not-terminated threads\n", j);

	if (j == 0)
		return NO_SLOT;

	ret = WaitForMultipleObjects(j, lpHandles, false, do_wait ? INFINITE : 0);
	if (ret == WAIT_FAILED)
	{
		/* Something went wrong */
		/* XXX */
	}

	if (ret == WAIT_TIMEOUT)
	{
		Assert(!do_wait);
		return NO_SLOT;
	}

	for (i = 0; i < pstate->numWorkers; i++)
	{
		if ((HANDLE) pstate->parallelSlot[i].hThread == lpHandles[WAIT_OBJECT_0 + ret])
		{
			Assert(pstate->parallelSlot[i].workerStatus != WRKR_TERMINATED);
			pstate->parallelSlot[i].workerStatus = WRKR_TERMINATED;
			/* XXX */
			pstate->parallelSlot[i].status = 0;
			CloseHandle((HANDLE) pstate->parallelSlot[i].hThread);
			pstate->parallelSlot[i].hThread = (uintptr_t) 0;
			printf("Found that process %d terminated\n", i);
			return i;
		}
	}

	Assert(false);

	/* leaks lpHandles, but it's static, so it's leaked only once */
	return NO_SLOT;
}
#else /* UNIX */
static int
CheckForWorkerTermination(ParallelState *pstate, bool do_wait)
{
	int		status, i;
	pid_t	pid;

	for (;;) {
		pid = waitpid(-1, &status, do_wait ? 0 : WNOHANG);
		/* Got a terminating child */
		if (pid > 0)
			break;
		/* Interrupted, should only happen for do_wait == true */
		if (pid < 0 && errno == EINTR)
			continue;
		/* No child has terminated */
		if (pid == 0 || (pid < 0 && errno == ECHILD)) {
			Assert(!do_wait);
			return NO_SLOT;
		}
		Assert(false);
	}

	if ((i = GetSlotOfProcess(pstate, pid)) != NO_SLOT)
	{
		pstate->parallelSlot[i].workerStatus = WRKR_TERMINATED;
		return i;
	}

	Assert(false);
	return NO_SLOT;
}
#endif


/*
 * Call exit() when running on
 *    UNIX: always
 *    Windows: if we are the master.
 *
 * Call ExitThread() if we are on Windows and a worker.
 *
 * This is a reasonably simple function but it cannot be a macro because it's
 * called from a different object file and we'd like to keep the OS-dependent
 * code all in one place.
 */
void
myExit(int status)
{
#ifdef WIN32
	if (tMasterThreadId != GetCurrentThreadId())
		ExitThread(status);
#endif
	exit(status);
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
		printf("select returned %d\n", i);

		/* XXX see if this can ever run in a non-master at all - see above */
		if (tMasterThreadId != GetCurrentThreadId())
			checkWorkerTerm();
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
		if (wantExit) {
			printf("leaving select_loop, wantExit == 1\n");
			return NO_SLOT;
		}
		if (i < 0 && errno == EINTR)
			continue;
		break;
	}

	return i;
}
#endif

#ifdef WIN32
/*
 * Shut down any remaining workers, this has an implicit do_wait == true
 */
static void
ShutdownWorkersHard(ArchiveHandle *AH, ParallelState *pstate)
{
	int i;

	/* The workers monitor this event via checkWorkerTerm(). */
	SetEvent(termEvent);

	/*
	 * The fastest way we can make them terminate is when they are listening
	 * for new commands and we just tell them to terminate.
	 */
	ShutdownWorkersSoft(AH, pstate, false);

	while ((i = CheckForWorkerTermination(pstate, true)) != NO_SLOT)
	{
		/* This will be set to NULL upon PQfinish() of the connection. */
		if (*(pstate->parallelSlot[i].conn))
		{
			uintptr_t handle;
			/*
			 * Call the cleanup thread on this object: Here's the trick, in
			 * order to shut the database connections down in parallel, we
			 * just launch new threads that do nothing but that.
			 */
			handle = _beginthreadex(NULL, 0, &ShutdownConnection,
									pstate->parallelSlot[i].conn,
									0, NULL);
			pstate->parallelSlot[i].hThread = handle;
			pstate->parallelSlot[i].workerStatus = WRKR_WORKING;
		}
	}
	Assert(HasEveryWorkerTerminated(pstate));
}
#else /* UNIX */
static void
ShutdownWorkersHard(ArchiveHandle *AH, ParallelState *pstate)
{
	int			i;

	/*
	 * The fastest way we can make them terminate is when they are listening
	 * for new commands and we just tell them to terminate.
	 */
	ShutdownWorkersSoft(AH, pstate, false);

	for (i = 0; i < pstate->numWorkers; i++)
		kill(pstate->parallelSlot[i].pid, SIGTERM);

	/* Reset our signal handler, if we get signaled again, terminate normally */
	signal(SIGINT, SIG_DFL);
	signal(SIGTERM, SIG_DFL);
	signal(SIGQUIT, SIG_DFL);

	printf("Waiting for workers to terminate\n");
	while (!HasEveryWorkerTerminated(pstate))
	{
		PrintStatus(pstate);
		CheckForWorkerTermination(pstate, true);
	}

	printf("All workers terminated\n");
}
#endif

/* Signal handling (UNIX only) */
#ifndef WIN32
static void
WorkerExit(int signum)
{
	/*
	 * Signal Handler. We could get multiple signals delivered and then we
	 * might get interrupted in the Signal handler only to enter a
	 * different incarnation of this function.
	 *
	 * The following holds for this function:
	 * - At least one function sees wantExit == 0 and increases it to 1.
	 * - there could be several of them, even though wantExit++ is most
	 *   probably atomic.
	 * - The first signal handler that succeeds setting wantExit to 1 will not be
	 *   interrupted further by new signal handler invocations
	 * - This process sees worker_conn != NULL and calls ShutdownConnection().
	 * - That function sets the worker_conn to NULL
	 * - The function that did the actual work terminates
	 * - Any other signal handler that is now continued sees worker_conn == NULL already.
	 */

	if (wantExit++ > 0)
		return;

	/* worker_conn == NULL is only true in the parent */
	if (worker_conn == NULL) {
		printf("worker_conn == NULL in %d\n", getpid());
		return;
	}

	if (*worker_conn == NULL) {
		printf("*worker_conn == NULL in %d\n", getpid());
		return;
	}

	/* we were here first */
	if (*worker_conn)
		ShutdownConnection(worker_conn);

	exit(1);
}

static void
MasterExit(int signum)
{
	/* catch SIGINT, SIGTERM */
	/* set a flag to indicate that we want to terminate */
	printf("In master's signal handler\n");
	wantExit = 1;
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

	WaitForCommands(AH, pipefd, worker);

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
	PGconn		  **conn;
} WorkerInfo;

static unsigned __stdcall
init_spawned_worker_win32(WorkerInfo *wi)
{
	ArchiveHandle *AH = wi->AH;
	int pipefd[2] = { wi->pipeRead, wi->pipeWrite };
	int worker = wi->worker;
	RestoreOptions *ropt = wi->ropt;
	PGconn **conn = wi->conn;

	printf("My thread handle: %d, master: %d\n", GetCurrentThreadId(), tMasterThreadId);

	/* evil hack to copy parents connection for now */
	AH->connection = *(wi->conn);
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
	worker_conn = NULL;

	signal(SIGTERM, MasterExit);
	signal(SIGINT, MasterExit);
	signal(SIGQUIT, MasterExit);
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
#ifdef WIN32
		/* Allocate a new structure for every worker */
		wi = (WorkerInfo *) pg_malloc(sizeof(WorkerInfo));

		wi->ropt = ropt;
		wi->worker = i;
		wi->AH = CloneArchive(AH);
		wi->conn = &(AH->connection);
		wi->pipeRead = pipeMW[PIPE_READ];
		wi->pipeWrite = pipeWM[PIPE_WRITE];

		pstate->parallelSlot[i].conn = &wi->AH->connection;

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
			 * otherwise in the worker process. On Windows this writes to the
			 * global pstate, in Unix we write to our process-local copy but
			 * that's also where we'd retrieve this information back from.
			 */
			pstate->parallelSlot[i].pipeRevRead = pipefd[PIPE_READ];
			pstate->parallelSlot[i].pipeRevWrite = pipefd[PIPE_WRITE];
			pstate->parallelSlot[i].pid = getpid();

			signal(SIGTERM, WorkerExit);
			signal(SIGINT, WorkerExit);
			signal(SIGQUIT, WorkerExit);

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
			worker_conn = &AH->connection;
			Assert(*worker_conn == AH->connection);

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

	printf("Waiting for workers to terminate\n");
	while (!HasEveryWorkerTerminated(pstate))
	{
		PrintStatus(pstate);
		CheckForWorkerTermination(pstate, true);
	}

	printf("All workers terminated\n");
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
#ifdef WIN32
static unsigned __stdcall
#else
static int
#endif
ShutdownConnection(PGconn **conn)
{
	PGcancel   *cancel;
	char		errbuf[1];
	int			i;

	Assert(conn != NULL);
	Assert(*conn != NULL);

	if (PQisBusy(*conn))
	{
		if ((cancel = PQgetCancel(*conn)))
		{
			PQcancel(cancel, errbuf, sizeof(errbuf));
			PQfreeCancel(cancel);
		}

		/* give the server a little while */
		for (i = 0; i < 10; i++)
		{
			PQconsumeInput(*conn);
			printf("busy in [%d]: %d\n", getpid(), PQisBusy(*conn));
			if (!PQisBusy(*conn))
				break;
			pg_usleep((SHUTDOWN_GRACE_PERIOD / 10) * 1000);
		}
	}
	printf("Finishing in [%d], busy: %d\n", getpid(), PQisBusy(*conn));
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

	printf("Locking table: %s\n", query->data);

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
#define messageStartsWith(msg, prefix) \
	(strncmp(msg, prefix, strlen(prefix)) == 0)
#define messageEquals(msg, pattern) \
	(strcmp(msg, pattern) == 0)
static void
WaitForCommands(ArchiveHandle *AH, int pipefd[2], int worker /* XXX remove me */)
{
	char	   *command;
	DumpId		dumpId;
	int			nBytes;
	char	   *str = NULL;
	TocEntry   *te;

	for(;;)
	{
		command = getMessageFromMaster(AH, pipefd);
		printf("Got message %s from master in worker %d\n", command, worker);

/* XXX
		if (worker == 5) {
			PQfinish(*worker_conn);
			die_horribly(AH, modulename, "Null bock mehr\n");
		}
  XXX
*/

		if (messageStartsWith(command, "DUMP "))
		{
			Assert(AH->format == archDirectory);
			sscanf(command + strlen("DUMP "), "%d%n", &dumpId, &nBytes);
			Assert(nBytes == strlen(command) - strlen("DUMP "));

			printf("DumpId: %d\n", dumpId);
			te = getTocEntryByDumpId(AH, dumpId);
			printf("got TocEntry for %s\n", te->tag);
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
			printf("Got from workerjob: %s\n", str);
			sendMessageToMaster(AH, pipefd, str);
			free(str);
		}
		else if (messageEquals(command, "TERMINATE"))
		{
			printf("Terminating in %d\n", getpid());
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

	/*
	 * terminated workers should send a message. Even if they don't, we should
	 * get info about them from select...
	if ((i = CheckForWorkerTermination(pstate, false)) != NO_SLOT)
	{
		die_horribly(AH, modulename,
					 "A worker died unexpectedly while working on %s %s\n",
					 pstate->parallelSlot[i].args->te->desc,
					 pstate->parallelSlot[i].args->te->tag);
	}
	*/

	printf("In ListenToWorkers()\n");
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
		die_horribly(AH, modulename, msg + strlen("ERROR "));
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

	/* Checks if we need to terminate and if so then exit right from here */
	if (wantExit)
		die_horribly(AH, modulename, "terminated by user\n");

	for (;;)
	{
		int nTerm = 0;
		while ((ret_worker = ReapWorkerStatus(pstate, &work_status)) != NO_SLOT)
		{
			if (work_status != 0)
				die_horribly(AH, modulename, "Error processing a parallel work item.\n" /* XXX \n or not ? */);

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

	printf("Sending message %s to master on fd %d\n", str, pipefd[PIPE_WRITE]);

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

	if (wantExit)
		die_horribly(AH, modulename, "terminated by user\n");

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

		if ((msg = readMessageFromPipe(pstate->parallelSlot[i].pipeRead, false)))
		{
			*worker = i;
			return msg;
		}
		else
		{
			/*
			 * Something bad has happened... We were promised that the FD is
			 * ready but we got NULL, so this worker has terminated the
			 * connection.
			 */
			printf("Need to shut down the workers - got a NULL msg from a worker\n");
			die_horribly(AH, modulename,
						 "A worker died unexpectedly while working on %s %s\n",
						 pstate->parallelSlot[i].args->te->desc,
						 pstate->parallelSlot[i].args->te->tag);
		}
	}
	printf("Nobody was ready...\n");
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

	printf("Sending message %s to worker %d on fd %d\n", str, worker, pstate->parallelSlot[worker].pipeWrite);

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

	/* XXX set this to some reasonable initial value for a release */
	bufsize = 1;
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

		if (ret == 0)
		{
			/* worker has closed the connection */
			return NULL;
		}
		if (ret < 0)
		{
			/* XXX test me */
			write_msg(NULL, "error reading from communication partner: %s\n",
					  strerror(errno));
			myExit(1);
		}

		Assert(ret == 1);

		if (msg[msgsize] == '\0') {
			printf("Got message %s from fd %d\n", msg, fd);
			return msg;
		}

		msgsize++;
		if (msgsize == bufsize)
		{
			bufsize += 10;
			msg = (char *) realloc(msg, bufsize);
		}
	}
}

