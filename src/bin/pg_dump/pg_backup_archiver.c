/*-------------------------------------------------------------------------
 *
 * pg_backup_archiver.c
 *
 *	Private implementation of the archiver routines.
 *
 *	See the headers to pg_restore for more details.
 *
 * Copyright (c) 2000, Philip Warner
 *	Rights are granted to use this software in any way so long
 *	as this notice is not removed.
 *
 *	The author is not responsible for loss or damages that may
 *	result from its use.
 *
 *
 * IDENTIFICATION
 *		src/bin/pg_dump/pg_backup_archiver.c
 *
 *-------------------------------------------------------------------------
 */

#include "pg_backup_db.h"
#include "dumputils.h"

#include <ctype.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>


#ifdef WIN32
static unsigned int __stdcall ShutdownConnection(PGconn **conn);
#else
static int ShutdownConnection(PGconn **conn);
#endif

static void ShutdownWorkersSoft(ArchiveHandle *AH, ParallelState *pstate);
static bool HasEveryWorkerTerminated(ParallelState *pstate);

static PGconn     *worker_conn = NULL;

#ifdef WIN32
static DWORD tMasterThreadId = 0;
static HANDLE termEvent = INVALID_HANDLE_VALUE;

		/* WIN32 */
		/* closesocket is a windows socket function and stays as-is */
		#define writesocket(fd, s, l) send((fd),(s),(l),0)
		#define readsocket(fd, s, l)  recv((fd),(s),(l),0)
		#define setnonblocking(fd) \
			do { u_long mode = 1; \
				 ioctlsocket((fd), FIONBIO, &mode); \
			} while(0);
		#define setblocking(fd) \
			do { u_long mode = 0; \
				 ioctlsocket((fd), FIONBIO, &mode); \
			} while(0);
		/* pgpipe is PostgreSQL's own pipe implementation, stays as-is */
		/*
		 * Call exit() if we are the master and ExitThread() if we are
		 * a worker.
		 */
		#define myExit(x) \
			do {  if (tMasterThreadId == GetCurrentThreadId()) \
					exit(x); \
				  ExitThread(x); \
			} while(0);

static int
select_loop(int maxFd, fd_set *workerset)
{
	int i;

	fd_set saveSet = *workerset;
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
		if (tMasterThreadId != GetCurrentThreadId())
			checkTerm();
		if (i == SOCKET_ERROR && WSAGetLastError() == WSAEINTR)
			break;
		if (i)
			break;
	}

	return i;
}

/*
 * If we have one worker that terminates for some reason, we'd like the other
 * threads to terminate as well (and not finish with their 70 GB table dump
 * first...). Now in UNIX we can just kill these processes, whereas in Windows,
 * the only way to safely terminate a thread is from within the thread itself.
 * So we need to do active polling here on the dumper formats that support
 * parallel Windows operations.
 */
void
checkTerm(void)
{
	//printf("In checkTerm\n");
	if (WaitForSingleObject(termEvent, 0) == WAIT_OBJECT_0)
		/* terminate */
		ExitThread(1);
}

static int
CheckForWorkerTermination(ParallelState *pstate, bool do_wait)
{
	static HANDLE* lpHandles = NULL;
	int i, ret, j = 0;
	
	if (!lpHandles)
		lpHandles = malloc(sizeof(HANDLE) * pstate->numWorkers);

	for(i = 0; i < pstate->numWorkers; i++)
		if (pstate->parallelSlot[i].workerStatus != WRKR_TERMINATED)
		{
			lpHandles[j] = (HANDLE) pstate->parallelSlot[i].hThread;
			printf("Added thread handle %p\n", lpHandles[j]);
			j++;
		}
	printf("Have %d not-terminated threads\n", j);

	if (j == 0)
		return NO_SLOT;

	ret = WaitForMultipleObjects(j, lpHandles, false, do_wait ? INFINITE : 0);

	printf("Ret to WaitForMultipleObjects: %d\n", ret);

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

static void
ShutdownWorkersHard(ArchiveHandle *AH, ParallelState *pstate)
{
	int i;

	/* The workers monitor this event via checkTerm() */
	SetEvent(termEvent);

	while ((i = CheckForWorkerTermination(pstate, true)) != NO_SLOT)
	{
		/* This will be set to NULL upon termination of the connection. */
		if (false && pstate->parallelSlot[i].conn)
		{
			uintptr_t handle;
			/*
			 * Call the cleanup thread on this object: Here's the trick, in
			 * order to shut the database connections down in parallel, we
			 * just launch new threads that do nothing else than that.
			 */
			handle = _beginthreadex(NULL, 0, &ShutdownConnection,
									&pstate->parallelSlot[i].conn,
									0, NULL);
			pstate->parallelSlot[i].hThread = handle;
			pstate->parallelSlot[i].workerStatus = WRKR_WORKING;
		}
	}
	Assert(HasEveryWorkerTerminated(pstate));
}

#else
static volatile sig_atomic_t wantExit = 0;
		/* UNIX */
		#define closesocket close
		#define writesocket write
		#define readsocket  read
		#define setnonblocking(fd) \
			do { long flags = (long) fcntl((fd), F_GETFL, 0); \
				fcntl(fd, F_SETFL, flags | O_NONBLOCK); \
			} while(0);
		#define setblocking(fd) \
			do { long flags = (long) fcntl((fd), F_GETFL, 0); \
				fcntl(fd, F_SETFL, flags & ~O_NONBLOCK); \
			} while(0);
		#define pgpipe pipe
		#define myExit(x)	  exit(x)

void
checkTerm(void)
{
	/* nothing to do in UNIX */
}

static int
select_loop(int maxFd, fd_set *workerset)
{
	int i;

	fd_set saveSet = *workerset;
	for (;;)
	{
		*workerset = saveSet;
		i = select(maxFd + 1, workerset, NULL, NULL, NULL);
		Assert(i != 0);
		if (i < 0 && errno == EINTR)
			continue;
		break;
	}

	return i;
}
static void
ShutdownWorkersHard(ArchiveHandle *AH, ParallelState *pstate)
{
	int i;

	for (i = 0; i < pstate->numWorkers; i++)
		kill(pstate->parallelSlot[i].pid, SIGTERM);
}

static int
CheckForWorkerTermination(ParallelState *pstate, bool do_wait)
{
	int		status, i;
	pid_t	pid = waitpid(-1, &status, do_wait ? 0 : WNOHANG);

	if (pid < 0 && errno == EINTR)
		return NO_SLOT;

	if (pid == 0 || (pid < 0 && errno == ECHILD)) {
		Assert(!do_wait);
		return NO_SLOT;
	}

	Assert(pid > 0);

	for (i = 0; i < pstate->numWorkers; i++)
	{
		if (pstate->parallelSlot[i].pid == pid)
		{
			Assert(pstate->parallelSlot[i].workerStatus != WRKR_TERMINATED);
			pstate->parallelSlot[i].workerStatus = WRKR_TERMINATED;
			if (WIFEXITED(status) && WEXITSTATUS(status) == 0)
				pstate->parallelSlot[i].status = 0;
			else
				/* XXX */
				pstate->parallelSlot[i].status = 1;
			return i;
		}
	}

	Assert(false);
	return NO_SLOT;
}

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
	 *   probably atomic, it's strictly speaking wantExit = wantExit + 1.
	 *   Now if the value has been read, is found to be 0 and then we get
	 *   another signal, we get interrupted again and we will first service
	 *   this interrupt.
	 * - The first signal handler that succeeds setting wantExit to 1 will not be
	 *   interrupted further by new signal handler invocations
	 * - This process sees worker_conn != NULL and calls ShutdownConnection().
	 * - That function sets the worker_conn to NULL
	 * - The function that did the actual work terminates
	 * - Any other signal handler that is now continued sees worker_conn == NULL already.
	 */
	if (wantExit++ > 0)
		return;

	if (worker_conn == NULL)
		return;

	Assert(*worker_conn != NULL);

	/* we were here first */
	if (*worker_conn)
		ShutdownConnection(&worker_conn);
	else
		printf("Gconn is NULL in %d\n", getpid());

	exit(1);
}
#endif

#define PIPE_READ 0
#define PIPE_WRITE 1
#define SHUTDOWN_GRACE_PERIOD			   (500)

#ifdef WIN32
int
pgpipe(int handles[2])
{
	SOCKET		s;
	struct sockaddr_in serv_addr;
	int			len = sizeof(serv_addr);

	handles[0] = handles[1] = INVALID_SOCKET;

	if ((s = socket(AF_INET, SOCK_STREAM, 0)) == INVALID_SOCKET)
	{
		perror("pgpipe could not create socket: %ui", WSAGetLastError());
		return -1;
	}

	memset((void *) &serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(0);
	serv_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	if (bind(s, (SOCKADDR *) &serv_addr, len) == SOCKET_ERROR)
	{
		perror("pgpipe could not bind: %ui", WSAGetLastError());
		closesocket(s);
		return -1;
	}
	if (listen(s, 1) == SOCKET_ERROR)
	{
		perror("pgpipe could not listen: %ui", WSAGetLastError());
		closesocket(s);
		return -1;
	}
	if (getsockname(s, (SOCKADDR *) &serv_addr, &len) == SOCKET_ERROR)
	{
		perror("pgpipe could not getsockname: %ui", WSAGetLastError());
		closesocket(s);
		return -1;
	}
	if ((handles[1] = socket(PF_INET, SOCK_STREAM, 0)) == INVALID_SOCKET)
	{
		perror("pgpipe could not create socket 2: %ui", WSAGetLastError());
		closesocket(s);
		return -1;
	}

	if (connect(handles[1], (SOCKADDR *) &serv_addr, len) == SOCKET_ERROR)
	{
		perror("pgpipe could not connect socket: %ui", WSAGetLastError());
		closesocket(s);
		return -1;
	}
	if ((handles[0] = accept(s, (SOCKADDR *) &serv_addr, &len)) == INVALID_SOCKET)
	{
		perror("pgpipe could not accept socket: %ui", WSAGetLastError());
		closesocket(handles[1]);
		handles[1] = INVALID_SOCKET;
		closesocket(s);
		return -1;
	}
	closesocket(s);
	printf("Returning pair %d (read) and %d (write)\n", handles[0], handles[1]);
	return 0;
}
#endif






#ifdef WIN32
#include <io.h>
#endif

#include "libpq/libpq-fs.h"

/* state needed to save/restore an archive's output target */
typedef struct _outputContext
{
	void	   *OF;
	int			gzOut;
} OutputContext;

const char *progname;

static const char *modulename = gettext_noop("archiver");

/* index array created by fix_dependencies -- only used in parallel restore */
static TocEntry **tocsByDumpId; /* index by dumpId - 1 */
static DumpId maxDumpId;		/* length of above array */


static ArchiveHandle *_allocAH(const char *FileSpec, const ArchiveFormat fmt,
		 const int compression, ArchiveMode mode);
static void _getObjectDescription(PQExpBuffer buf, TocEntry *te,
					  ArchiveHandle *AH);
static void _printTocEntry(ArchiveHandle *AH, TocEntry *te, RestoreOptions *ropt, bool isData, bool acl_pass);


static void _doSetFixedOutputState(ArchiveHandle *AH);
static void _doSetSessionAuth(ArchiveHandle *AH, const char *user);
static void _doSetWithOids(ArchiveHandle *AH, const bool withOids);
static void _reconnectToDB(ArchiveHandle *AH, const char *dbname);
static void _becomeUser(ArchiveHandle *AH, const char *user);
static void _becomeOwner(ArchiveHandle *AH, TocEntry *te);
static void _selectOutputSchema(ArchiveHandle *AH, const char *schemaName);
static void _selectTablespace(ArchiveHandle *AH, const char *tablespace);
static void processEncodingEntry(ArchiveHandle *AH, TocEntry *te);
static void processStdStringsEntry(ArchiveHandle *AH, TocEntry *te);
static teReqs _tocEntryRequired(TocEntry *te, RestoreOptions *ropt, bool include_acls);
static bool _tocEntryIsACL(TocEntry *te);
static void _disableTriggersIfNecessary(ArchiveHandle *AH, TocEntry *te, RestoreOptions *ropt);
static void _enableTriggersIfNecessary(ArchiveHandle *AH, TocEntry *te, RestoreOptions *ropt);
static TocEntry *getTocEntryByDumpId(ArchiveHandle *AH, DumpId id);
static void _moveBefore(ArchiveHandle *AH, TocEntry *pos, TocEntry *te);
static int	_discoverArchiveFormat(ArchiveHandle *AH);

static int	RestoringToDB(ArchiveHandle *AH);
static void dump_lo_buf(ArchiveHandle *AH);
static void _write_msg(const char *modulename, const char *fmt, va_list ap) __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 0)));
static void _die_horribly(ArchiveHandle *AH, const char *modulename, const char *fmt, va_list ap) __attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 0)));

static void dumpTimestamp(ArchiveHandle *AH, const char *msg, time_t tim);
static void SetOutput(ArchiveHandle *AH, char *filename, int compression);
static OutputContext SaveOutput(ArchiveHandle *AH);
static void RestoreOutput(ArchiveHandle *AH, OutputContext savedContext);

static int restore_toc_entry(ArchiveHandle *AH, TocEntry *te,
							 RestoreOptions *ropt, bool is_parallel);
static void restore_toc_entries_parallel(ArchiveHandle *AH, ParallelState *pstate);
static void par_list_header_init(TocEntry *l);
static void par_list_append(TocEntry *l, TocEntry *te);
static void par_list_remove(TocEntry *te);
static TocEntry *get_next_work_item(ArchiveHandle *AH,
				   TocEntry *ready_list,
				   ParallelState *pstate);
static void mark_work_done(ArchiveHandle *AH, TocEntry *ready_list,
			   int worker, int status,
			   ParallelState *pstate);
static void fix_dependencies(ArchiveHandle *AH);
static bool has_lock_conflicts(TocEntry *te1, TocEntry *te2);
static void repoint_table_dependencies(ArchiveHandle *AH,
						   DumpId tableId, DumpId tableDataId);
static void identify_locking_dependencies(TocEntry *te);
static void reduce_dependencies(ArchiveHandle *AH, TocEntry *te,
					TocEntry *ready_list);
static void mark_create_done(ArchiveHandle *AH, TocEntry *te);
static void inhibit_data_for_failed_table(ArchiveHandle *AH, TocEntry *te);

static void ListenToWorkers(ArchiveHandle *AH, ParallelState *pstate, bool do_wait);
static void PrintStatus(ParallelState *pstate);
static int GetIdleWorker(ParallelState *pstate);
static int ReapworkerStatus(ParallelState *pstate, int *status);
static bool HasEveryWorkerTerminated(ParallelState *pstate);
static bool IsEveryWorkerIdle(ParallelState *pstate);

static char *getMessageFromWorker(ArchiveHandle *AH, ParallelState *pstate,
								 bool do_wait, int *worker);
static void sendMessageToWorker(ArchiveHandle *AH, ParallelState *pstate,
							   int worker, const char *str);
static char *readMessageFromPipe(int fd, bool do_wait);

static void WaitForCommands(ArchiveHandle *AH, int pipefd[2], int worker);
static char *getMessageFromMaster(ArchiveHandle *AH, int pipefd[2]);
static void sendMessageToMaster(ArchiveHandle *AH, int pipefd[2], const char *str);
static void SetupWorker(ArchiveHandle *AH, int pipefd[2], int worker,
					   RestoreOptions *ropt, PGconn **conn);

/*
 *	Wrapper functions.
 *
 *	The objective it to make writing new formats and dumpers as simple
 *	as possible, if necessary at the expense of extra function calls etc.
 *
 */


/* Create a new archive */
/* Public */
Archive *
CreateArchive(const char *FileSpec, const ArchiveFormat fmt,
			  const int compression, ArchiveMode mode)

{
	ArchiveHandle *AH = _allocAH(FileSpec, fmt, compression, mode);

	return (Archive *) AH;
}

/* Open an existing archive */
/* Public */
Archive *
OpenArchive(const char *FileSpec, const ArchiveFormat fmt)
{
	ArchiveHandle *AH = _allocAH(FileSpec, fmt, 0, archModeRead);

	return (Archive *) AH;
}

/* Public */
void
CloseArchive(Archive *AHX)
{
	int			res = 0;
	ArchiveHandle *AH = (ArchiveHandle *) AHX;

	(*AH->ClosePtr) (AH);

	/* Close the output */
	if (AH->gzOut)
		res = GZCLOSE(AH->OF);
	else if (AH->OF != stdout)
		res = fclose(AH->OF);

	if (res != 0)
		die_horribly(AH, modulename, "could not close output file: %s\n",
					 strerror(errno));
}

/* Public */
void
RestoreArchive(Archive *AHX, RestoreOptions *ropt)
{
	ArchiveHandle *AH = (ArchiveHandle *) AHX;
	bool		parallel_mode;
	TocEntry   *te;
	teReqs		reqs;
	OutputContext sav;

	AH->ropt = ropt;
	AH->stage = STAGE_INITIALIZING;

	/*
	 * Check for nonsensical option combinations.
	 *
	 * NB: createDB+dropSchema is useless because if you're creating the DB,
	 * there's no need to drop individual items in it.  Moreover, if we tried
	 * to do that then we'd issue the drops in the database initially
	 * connected to, not the one we will create, which is very bad...
	 */
	if (ropt->createDB && ropt->dropSchema)
		die_horribly(AH, modulename, "-C and -c are incompatible options\n");

	/*
	 * -C is not compatible with -1, because we can't create a database inside
	 * a transaction block.
	 */
	if (ropt->createDB && ropt->single_txn)
		die_horribly(AH, modulename, "-C and -1 are incompatible options\n");

	/*
	 * If we're going to do parallel restore, there are some restrictions.
	 */
	parallel_mode = (AH->public.numWorkers > 1 && ropt->useDB);
	if (parallel_mode)
	{
		/* We haven't got round to making this work for all archive formats */
		if (AH->ClonePtr == NULL || AH->ReopenPtr == NULL)
			die_horribly(AH, modulename, "parallel restore is not supported with this archive file format\n");

		/* Doesn't work if the archive represents dependencies as OIDs */
		if (AH->version < K_VERS_1_8)
			die_horribly(AH, modulename, "parallel restore is not supported with archives made by pre-8.0 pg_dump\n");

		/*
		 * It's also not gonna work if we can't reopen the input file, so
		 * let's try that immediately.
		 */
		(AH->ReopenPtr) (AH);
	}

	/*
	 * Make sure we won't need (de)compression we haven't got
	 */
#ifndef HAVE_LIBZ
	if (AH->compression != 0 && AH->PrintTocDataPtr !=NULL)
	{
		for (te = AH->toc->next; te != AH->toc; te = te->next)
		{
			reqs = _tocEntryRequired(te, ropt, false);
			if (te->hadDumper && (reqs & REQ_DATA) != 0)
				die_horribly(AH, modulename, "cannot restore from compressed archive (compression not supported in this installation)\n");
		}
	}
#endif

	/*
	 * If we're using a DB connection, then connect it.
	 */
	if (ropt->useDB)
	{
		ahlog(AH, 1, "connecting to database for restore\n");
		if (AH->version < K_VERS_1_3)
			die_horribly(AH, modulename, "direct database connections are not supported in pre-1.3 archives\n");

		/* XXX Should get this from the archive */
		AHX->minRemoteVersion = 070100;
		AHX->maxRemoteVersion = 999999;

		ConnectDatabase(AHX, ropt->connParams.dbname,
							 ropt->connParams.pghost,
							 ropt->connParams.pgport,
							 ropt->connParams.username,
							 ropt->promptPassword);

		/*
		 * If we're talking to the DB directly, don't send comments since they
		 * obscure SQL when displaying errors
		 */
		AH->noTocComments = 1;
	}

	/*
	 * Work out if we have an implied data-only restore. This can happen if
	 * the dump was data only or if the user has used a toc list to exclude
	 * all of the schema data. All we do is look for schema entries - if none
	 * are found then we set the dataOnly flag.
	 *
	 * We could scan for wanted TABLE entries, but that is not the same as
	 * dataOnly. At this stage, it seems unnecessary (6-Mar-2001).
	 */
	if (!ropt->dataOnly)
	{
		int			impliedDataOnly = 1;

		for (te = AH->toc->next; te != AH->toc; te = te->next)
		{
			reqs = _tocEntryRequired(te, ropt, true);
			if ((reqs & REQ_SCHEMA) != 0)
			{					/* It's schema, and it's wanted */
				impliedDataOnly = 0;
				break;
			}
		}
		if (impliedDataOnly)
		{
			ropt->dataOnly = impliedDataOnly;
			ahlog(AH, 1, "implied data-only restore\n");
		}
	}

	/*
	 * Setup the output file if necessary.
	 */
	sav = SaveOutput(AH);
	if (ropt->filename || ropt->compression)
		SetOutput(AH, ropt->filename, ropt->compression);

	ahprintf(AH, "--\n-- PostgreSQL database dump\n--\n\n");

	if (AH->public.verbose)
	{
		if (AH->archiveRemoteVersion)
			ahprintf(AH, "-- Dumped from database version %s\n",
					 AH->archiveRemoteVersion);
		if (AH->archiveDumpVersion)
			ahprintf(AH, "-- Dumped by pg_dump version %s\n",
					 AH->archiveDumpVersion);
		dumpTimestamp(AH, "Started on", AH->createDate);
	}

	if (ropt->single_txn)
	{
		if (AH->connection)
			StartTransaction(AH);
		else
			ahprintf(AH, "BEGIN;\n\n");
	}

	/*
	 * Establish important parameter values right away.
	 */
	_doSetFixedOutputState(AH);

	AH->stage = STAGE_PROCESSING;

	/*
	 * Drop the items at the start, in reverse order
	 */
	if (ropt->dropSchema)
	{
		for (te = AH->toc->prev; te != AH->toc; te = te->prev)
		{
			AH->currentTE = te;

			reqs = _tocEntryRequired(te, ropt, false /* needn't drop ACLs */ );
			/* We want anything that's selected and has a dropStmt */
			if (((reqs & (REQ_SCHEMA | REQ_DATA)) != 0) && te->dropStmt)
			{
				ahlog(AH, 1, "dropping %s %s\n", te->desc, te->tag);
				/* Select owner and schema as necessary */
				_becomeOwner(AH, te);
				_selectOutputSchema(AH, te->namespace);
				/* Drop it */
				ahprintf(AH, "%s", te->dropStmt);
			}
		}

		/*
		 * _selectOutputSchema may have set currSchema to reflect the effect
		 * of a "SET search_path" command it emitted.  However, by now we may
		 * have dropped that schema; or it might not have existed in the first
		 * place.  In either case the effective value of search_path will not
		 * be what we think.  Forcibly reset currSchema so that we will
		 * re-establish the search_path setting when needed (after creating
		 * the schema).
		 *
		 * If we treated users as pg_dump'able objects then we'd need to reset
		 * currUser here too.
		 */
		if (AH->currSchema)
			free(AH->currSchema);
		AH->currSchema = NULL;
	}

	/*
	 * In serial mode, we now process each non-ACL TOC entry.
	 *
	 * In parallel mode, turn control over to the parallel-restore logic.
	 */
	if (parallel_mode)
	{
		ParallelState *pstate;
		/* ParallelBackupStart() will actually fork the processes */
		pstate = ParallelBackupStart(AH, ropt);
		restore_toc_entries_parallel(AH, pstate);
		ParallelBackupEnd(AH, pstate);
	}
	else
	{
		for (te = AH->toc->next; te != AH->toc; te = te->next)
			(void) restore_toc_entry(AH, te, ropt, false);
	}

	/*
	 * Scan TOC again to output ownership commands and ACLs
	 */
	for (te = AH->toc->next; te != AH->toc; te = te->next)
	{
		AH->currentTE = te;

		/* Work out what, if anything, we want from this entry */
		reqs = _tocEntryRequired(te, ropt, true);

		/* Both schema and data objects might now have ownership/ACLs */
		if ((reqs & (REQ_SCHEMA | REQ_DATA)) != 0)
		{
			ahlog(AH, 1, "setting owner and privileges for %s %s\n",
				  te->desc, te->tag);
			_printTocEntry(AH, te, ropt, false, true);
		}
	}

	if (ropt->single_txn)
	{
		if (AH->connection)
			CommitTransaction(AH);
		else
			ahprintf(AH, "COMMIT;\n\n");
	}

	if (AH->public.verbose)
		dumpTimestamp(AH, "Completed on", time(NULL));

	ahprintf(AH, "--\n-- PostgreSQL database dump complete\n--\n\n");

	/*
	 * Clean up & we're done.
	 */
	AH->stage = STAGE_FINALIZING;

	if (ropt->filename || ropt->compression)
		RestoreOutput(AH, sav);

	if (ropt->useDB)
	{
		PQfinish(AH->connection);
		AH->connection = NULL;
	}
}

/*
 * Restore a single TOC item.  Used in both parallel and non-parallel restore;
 * is_parallel is true if we are in a worker child process.
 *
 * Returns 0 normally, but WORKER_CREATE_DONE or WORKER_INHIBIT_DATA if
 * the parallel parent has to make the corresponding status update.
 */
static int
restore_toc_entry(ArchiveHandle *AH, TocEntry *te,
				  RestoreOptions *ropt, bool is_parallel)
{
	int			status = WORKER_OK;
	teReqs		reqs;
	bool		defnDumped;

	AH->currentTE = te;

	/* Work out what, if anything, we want from this entry */
	reqs = _tocEntryRequired(te, ropt, false);

	/* Dump any relevant dump warnings to stderr */
	if (!ropt->suppressDumpWarnings && strcmp(te->desc, "WARNING") == 0)
	{
		if (!ropt->dataOnly && te->defn != NULL && strlen(te->defn) != 0)
			write_msg(modulename, "warning from original dump file: %s\n", te->defn);
		else if (te->copyStmt != NULL && strlen(te->copyStmt) != 0)
			write_msg(modulename, "warning from original dump file: %s\n", te->copyStmt);
	}

	defnDumped = false;

	if ((reqs & REQ_SCHEMA) != 0)		/* We want the schema */
	{
		ahlog(AH, 1, "creating %s %s\n", te->desc, te->tag);

		_printTocEntry(AH, te, ropt, false, false);
		defnDumped = true;

		if (strcmp(te->desc, "TABLE") == 0)
		{
			if (AH->lastErrorTE == te)
			{
				/*
				 * We failed to create the table. If
				 * --no-data-for-failed-tables was given, mark the
				 * corresponding TABLE DATA to be ignored.
				 *
				 * In the parallel case this must be done in the parent, so we
				 * just set the return value.
				 */
				if (ropt->noDataForFailedTables)
				{
					if (is_parallel)
						status = WORKER_INHIBIT_DATA;
					else
						inhibit_data_for_failed_table(AH, te);
				}
			}
			else
			{
				/*
				 * We created the table successfully.  Mark the corresponding
				 * TABLE DATA for possible truncation.
				 *
				 * In the parallel case this must be done in the parent, so we
				 * just set the return value.
				 */
				if (is_parallel)
					status = WORKER_CREATE_DONE;
				else
					mark_create_done(AH, te);
			}
		}

		/* If we created a DB, connect to it... */
		if (strcmp(te->desc, "DATABASE") == 0)
		{
			ahlog(AH, 1, "connecting to new database \"%s\"\n", te->tag);
			_reconnectToDB(AH, te->tag);
			ropt->connParams.dbname = strdup(te->tag);
		}
	}

	/*
	 * If we have a data component, then process it
	 */
	if ((reqs & REQ_DATA) != 0)
	{
		/*
		 * hadDumper will be set if there is genuine data component for this
		 * node. Otherwise, we need to check the defn field for statements
		 * that need to be executed in data-only restores.
		 */
		if (te->hadDumper)
		{
			/*
			 * If we can output the data, then restore it.
			 */
			if (AH->PrintTocDataPtr !=NULL && (reqs & REQ_DATA) != 0)
			{
				_printTocEntry(AH, te, ropt, true, false);

				if (strcmp(te->desc, "BLOBS") == 0 ||
					strcmp(te->desc, "BLOB COMMENTS") == 0)
				{
					ahlog(AH, 1, "restoring %s\n", te->desc);

					_selectOutputSchema(AH, "pg_catalog");

					(*AH->PrintTocDataPtr) (AH, te, ropt);
				}
				else
				{
					_disableTriggersIfNecessary(AH, te, ropt);

					/* Select owner and schema as necessary */
					_becomeOwner(AH, te);
					_selectOutputSchema(AH, te->namespace);

					ahlog(AH, 1, "restoring data for table \"%s\"\n",
						  te->tag);

					/*
					 * In parallel restore, if we created the table earlier in
					 * the run then we wrap the COPY in a transaction and
					 * precede it with a TRUNCATE.	If archiving is not on
					 * this prevents WAL-logging the COPY.	This obtains a
					 * speedup similar to that from using single_txn mode in
					 * non-parallel restores.
					 */
					if (is_parallel && te->created)
					{
						/*
						 * Parallel restore is always talking directly to a
						 * server, so no need to see if we should issue BEGIN.
						 */
						StartTransaction(AH);

						/*
						 * If the server version is >= 8.4, make sure we issue
						 * TRUNCATE with ONLY so that child tables are not
						 * wiped.
						 */
						ahprintf(AH, "TRUNCATE TABLE %s%s;\n\n",
								 (PQserverVersion(AH->connection) >= 80400 ?
								  "ONLY " : ""),
								 fmtId(te->tag));
					}

					/*
					 * If we have a copy statement, use it.
					 */
					if (te->copyStmt && strlen(te->copyStmt) > 0)
					{
						ahprintf(AH, "%s", te->copyStmt);
						AH->writingCopyData = true;
					}

					(*AH->PrintTocDataPtr) (AH, te, ropt);

					/*
					 * Terminate COPY if needed.
					 */
					if (AH->writingCopyData)
					{
						if (RestoringToDB(AH))
							EndDBCopyMode(AH, te);
						AH->writingCopyData = false;
					}

					/* close out the transaction started above */
					if (is_parallel && te->created)
						CommitTransaction(AH);

					_enableTriggersIfNecessary(AH, te, ropt);
				}
			}
		}
		else if (!defnDumped)
		{
			/* If we haven't already dumped the defn part, do so now */
			ahlog(AH, 1, "executing %s %s\n", te->desc, te->tag);
			_printTocEntry(AH, te, ropt, false, false);
		}
	}

	if (AH->public.n_errors > 0 && status == WORKER_OK)
		status = WORKER_IGNORED_ERRORS;

	return status;
}

/*
 * Allocate a new RestoreOptions block.
 * This is mainly so we can initialize it, but also for future expansion,
 */
RestoreOptions *
NewRestoreOptions(void)
{
	RestoreOptions *opts;

	opts = (RestoreOptions *) calloc(1, sizeof(RestoreOptions));

	/* set any fields that shouldn't default to zeroes */
	opts->format = archUnknown;
	opts->promptPassword = TRI_DEFAULT;

	return opts;
}

static void
_disableTriggersIfNecessary(ArchiveHandle *AH, TocEntry *te, RestoreOptions *ropt)
{
	/* This hack is only needed in a data-only restore */
	if (!ropt->dataOnly || !ropt->disable_triggers)
		return;

	ahlog(AH, 1, "disabling triggers for %s\n", te->tag);

	/*
	 * Become superuser if possible, since they are the only ones who can
	 * disable constraint triggers.  If -S was not given, assume the initial
	 * user identity is a superuser.  (XXX would it be better to become the
	 * table owner?)
	 */
	_becomeUser(AH, ropt->superuser);

	/*
	 * Disable them.
	 */
	_selectOutputSchema(AH, te->namespace);

	ahprintf(AH, "ALTER TABLE %s DISABLE TRIGGER ALL;\n\n",
			 fmtId(te->tag));
}

static void
_enableTriggersIfNecessary(ArchiveHandle *AH, TocEntry *te, RestoreOptions *ropt)
{
	/* This hack is only needed in a data-only restore */
	if (!ropt->dataOnly || !ropt->disable_triggers)
		return;

	ahlog(AH, 1, "enabling triggers for %s\n", te->tag);

	/*
	 * Become superuser if possible, since they are the only ones who can
	 * disable constraint triggers.  If -S was not given, assume the initial
	 * user identity is a superuser.  (XXX would it be better to become the
	 * table owner?)
	 */
	_becomeUser(AH, ropt->superuser);

	/*
	 * Enable them.
	 */
	_selectOutputSchema(AH, te->namespace);

	ahprintf(AH, "ALTER TABLE %s ENABLE TRIGGER ALL;\n\n",
			 fmtId(te->tag));
}

/*
 * This is a routine that is part of the dumper interface, hence the 'Archive*' parameter.
 */

/* Public */
size_t
WriteData(Archive *AHX, const void *data, size_t dLen)
{
	ArchiveHandle *AH = (ArchiveHandle *) AHX;

	if (!AH->currToc)
		die_horribly(AH, modulename, "internal error -- WriteData cannot be called outside the context of a DataDumper routine\n");

	return (*AH->WriteDataPtr) (AH, data, dLen);
}

/*
 * Create a new TOC entry. The TOC was designed as a TOC, but is now the
 * repository for all metadata. But the name has stuck.
 */

/* Public */
void
ArchiveEntry(Archive *AHX,
			 CatalogId catalogId, DumpId dumpId,
			 const char *tag,
			 const char *namespace,
			 const char *tablespace,
			 const char *owner,
			 unsigned long int relpages, bool withOids,
			 const char *desc, teSection section,
			 const char *defn,
			 const char *dropStmt, const char *copyStmt,
			 const DumpId *deps, int nDeps,
			 DataDumperPtr dumpFn, void *dumpArg)
{
	ArchiveHandle *AH = (ArchiveHandle *) AHX;
	TocEntry   *newToc;

	newToc = (TocEntry *) calloc(1, sizeof(TocEntry));
	if (!newToc)
		die_horribly(AH, modulename, "out of memory\n");

	AH->tocCount++;
	if (dumpId > AH->maxDumpId)
		AH->maxDumpId = dumpId;

	newToc->prev = AH->toc->prev;
	newToc->next = AH->toc;
	AH->toc->prev->next = newToc;
	AH->toc->prev = newToc;

	newToc->catalogId = catalogId;
	newToc->dumpId = dumpId;
	newToc->section = section;

	newToc->tag = strdup(tag);
	newToc->namespace = namespace ? strdup(namespace) : NULL;
	newToc->tablespace = tablespace ? strdup(tablespace) : NULL;
	newToc->owner = strdup(owner);
	newToc->withOids = withOids;
	newToc->desc = strdup(desc);
	newToc->defn = strdup(defn);
	newToc->dropStmt = strdup(dropStmt);
	newToc->copyStmt = copyStmt ? strdup(copyStmt) : NULL;

	if (nDeps > 0)
	{
		newToc->dependencies = (DumpId *) malloc(nDeps * sizeof(DumpId));
		memcpy(newToc->dependencies, deps, nDeps * sizeof(DumpId));
		newToc->nDeps = nDeps;
	}
	else
	{
		newToc->dependencies = NULL;
		newToc->nDeps = 0;
	}

	newToc->dataDumper = dumpFn;
	newToc->dataDumperArg = dumpArg;
	newToc->hadDumper = dumpFn ? true : false;

	newToc->formatData = NULL;

	if (AH->ArchiveEntryPtr !=NULL)
		(*AH->ArchiveEntryPtr) (AH, newToc);
}

/* Public */
void
PrintTOCSummary(Archive *AHX, RestoreOptions *ropt)
{
	ArchiveHandle *AH = (ArchiveHandle *) AHX;
	TocEntry   *te;
	OutputContext sav;
	char	   *fmtName;

	sav = SaveOutput(AH);
	if (ropt->filename)
		SetOutput(AH, ropt->filename, 0 /* no compression */ );

	ahprintf(AH, ";\n; Archive created at %s", ctime(&AH->createDate));
	ahprintf(AH, ";     dbname: %s\n;     TOC Entries: %d\n;     Compression: %d\n",
			 AH->archdbname, AH->tocCount, AH->compression);

	switch (AH->format)
	{
		case archFiles:
			fmtName = "FILES";
			break;
		case archCustom:
			fmtName = "CUSTOM";
			break;
		case archTar:
			fmtName = "TAR";
			break;
		default:
			fmtName = "UNKNOWN";
	}

	ahprintf(AH, ";     Dump Version: %d.%d-%d\n", AH->vmaj, AH->vmin, AH->vrev);
	ahprintf(AH, ";     Format: %s\n", fmtName);
	ahprintf(AH, ";     Integer: %d bytes\n", (int) AH->intSize);
	ahprintf(AH, ";     Offset: %d bytes\n", (int) AH->offSize);
	if (AH->archiveRemoteVersion)
		ahprintf(AH, ";     Dumped from database version: %s\n",
				 AH->archiveRemoteVersion);
	if (AH->archiveDumpVersion)
		ahprintf(AH, ";     Dumped by pg_dump version: %s\n",
				 AH->archiveDumpVersion);

	ahprintf(AH, ";\n;\n; Selected TOC Entries:\n;\n");

	/* We should print DATABASE entries whether or not -C was specified */
	ropt->createDB = 1;

	for (te = AH->toc->next; te != AH->toc; te = te->next)
	{
		if (ropt->verbose || _tocEntryRequired(te, ropt, true) != 0)
			ahprintf(AH, "%d; %u %u %s %s %s %s\n", te->dumpId,
					 te->catalogId.tableoid, te->catalogId.oid,
					 te->desc, te->namespace ? te->namespace : "-",
					 te->tag, te->owner);
		if (ropt->verbose && te->nDeps > 0)
		{
			int			i;

			ahprintf(AH, ";\tdepends on:");
			for (i = 0; i < te->nDeps; i++)
				ahprintf(AH, " %d", te->dependencies[i]);
			ahprintf(AH, "\n");
		}
	}

	if (ropt->filename)
		RestoreOutput(AH, sav);
}

/***********
 * BLOB Archival
 ***********/

/* Called by a dumper to signal start of a BLOB */
int
StartBlob(Archive *AHX, Oid oid)
{
	ArchiveHandle *AH = (ArchiveHandle *) AHX;

	if (!AH->StartBlobPtr)
		die_horribly(AH, modulename, "large-object output not supported in chosen format\n");

	(*AH->StartBlobPtr) (AH, AH->currToc, oid);

	return 1;
}

/* Called by a dumper to signal end of a BLOB */
int
EndBlob(Archive *AHX, Oid oid)
{
	ArchiveHandle *AH = (ArchiveHandle *) AHX;

	if (AH->EndBlobPtr)
		(*AH->EndBlobPtr) (AH, AH->currToc, oid);

	return 1;
}

/**********
 * BLOB Restoration
 **********/

/*
 * Called by a format handler before any blobs are restored
 */
void
StartRestoreBlobs(ArchiveHandle *AH)
{
	if (!AH->ropt->single_txn)
	{
		if (AH->connection)
			StartTransaction(AH);
		else
			ahprintf(AH, "BEGIN;\n\n");
	}

	AH->blobCount = 0;
}

/*
 * Called by a format handler after all blobs are restored
 */
void
EndRestoreBlobs(ArchiveHandle *AH)
{
	if (!AH->ropt->single_txn)
	{
		if (AH->connection)
			CommitTransaction(AH);
		else
			ahprintf(AH, "COMMIT;\n\n");
	}

	ahlog(AH, 1, ngettext("restored %d large object\n",
						  "restored %d large objects\n",
						  AH->blobCount),
		  AH->blobCount);
}


/*
 * Called by a format handler to initiate restoration of a blob
 */
void
StartRestoreBlob(ArchiveHandle *AH, Oid oid, bool drop)
{
	bool		old_blob_style = (AH->version < K_VERS_1_12);
	Oid			loOid;

	AH->blobCount++;

	/* Initialize the LO Buffer */
	AH->lo_buf_used = 0;

	ahlog(AH, 2, "restoring large object with OID %u\n", oid);

	/* With an old archive we must do drop and create logic here */
	if (old_blob_style && drop)
		DropBlobIfExists(AH, oid);

	if (AH->connection)
	{
		if (old_blob_style)
		{
			loOid = lo_create(AH->connection, oid);
			if (loOid == 0 || loOid != oid)
				die_horribly(AH, modulename, "could not create large object %u: %s",
							 oid, PQerrorMessage(AH->connection));
		}
		AH->loFd = lo_open(AH->connection, oid, INV_WRITE);
		if (AH->loFd == -1)
			die_horribly(AH, modulename, "could not open large object %u: %s",
						 oid, PQerrorMessage(AH->connection));
	}
	else
	{
		if (old_blob_style)
			ahprintf(AH, "SELECT pg_catalog.lo_open(pg_catalog.lo_create('%u'), %d);\n",
					 oid, INV_WRITE);
		else
			ahprintf(AH, "SELECT pg_catalog.lo_open('%u', %d);\n",
					 oid, INV_WRITE);
	}

	AH->writingBlob = 1;
}

void
EndRestoreBlob(ArchiveHandle *AH, Oid oid)
{
	if (AH->lo_buf_used > 0)
	{
		/* Write remaining bytes from the LO buffer */
		dump_lo_buf(AH);
	}

	AH->writingBlob = 0;

	if (AH->connection)
	{
		lo_close(AH->connection, AH->loFd);
		AH->loFd = -1;
	}
	else
	{
		ahprintf(AH, "SELECT pg_catalog.lo_close(0);\n\n");
	}
}

/***********
 * Sorting and Reordering
 ***********/

void
SortTocFromFile(Archive *AHX, RestoreOptions *ropt)
{
	ArchiveHandle *AH = (ArchiveHandle *) AHX;
	FILE	   *fh;
	char		buf[100];
	bool		incomplete_line;

	/* Allocate space for the 'wanted' array, and init it */
	ropt->idWanted = (bool *) malloc(sizeof(bool) * AH->maxDumpId);
	memset(ropt->idWanted, 0, sizeof(bool) * AH->maxDumpId);

	/* Setup the file */
	fh = fopen(ropt->tocFile, PG_BINARY_R);
	if (!fh)
		die_horribly(AH, modulename, "could not open TOC file \"%s\": %s\n",
					 ropt->tocFile, strerror(errno));

	incomplete_line = false;
	while (fgets(buf, sizeof(buf), fh) != NULL)
	{
		bool		prev_incomplete_line = incomplete_line;
		int			buflen;
		char	   *cmnt;
		char	   *endptr;
		DumpId		id;
		TocEntry   *te;

		/*
		 * Some lines in the file might be longer than sizeof(buf).  This is
		 * no problem, since we only care about the leading numeric ID which
		 * can be at most a few characters; but we have to skip continuation
		 * bufferloads when processing a long line.
		 */
		buflen = strlen(buf);
		if (buflen > 0 && buf[buflen - 1] == '\n')
			incomplete_line = false;
		else
			incomplete_line = true;
		if (prev_incomplete_line)
			continue;

		/* Truncate line at comment, if any */
		cmnt = strchr(buf, ';');
		if (cmnt != NULL)
			cmnt[0] = '\0';

		/* Ignore if all blank */
		if (strspn(buf, " \t\r\n") == strlen(buf))
			continue;

		/* Get an ID, check it's valid and not already seen */
		id = strtol(buf, &endptr, 10);
		if (endptr == buf || id <= 0 || id > AH->maxDumpId ||
			ropt->idWanted[id - 1])
		{
			write_msg(modulename, "WARNING: line ignored: %s\n", buf);
			continue;
		}

		/* Find TOC entry */
		te = getTocEntryByDumpId(AH, id);
		if (!te)
			die_horribly(AH, modulename, "could not find entry for ID %d\n",
						 id);

		/* Mark it wanted */
		ropt->idWanted[id - 1] = true;

		/*
		 * Move each item to the end of the list as it is selected, so that
		 * they are placed in the desired order.  Any unwanted items will end
		 * up at the front of the list, which may seem unintuitive but it's
		 * what we need.  In an ordinary serial restore that makes no
		 * difference, but in a parallel restore we need to mark unrestored
		 * items' dependencies as satisfied before we start examining
		 * restorable items.  Otherwise they could have surprising
		 * side-effects on the order in which restorable items actually get
		 * restored.
		 */
		_moveBefore(AH, AH->toc, te);
	}

	if (fclose(fh) != 0)
		die_horribly(AH, modulename, "could not close TOC file: %s\n",
					 strerror(errno));
}

/*
 * Set up a dummy ID filter that selects all dump IDs
 */
void
InitDummyWantedList(Archive *AHX, RestoreOptions *ropt)
{
	ArchiveHandle *AH = (ArchiveHandle *) AHX;

	/* Allocate space for the 'wanted' array, and init it to 1's */
	ropt->idWanted = (bool *) malloc(sizeof(bool) * AH->maxDumpId);
	memset(ropt->idWanted, 1, sizeof(bool) * AH->maxDumpId);
}

/**********************
 * 'Convenience functions that look like standard IO functions
 * for writing data when in dump mode.
 **********************/

/* Public */
int
archputs(const char *s, Archive *AH)
{
	return WriteData(AH, s, strlen(s));
}

/* Public */
int
archprintf(Archive *AH, const char *fmt,...)
{
	char	   *p = NULL;
	va_list		ap;
	int			bSize = strlen(fmt) + 256;
	int			cnt = -1;

	/*
	 * This is paranoid: deal with the possibility that vsnprintf is willing
	 * to ignore trailing null or returns > 0 even if string does not fit. It
	 * may be the case that it returns cnt = bufsize
	 */
	while (cnt < 0 || cnt >= (bSize - 1))
	{
		if (p != NULL)
			free(p);
		bSize *= 2;
		p = (char *) malloc(bSize);
		if (p == NULL)
			exit_horribly(AH, modulename, "out of memory\n");
		va_start(ap, fmt);
		cnt = vsnprintf(p, bSize, fmt, ap);
		va_end(ap);
	}
	WriteData(AH, p, cnt);
	free(p);
	return cnt;
}


/*******************************
 * Stuff below here should be 'private' to the archiver routines
 *******************************/

static void
SetOutput(ArchiveHandle *AH, char *filename, int compression)
{
	int			fn;

	if (filename)
		fn = -1;
	else if (AH->FH)
		fn = fileno(AH->FH);
	else if (AH->fSpec)
	{
		fn = -1;
		filename = AH->fSpec;
	}
	else
		fn = fileno(stdout);

	/* If compression explicitly requested, use gzopen */
#ifdef HAVE_LIBZ
	if (compression != 0)
	{
		char		fmode[10];

		/* Don't use PG_BINARY_x since this is zlib */
		sprintf(fmode, "wb%d", compression);
		if (fn >= 0)
			AH->OF = gzdopen(dup(fn), fmode);
		else
			AH->OF = gzopen(filename, fmode);
		AH->gzOut = 1;
	}
	else
#endif
	{							/* Use fopen */
		if (AH->mode == archModeAppend)
		{
			if (fn >= 0)
				AH->OF = fdopen(dup(fn), PG_BINARY_A);
			else
				AH->OF = fopen(filename, PG_BINARY_A);
		}
		else
		{
			if (fn >= 0)
				AH->OF = fdopen(dup(fn), PG_BINARY_W);
			else
				AH->OF = fopen(filename, PG_BINARY_W);
		}
		AH->gzOut = 0;
	}

	if (!AH->OF)
	{
		if (filename)
			die_horribly(AH, modulename, "could not open output file \"%s\": %s\n",
						 filename, strerror(errno));
		else
			die_horribly(AH, modulename, "could not open output file: %s\n",
						 strerror(errno));
	}
}

static OutputContext
SaveOutput(ArchiveHandle *AH)
{
	OutputContext sav;

	sav.OF = AH->OF;
	sav.gzOut = AH->gzOut;

	return sav;
}

static void
RestoreOutput(ArchiveHandle *AH, OutputContext savedContext)
{
	int			res;

	if (AH->gzOut)
		res = GZCLOSE(AH->OF);
	else
		res = fclose(AH->OF);

	if (res != 0)
		die_horribly(AH, modulename, "could not close output file: %s\n",
					 strerror(errno));

	AH->gzOut = savedContext.gzOut;
	AH->OF = savedContext.OF;
}



/*
 *	Print formatted text to the output file (usually stdout).
 */
int
ahprintf(ArchiveHandle *AH, const char *fmt,...)
{
	char	   *p = NULL;
	va_list		ap;
	int			bSize = strlen(fmt) + 256;		/* Usually enough */
	int			cnt = -1;

	/*
	 * This is paranoid: deal with the possibility that vsnprintf is willing
	 * to ignore trailing null or returns > 0 even if string does not fit.
	 * It may be the case that it returns cnt = bufsize.
	 */
	while (cnt < 0 || cnt >= (bSize - 1))
	{
		if (p != NULL)
			free(p);
		bSize *= 2;
		p = (char *) malloc(bSize);
		if (p == NULL)
			die_horribly(AH, modulename, "out of memory\n");
		va_start(ap, fmt);
		cnt = vsnprintf(p, bSize, fmt, ap);
		va_end(ap);
	}
	ahwrite(p, 1, cnt, AH);
	free(p);
	return cnt;
}

void
ahlog(ArchiveHandle *AH, int level, const char *fmt,...)
{
	va_list		ap;

	if (AH->debugLevel < level && (!AH->public.verbose || level > 1))
		return;

	va_start(ap, fmt);
	_write_msg(NULL, fmt, ap);
	va_end(ap);
}

/*
 * Single place for logic which says 'We are restoring to a direct DB connection'.
 */
static int
RestoringToDB(ArchiveHandle *AH)
{
	return (AH->ropt && AH->ropt->useDB && AH->connection);
}

/*
 * Dump the current contents of the LO data buffer while writing a BLOB
 */
static void
dump_lo_buf(ArchiveHandle *AH)
{
	if (AH->connection)
	{
		size_t		res;

		res = lo_write(AH->connection, AH->loFd, AH->lo_buf, AH->lo_buf_used);
		ahlog(AH, 5, ngettext("wrote %lu byte of large object data (result = %lu)\n",
					 "wrote %lu bytes of large object data (result = %lu)\n",
							  AH->lo_buf_used),
			  (unsigned long) AH->lo_buf_used, (unsigned long) res);
		if (res != AH->lo_buf_used)
			die_horribly(AH, modulename,
			"could not write to large object (result: %lu, expected: %lu)\n",
					   (unsigned long) res, (unsigned long) AH->lo_buf_used);
	}
	else
	{
		PQExpBuffer buf = createPQExpBuffer();

		appendByteaLiteralAHX(buf,
							  (const unsigned char *) AH->lo_buf,
							  AH->lo_buf_used,
							  AH);

		/* Hack: turn off writingBlob so ahwrite doesn't recurse to here */
		AH->writingBlob = 0;
		ahprintf(AH, "SELECT pg_catalog.lowrite(0, %s);\n", buf->data);
		AH->writingBlob = 1;

		destroyPQExpBuffer(buf);
	}
	AH->lo_buf_used = 0;
}


/*
 *	Write buffer to the output file (usually stdout). This is used for
 *	outputting 'restore' scripts etc. It is even possible for an archive
 *	format to create a custom output routine to 'fake' a restore if it
 *	wants to generate a script (see TAR output).
 */
int
ahwrite(const void *ptr, size_t size, size_t nmemb, ArchiveHandle *AH)
{
	size_t		res;

	if (AH->writingBlob)
	{
		size_t		remaining = size * nmemb;

		while (AH->lo_buf_used + remaining > AH->lo_buf_size)
		{
			size_t		avail = AH->lo_buf_size - AH->lo_buf_used;

			memcpy((char *) AH->lo_buf + AH->lo_buf_used, ptr, avail);
			ptr = (const void *) ((const char *) ptr + avail);
			remaining -= avail;
			AH->lo_buf_used += avail;
			dump_lo_buf(AH);
		}

		memcpy((char *) AH->lo_buf + AH->lo_buf_used, ptr, remaining);
		AH->lo_buf_used += remaining;

		return size * nmemb;
	}
	else if (AH->gzOut)
	{
		res = GZWRITE(ptr, size, nmemb, AH->OF);
		if (res != (nmemb * size))
			die_horribly(AH, modulename, "could not write to output file: %s\n", strerror(errno));
		return res;
	}
	else if (AH->CustomOutPtr)
	{
		res = AH->CustomOutPtr (AH, ptr, size * nmemb);

		if (res != (nmemb * size))
			die_horribly(AH, modulename, "could not write to custom output routine\n");
		return res;
	}
	else
	{
		/*
		 * If we're doing a restore, and it's direct to DB, and we're
		 * connected then send it to the DB.
		 */
		if (RestoringToDB(AH))
			return ExecuteSqlCommandBuf(AH, (const char *) ptr, size * nmemb);
		else
		{
			res = fwrite(ptr, size, nmemb, AH->OF);
			if (res != nmemb)
				die_horribly(AH, modulename, "could not write to output file: %s\n",
							 strerror(errno));
			return res;
		}
	}
}

/* Common exit code */
static void
_write_msg(const char *modulename, const char *fmt, va_list ap)
{
	if (modulename)
		fprintf(stderr, "%s: [%s] ", progname, _(modulename));
	else
		fprintf(stderr, "%s: ", progname);
	vfprintf(stderr, _(fmt), ap);
}

void
write_msg(const char *modulename, const char *fmt,...)
{
	va_list		ap;

	va_start(ap, fmt);
	_write_msg(modulename, fmt, ap);
	va_end(ap);
}


static void
_die_horribly(ArchiveHandle *AH, const char *modulename, const char *fmt, va_list ap)
{
	_write_msg(modulename, fmt, ap);

	if (AH)
	{
		if (AH->public.verbose)
			write_msg(NULL, "*** aborted because of error\n");
		if (AH->connection) {
			PQfinish(AH->connection);
			AH->connection = NULL;
		}
	}

	myExit(1);
}

/* External use */
void
exit_horribly(Archive *AH, const char *modulename, const char *fmt,...)
{
	va_list		ap;

	va_start(ap, fmt);
	_die_horribly((ArchiveHandle *) AH, modulename, fmt, ap);
	va_end(ap);
}

/* Archiver use (just different arg declaration) */
void
die_horribly(ArchiveHandle *AH, const char *modulename, const char *fmt,...)
{
	va_list		ap;

	va_start(ap, fmt);
	_die_horribly(AH, modulename, fmt, ap);
	va_end(ap);
}

/* on some error, we may decide to go on... */
void
warn_or_die_horribly(ArchiveHandle *AH,
					 const char *modulename, const char *fmt,...)
{
	va_list		ap;

	switch (AH->stage)
	{

		case STAGE_NONE:
			/* Do nothing special */
			break;

		case STAGE_INITIALIZING:
			if (AH->stage != AH->lastErrorStage)
				write_msg(modulename, "Error while INITIALIZING:\n");
			break;

		case STAGE_PROCESSING:
			if (AH->stage != AH->lastErrorStage)
				write_msg(modulename, "Error while PROCESSING TOC:\n");
			break;

		case STAGE_FINALIZING:
			if (AH->stage != AH->lastErrorStage)
				write_msg(modulename, "Error while FINALIZING:\n");
			break;
	}
	if (AH->currentTE != NULL && AH->currentTE != AH->lastErrorTE)
	{
		write_msg(modulename, "Error from TOC entry %d; %u %u %s %s %s\n",
				  AH->currentTE->dumpId,
			 AH->currentTE->catalogId.tableoid, AH->currentTE->catalogId.oid,
			  AH->currentTE->desc, AH->currentTE->tag, AH->currentTE->owner);
	}
	AH->lastErrorStage = AH->stage;
	AH->lastErrorTE = AH->currentTE;

	va_start(ap, fmt);
	if (AH->public.exit_on_error)
		_die_horribly(AH, modulename, fmt, ap);
	else
	{
		_write_msg(modulename, fmt, ap);
		AH->public.n_errors++;
	}
	va_end(ap);
}

#ifdef NOT_USED

static void
_moveAfter(ArchiveHandle *AH, TocEntry *pos, TocEntry *te)
{
	/* Unlink te from list */
	te->prev->next = te->next;
	te->next->prev = te->prev;

	/* and insert it after "pos" */
	te->prev = pos;
	te->next = pos->next;
	pos->next->prev = te;
	pos->next = te;
}
#endif

static void
_moveBefore(ArchiveHandle *AH, TocEntry *pos, TocEntry *te)
{
	/* Unlink te from list */
	te->prev->next = te->next;
	te->next->prev = te->prev;

	/* and insert it before "pos" */
	te->prev = pos->prev;
	te->next = pos;
	pos->prev->next = te;
	pos->prev = te;
}

static TocEntry *
getTocEntryByDumpId(ArchiveHandle *AH, DumpId id)
{
	TocEntry   *te;

	for (te = AH->toc->next; te != AH->toc; te = te->next)
	{
		if (te->dumpId == id)
			return te;
	}
	return NULL;
}

teReqs
TocIDRequired(ArchiveHandle *AH, DumpId id, RestoreOptions *ropt)
{
	TocEntry   *te = getTocEntryByDumpId(AH, id);

	if (!te)
		return 0;

	return _tocEntryRequired(te, ropt, true);
}

size_t
WriteOffset(ArchiveHandle *AH, pgoff_t o, int wasSet)
{
	int			off;

	/* Save the flag */
	(*AH->WriteBytePtr) (AH, wasSet);

	/* Write out pgoff_t smallest byte first, prevents endian mismatch */
	for (off = 0; off < sizeof(pgoff_t); off++)
	{
		(*AH->WriteBytePtr) (AH, o & 0xFF);
		o >>= 8;
	}
	return sizeof(pgoff_t) + 1;
}

int
ReadOffset(ArchiveHandle *AH, pgoff_t * o)
{
	int			i;
	int			off;
	int			offsetFlg;

	/* Initialize to zero */
	*o = 0;

	/* Check for old version */
	if (AH->version < K_VERS_1_7)
	{
		/* Prior versions wrote offsets using WriteInt */
		i = ReadInt(AH);
		/* -1 means not set */
		if (i < 0)
			return K_OFFSET_POS_NOT_SET;
		else if (i == 0)
			return K_OFFSET_NO_DATA;

		/* Cast to pgoff_t because it was written as an int. */
		*o = (pgoff_t) i;
		return K_OFFSET_POS_SET;
	}

	/*
	 * Read the flag indicating the state of the data pointer. Check if valid
	 * and die if not.
	 *
	 * This used to be handled by a negative or zero pointer, now we use an
	 * extra byte specifically for the state.
	 */
	offsetFlg = (*AH->ReadBytePtr) (AH) & 0xFF;

	switch (offsetFlg)
	{
		case K_OFFSET_POS_NOT_SET:
		case K_OFFSET_NO_DATA:
		case K_OFFSET_POS_SET:

			break;

		default:
			die_horribly(AH, modulename, "unexpected data offset flag %d\n", offsetFlg);
	}

	/*
	 * Read the bytes
	 */
	for (off = 0; off < AH->offSize; off++)
	{
		if (off < sizeof(pgoff_t))
			*o |= ((pgoff_t) ((*AH->ReadBytePtr) (AH))) << (off * 8);
		else
		{
			if ((*AH->ReadBytePtr) (AH) != 0)
				die_horribly(AH, modulename, "file offset in dump file is too large\n");
		}
	}

	return offsetFlg;
}

size_t
WriteInt(ArchiveHandle *AH, int i)
{
	int			b;

	/*
	 * This is a bit yucky, but I don't want to make the binary format very
	 * dependent on representation, and not knowing much about it, I write out
	 * a sign byte. If you change this, don't forget to change the file
	 * version #, and modify readInt to read the new format AS WELL AS the old
	 * formats.
	 */

	/* SIGN byte */
	if (i < 0)
	{
		(*AH->WriteBytePtr) (AH, 1);
		i = -i;
	}
	else
		(*AH->WriteBytePtr) (AH, 0);

	for (b = 0; b < AH->intSize; b++)
	{
		(*AH->WriteBytePtr) (AH, i & 0xFF);
		i >>= 8;
	}

	return AH->intSize + 1;
}

int
ReadInt(ArchiveHandle *AH)
{
	int			res = 0;
	int			bv,
				b;
	int			sign = 0;		/* Default positive */
	int			bitShift = 0;

	if (AH->version > K_VERS_1_0)
		/* Read a sign byte */
		sign = (*AH->ReadBytePtr) (AH);

	for (b = 0; b < AH->intSize; b++)
	{
		bv = (*AH->ReadBytePtr) (AH) & 0xFF;
		if (bv != 0)
			res = res + (bv << bitShift);
		bitShift += 8;
	}

	if (sign)
		res = -res;

	return res;
}

size_t
WriteStr(ArchiveHandle *AH, const char *c)
{
	size_t		res;

	if (c)
	{
		res = WriteInt(AH, strlen(c));
		res += (*AH->WriteBufPtr) (AH, c, strlen(c));
	}
	else
		res = WriteInt(AH, -1);

	return res;
}

char *
ReadStr(ArchiveHandle *AH)
{
	char	   *buf;
	int			l;

	l = ReadInt(AH);
	if (l < 0)
		buf = NULL;
	else
	{
		buf = (char *) malloc(l + 1);
		if (!buf)
			die_horribly(AH, modulename, "out of memory\n");

		if ((*AH->ReadBufPtr) (AH, (void *) buf, l) != l)
			die_horribly(AH, modulename, "unexpected end of file\n");

		buf[l] = '\0';
	}

	return buf;
}

static int
_discoverArchiveFormat(ArchiveHandle *AH)
{
	FILE	   *fh;
	char		sig[6];			/* More than enough */
	size_t		cnt;
	int			wantClose = 0;

#if 0
	write_msg(modulename, "attempting to ascertain archive format\n");
#endif

	if (AH->lookahead)
		free(AH->lookahead);

	AH->lookaheadSize = 512;
	AH->lookahead = calloc(1, 512);
	AH->lookaheadLen = 0;
	AH->lookaheadPos = 0;

	if (AH->fSpec)
	{
		struct stat st;

		wantClose = 1;

		/*
		 * Check if the specified archive is a directory. If so, check if
		 * there's a "toc.dat" (or "toc.dat.gz") file in it.
		 */
		if (stat(AH->fSpec, &st) == 0 && S_ISDIR(st.st_mode))
		{
			char		buf[MAXPGPATH];

			if (snprintf(buf, MAXPGPATH, "%s/toc.dat", AH->fSpec) >= MAXPGPATH)
				die_horribly(AH, modulename, "directory name too long: \"%s\"\n",
							 AH->fSpec);
			if (stat(buf, &st) == 0 && S_ISREG(st.st_mode))
			{
				AH->format = archDirectory;
				return AH->format;
			}

#ifdef HAVE_LIBZ
			if (snprintf(buf, MAXPGPATH, "%s/toc.dat.gz", AH->fSpec) >= MAXPGPATH)
				die_horribly(AH, modulename, "directory name too long: \"%s\"\n",
							 AH->fSpec);
			if (stat(buf, &st) == 0 && S_ISREG(st.st_mode))
			{
				AH->format = archDirectory;
				return AH->format;
			}
#endif
			die_horribly(AH, modulename, "directory \"%s\" does not appear to be a valid archive (\"toc.dat\" does not exist)\n",
						 AH->fSpec);
			fh = NULL;			/* keep compiler quiet */
		}
		else
		{
			fh = fopen(AH->fSpec, PG_BINARY_R);
			if (!fh)
				die_horribly(AH, modulename, "could not open input file \"%s\": %s\n",
							 AH->fSpec, strerror(errno));
		}
	}
	else
	{
		fh = stdin;
		if (!fh)
			die_horribly(AH, modulename, "could not open input file: %s\n",
						 strerror(errno));
	}

	cnt = fread(sig, 1, 5, fh);

	if (cnt != 5)
	{
		if (ferror(fh))
			die_horribly(AH, modulename, "could not read input file: %s\n", strerror(errno));
		else
			die_horribly(AH, modulename, "input file is too short (read %lu, expected 5)\n",
						 (unsigned long) cnt);
	}

	/* Save it, just in case we need it later */
	strncpy(&AH->lookahead[0], sig, 5);
	AH->lookaheadLen = 5;

	if (strncmp(sig, "PGDMP", 5) == 0)
	{
		/*
		 * Finish reading (most of) a custom-format header.
		 *
		 * NB: this code must agree with ReadHead().
		 */
		AH->vmaj = fgetc(fh);
		AH->vmin = fgetc(fh);

		/* Save these too... */
		AH->lookahead[AH->lookaheadLen++] = AH->vmaj;
		AH->lookahead[AH->lookaheadLen++] = AH->vmin;

		/* Check header version; varies from V1.0 */
		if (AH->vmaj > 1 || ((AH->vmaj == 1) && (AH->vmin > 0)))		/* Version > 1.0 */
		{
			AH->vrev = fgetc(fh);
			AH->lookahead[AH->lookaheadLen++] = AH->vrev;
		}
		else
			AH->vrev = 0;

		/* Make a convenient integer <maj><min><rev>00 */
		AH->version = ((AH->vmaj * 256 + AH->vmin) * 256 + AH->vrev) * 256 + 0;

		AH->intSize = fgetc(fh);
		AH->lookahead[AH->lookaheadLen++] = AH->intSize;

		if (AH->version >= K_VERS_1_7)
		{
			AH->offSize = fgetc(fh);
			AH->lookahead[AH->lookaheadLen++] = AH->offSize;
		}
		else
			AH->offSize = AH->intSize;

		AH->format = fgetc(fh);
		AH->lookahead[AH->lookaheadLen++] = AH->format;
	}
	else
	{
		/*
		 * *Maybe* we have a tar archive format file... So, read first 512
		 * byte header...
		 */
		cnt = fread(&AH->lookahead[AH->lookaheadLen], 1, 512 - AH->lookaheadLen, fh);
		AH->lookaheadLen += cnt;

		if (AH->lookaheadLen != 512)
			die_horribly(AH, modulename, "input file does not appear to be a valid archive (too short?)\n");

		if (!isValidTarHeader(AH->lookahead))
			die_horribly(AH, modulename, "input file does not appear to be a valid archive\n");

		AH->format = archTar;
	}

	/* If we can't seek, then mark the header as read */
	if (fseeko(fh, 0, SEEK_SET) != 0)
	{
		/*
		 * NOTE: Formats that use the lookahead buffer can unset this in their
		 * Init routine.
		 */
		AH->readHeader = 1;
	}
	else
		AH->lookaheadLen = 0;	/* Don't bother since we've reset the file */

	/* Close the file */
	if (wantClose)
		if (fclose(fh) != 0)
			die_horribly(AH, modulename, "could not close input file: %s\n",
						 strerror(errno));

	return AH->format;
}


/*
 * Allocate an archive handle
 */
static ArchiveHandle *
_allocAH(const char *FileSpec, const ArchiveFormat fmt,
		 const int compression, ArchiveMode mode)
{
	ArchiveHandle *AH;

#if 0
	write_msg(modulename, "allocating AH for %s, format %d\n", FileSpec, fmt);
#endif

	AH = (ArchiveHandle *) calloc(1, sizeof(ArchiveHandle));
	if (!AH)
		die_horribly(AH, modulename, "out of memory\n");

	/* AH->debugLevel = 100; */

	AH->vmaj = K_VERS_MAJOR;
	AH->vmin = K_VERS_MINOR;
	AH->vrev = K_VERS_REV;

	/* Make a convenient integer <maj><min><rev>00 */
	AH->version = ((AH->vmaj * 256 + AH->vmin) * 256 + AH->vrev) * 256 + 0;

	/* initialize for backwards compatible string processing */
	AH->public.encoding = 0;	/* PG_SQL_ASCII */
	AH->public.std_strings = false;

	/* sql error handling */
	AH->public.exit_on_error = true;
	AH->public.n_errors = 0;

	AH->archiveDumpVersion = PG_VERSION;

	AH->createDate = time(NULL);

	AH->intSize = sizeof(int);
	AH->offSize = sizeof(pgoff_t);
	if (FileSpec)
	{
		AH->fSpec = strdup(FileSpec);

		/*
		 * Not used; maybe later....
		 *
		 * AH->workDir = strdup(FileSpec); for(i=strlen(FileSpec) ; i > 0 ;
		 * i--) if (AH->workDir[i-1] == '/')
		 */
	}
	else
		AH->fSpec = NULL;

	AH->currUser = NULL;		/* unknown */
	AH->currSchema = NULL;		/* ditto */
	AH->currTablespace = NULL;	/* ditto */
	AH->currWithOids = -1;		/* force SET */

	AH->toc = (TocEntry *) calloc(1, sizeof(TocEntry));
	if (!AH->toc)
		die_horribly(AH, modulename, "out of memory\n");

	AH->toc->next = AH->toc;
	AH->toc->prev = AH->toc;

	AH->mode = mode;
	AH->compression = compression;

	/* Open stdout with no compression for AH output handle */
	AH->gzOut = 0;
	AH->OF = stdout;

	/*
	 * On Windows, we need to use binary mode to read/write non-text archive
	 * formats.  Force stdin/stdout into binary mode if that is what we are
	 * using.
	 */
#ifdef WIN32
	if (fmt != archNull &&
		(AH->fSpec == NULL || strcmp(AH->fSpec, "") == 0))
	{
		if (mode == archModeWrite)
			setmode(fileno(stdout), O_BINARY);
		else
			setmode(fileno(stdin), O_BINARY);
	}
#endif

	if (fmt == archUnknown)
		AH->format = _discoverArchiveFormat(AH);
	else
		AH->format = fmt;

	AH->promptPassword = TRI_DEFAULT;

	switch (AH->format)
	{
		case archCustom:
			InitArchiveFmt_Custom(AH);
			break;

		case archFiles:
			InitArchiveFmt_Files(AH);
			break;

		case archNull:
			InitArchiveFmt_Null(AH);
			break;

		case archDirectory:
			InitArchiveFmt_Directory(AH);
			break;

		case archTar:
			InitArchiveFmt_Tar(AH);
			break;

		default:
			die_horribly(AH, modulename, "unrecognized file format \"%d\"\n", fmt);
	}

	return AH;
}


void
WriteDataChunks(ArchiveHandle *AH)
{
	TocEntry	   *te;
	ParallelState  *pstate = NULL;

	if (AH->GetParallelStatePtr)
		pstate = (AH->GetParallelStatePtr)(AH);

	for (te = AH->toc->next; te != AH->toc; te = te->next)
	{
		if (!te->hadDumper)
			continue;

		printf("Dumping table %s (%d)\n", te->tag, te->dumpId);
		/*
		 * If we are in a parallel backup, we are always the master process.
		 */
		if (pstate)
		{
			int		ret_worker;
			int		work_status;

			for (;;)
			{
				int nTerm = 0;
				while ((ret_worker = ReapworkerStatus(pstate, &work_status)) != NO_SLOT)
				{
					if (work_status != 0)
						die_horribly(AH, modulename, "Error processing a parallel work item\n");

					nTerm++;
				}

				/* We need to make sure that we have an idle worker before dispatching
				 * the next item. If nTerm > 0 we already have that (quick check). */
				if (nTerm > 0)
					break;

				/* explicit check for an idle worker */
				if (GetIdleWorker(pstate) != NO_SLOT)
					break;

				/*
				 * If we have no idle worker, read the result of one or more
				 * workers and loop the loop to call ReapworkerStatus() on them
				 */
				ListenToWorkers(AH, pstate, true);
			}

			Assert(GetIdleWorker(pstate) != NO_SLOT);
			DispatchJobForTocEntry(AH, pstate, te, ACT_DUMP);
		}
		else
		{
			WriteDataChunksForTocEntry(AH, te);
		}
	}
	if (pstate)
	{
		int		work_status;

		/* Waiting for the remaining worker processes to finish */
		while (!IsEveryWorkerIdle(pstate))
		{
			if (ReapworkerStatus(pstate, &work_status) == NO_SLOT)
				ListenToWorkers(AH, pstate, true);

			if (work_status != 0)
				die_horribly(AH, modulename, "Error processing a parallel work item\n");
		}
	}
}

void
WriteDataChunksForTocEntry(ArchiveHandle *AH, TocEntry *te)
{
	StartDataPtr startPtr;
	EndDataPtr	endPtr;

	AH->currToc = te;

	if (strcmp(te->desc, "BLOBS") == 0)
	{
		startPtr = AH->StartBlobsPtr;
		endPtr = AH->EndBlobsPtr;
	}
	else
	{
		startPtr = AH->StartDataPtr;
		endPtr = AH->EndDataPtr;
	}

	if (startPtr != NULL)
		(*startPtr) (AH, te);

	/*
	 * The user-provided DataDumper routine needs to call
	 * AH->WriteData
	 */
	(*te->dataDumper) ((Archive *) AH, te->dataDumperArg);

	if (endPtr != NULL)
		(*endPtr) (AH, te);

	AH->currToc = NULL;
}

void
WriteToc(ArchiveHandle *AH)
{
	TocEntry   *te;
	char		workbuf[32];
	int			i;

	/* printf("%d TOC Entries to save\n", AH->tocCount); */

	WriteInt(AH, AH->tocCount);

	for (te = AH->toc->next; te != AH->toc; te = te->next)
	{
		WriteInt(AH, te->dumpId);
		WriteInt(AH, te->dataDumper ? 1 : 0);

		/* OID is recorded as a string for historical reasons */
		sprintf(workbuf, "%u", te->catalogId.tableoid);
		WriteStr(AH, workbuf);
		sprintf(workbuf, "%u", te->catalogId.oid);
		WriteStr(AH, workbuf);

		WriteStr(AH, te->tag);
		WriteStr(AH, te->desc);
		WriteInt(AH, te->section);
		WriteStr(AH, te->defn);
		WriteStr(AH, te->dropStmt);
		WriteStr(AH, te->copyStmt);
		WriteStr(AH, te->namespace);
		WriteStr(AH, te->tablespace);
		WriteStr(AH, te->owner);
		WriteStr(AH, te->withOids ? "true" : "false");

		/* Dump list of dependencies */
		for (i = 0; i < te->nDeps; i++)
		{
			sprintf(workbuf, "%d", te->dependencies[i]);
			WriteStr(AH, workbuf);
		}
		WriteStr(AH, NULL);		/* Terminate List */

		if (AH->WriteExtraTocPtr)
			(*AH->WriteExtraTocPtr) (AH, te);
	}
}

void
ReadToc(ArchiveHandle *AH)
{
	int			i;
	char	   *tmp;
	DumpId	   *deps;
	int			depIdx;
	int			depSize;
	TocEntry   *te;

	AH->tocCount = ReadInt(AH);
	AH->maxDumpId = 0;

	for (i = 0; i < AH->tocCount; i++)
	{
		te = (TocEntry *) calloc(1, sizeof(TocEntry));
		te->dumpId = ReadInt(AH);

		if (te->dumpId > AH->maxDumpId)
			AH->maxDumpId = te->dumpId;

		/* Sanity check */
		if (te->dumpId <= 0)
			die_horribly(AH, modulename,
					   "entry ID %d out of range -- perhaps a corrupt TOC\n",
						 te->dumpId);

		te->hadDumper = ReadInt(AH);

		if (AH->version >= K_VERS_1_8)
		{
			tmp = ReadStr(AH);
			sscanf(tmp, "%u", &te->catalogId.tableoid);
			free(tmp);
		}
		else
			te->catalogId.tableoid = InvalidOid;
		tmp = ReadStr(AH);
		sscanf(tmp, "%u", &te->catalogId.oid);
		free(tmp);

		te->tag = ReadStr(AH);
		te->desc = ReadStr(AH);

		if (AH->version >= K_VERS_1_11)
		{
			te->section = ReadInt(AH);
		}
		else
		{
			/*
			 * Rules for pre-8.4 archives wherein pg_dump hasn't classified
			 * the entries into sections.  This list need not cover entry
			 * types added later than 8.4.
			 */
			if (strcmp(te->desc, "COMMENT") == 0 ||
				strcmp(te->desc, "ACL") == 0 ||
				strcmp(te->desc, "ACL LANGUAGE") == 0)
				te->section = SECTION_NONE;
			else if (strcmp(te->desc, "TABLE DATA") == 0 ||
					 strcmp(te->desc, "BLOBS") == 0 ||
					 strcmp(te->desc, "BLOB COMMENTS") == 0)
				te->section = SECTION_DATA;
			else if (strcmp(te->desc, "CONSTRAINT") == 0 ||
					 strcmp(te->desc, "CHECK CONSTRAINT") == 0 ||
					 strcmp(te->desc, "FK CONSTRAINT") == 0 ||
					 strcmp(te->desc, "INDEX") == 0 ||
					 strcmp(te->desc, "RULE") == 0 ||
					 strcmp(te->desc, "TRIGGER") == 0)
				te->section = SECTION_POST_DATA;
			else
				te->section = SECTION_PRE_DATA;
		}

		te->defn = ReadStr(AH);
		te->dropStmt = ReadStr(AH);

		if (AH->version >= K_VERS_1_3)
			te->copyStmt = ReadStr(AH);

		if (AH->version >= K_VERS_1_6)
			te->namespace = ReadStr(AH);

		if (AH->version >= K_VERS_1_10)
			te->tablespace = ReadStr(AH);

		te->owner = ReadStr(AH);
		if (AH->version >= K_VERS_1_9)
		{
			if (strcmp(ReadStr(AH), "true") == 0)
				te->withOids = true;
			else
				te->withOids = false;
		}
		else
			te->withOids = true;

		/* Read TOC entry dependencies */
		if (AH->version >= K_VERS_1_5)
		{
			depSize = 100;
			deps = (DumpId *) malloc(sizeof(DumpId) * depSize);
			depIdx = 0;
			for (;;)
			{
				tmp = ReadStr(AH);
				if (!tmp)
					break;		/* end of list */
				if (depIdx >= depSize)
				{
					depSize *= 2;
					deps = (DumpId *) realloc(deps, sizeof(DumpId) * depSize);
				}
				sscanf(tmp, "%d", &deps[depIdx]);
				free(tmp);
				depIdx++;
			}

			if (depIdx > 0)		/* We have a non-null entry */
			{
				deps = (DumpId *) realloc(deps, sizeof(DumpId) * depIdx);
				te->dependencies = deps;
				te->nDeps = depIdx;
			}
			else
			{
				free(deps);
				te->dependencies = NULL;
				te->nDeps = 0;
			}
		}
		else
		{
			te->dependencies = NULL;
			te->nDeps = 0;
		}

		if (AH->ReadExtraTocPtr)
			(*AH->ReadExtraTocPtr) (AH, te);

		ahlog(AH, 3, "read TOC entry %d (ID %d) for %s %s\n",
			  i, te->dumpId, te->desc, te->tag);

		/* link completed entry into TOC circular list */
		te->prev = AH->toc->prev;
		AH->toc->prev->next = te;
		AH->toc->prev = te;
		te->next = AH->toc;

		/* special processing immediately upon read for some items */
		if (strcmp(te->desc, "ENCODING") == 0)
			processEncodingEntry(AH, te);
		else if (strcmp(te->desc, "STDSTRINGS") == 0)
			processStdStringsEntry(AH, te);
	}
}

static void
processEncodingEntry(ArchiveHandle *AH, TocEntry *te)
{
	/* te->defn should have the form SET client_encoding = 'foo'; */
	char	   *defn = strdup(te->defn);
	char	   *ptr1;
	char	   *ptr2 = NULL;
	int			encoding;

	ptr1 = strchr(defn, '\'');
	if (ptr1)
		ptr2 = strchr(++ptr1, '\'');
	if (ptr2)
	{
		*ptr2 = '\0';
		encoding = pg_char_to_encoding(ptr1);
		if (encoding < 0)
			die_horribly(AH, modulename, "unrecognized encoding \"%s\"\n",
						 ptr1);
		AH->public.encoding = encoding;
	}
	else
		die_horribly(AH, modulename, "invalid ENCODING item: %s\n",
					 te->defn);

	free(defn);
}

static void
processStdStringsEntry(ArchiveHandle *AH, TocEntry *te)
{
	/* te->defn should have the form SET standard_conforming_strings = 'x'; */
	char	   *ptr1;

	ptr1 = strchr(te->defn, '\'');
	if (ptr1 && strncmp(ptr1, "'on'", 4) == 0)
		AH->public.std_strings = true;
	else if (ptr1 && strncmp(ptr1, "'off'", 5) == 0)
		AH->public.std_strings = false;
	else
		die_horribly(AH, modulename, "invalid STDSTRINGS item: %s\n",
					 te->defn);
}

static teReqs
_tocEntryRequired(TocEntry *te, RestoreOptions *ropt, bool include_acls)
{
	teReqs		res = REQ_ALL;

	/* ENCODING and STDSTRINGS items are dumped specially, so always reject */
	if (strcmp(te->desc, "ENCODING") == 0 ||
		strcmp(te->desc, "STDSTRINGS") == 0)
		return 0;

	/* If it's an ACL, maybe ignore it */
	if ((!include_acls || ropt->aclsSkip) && _tocEntryIsACL(te))
		return 0;

	/* If it's security labels, maybe ignore it */
	if (ropt->no_security_labels && strcmp(te->desc, "SECURITY LABEL") == 0)
		return 0;

	/* Ignore DATABASE entry unless we should create it */
	if (!ropt->createDB && strcmp(te->desc, "DATABASE") == 0)
		return 0;

	/* Check options for selective dump/restore */
	if (ropt->schemaNames)
	{
		/* If no namespace is specified, it means all. */
		if (!te->namespace)
			return 0;
		if (strcmp(ropt->schemaNames, te->namespace) != 0)
			return 0;
	}

	if (ropt->selTypes)
	{
		if (strcmp(te->desc, "TABLE") == 0 ||
			strcmp(te->desc, "TABLE DATA") == 0)
		{
			if (!ropt->selTable)
				return 0;
			if (ropt->tableNames && strcmp(ropt->tableNames, te->tag) != 0)
				return 0;
		}
		else if (strcmp(te->desc, "INDEX") == 0)
		{
			if (!ropt->selIndex)
				return 0;
			if (ropt->indexNames && strcmp(ropt->indexNames, te->tag) != 0)
				return 0;
		}
		else if (strcmp(te->desc, "FUNCTION") == 0)
		{
			if (!ropt->selFunction)
				return 0;
			if (ropt->functionNames && strcmp(ropt->functionNames, te->tag) != 0)
				return 0;
		}
		else if (strcmp(te->desc, "TRIGGER") == 0)
		{
			if (!ropt->selTrigger)
				return 0;
			if (ropt->triggerNames && strcmp(ropt->triggerNames, te->tag) != 0)
				return 0;
		}
		else
			return 0;
	}

	/*
	 * Check if we had a dataDumper. Indicates if the entry is schema or data
	 */
	if (!te->hadDumper)
	{
		/*
		 * Special Case: If 'SEQUENCE SET' or anything to do with BLOBs, then
		 * it is considered a data entry.  We don't need to check for the
		 * BLOBS entry or old-style BLOB COMMENTS, because they will have
		 * hadDumper = true ... but we do need to check new-style BLOB
		 * comments.
		 */
		if (strcmp(te->desc, "SEQUENCE SET") == 0 ||
			strcmp(te->desc, "BLOB") == 0 ||
			(strcmp(te->desc, "ACL") == 0 &&
			 strncmp(te->tag, "LARGE OBJECT ", 13) == 0) ||
			(strcmp(te->desc, "COMMENT") == 0 &&
			 strncmp(te->tag, "LARGE OBJECT ", 13) == 0) ||
			(strcmp(te->desc, "SECURITY LABEL") == 0 &&
			 strncmp(te->tag, "LARGE OBJECT ", 13) == 0))
			res = res & REQ_DATA;
		else
			res = res & ~REQ_DATA;
	}

	/*
	 * Special case: <Init> type with <Max OID> tag; this is obsolete and we
	 * always ignore it.
	 */
	if ((strcmp(te->desc, "<Init>") == 0) && (strcmp(te->tag, "Max OID") == 0))
		return 0;

	/* Mask it if we only want schema */
	if (ropt->schemaOnly)
		res = res & REQ_SCHEMA;

	/* Mask it we only want data */
	if (ropt->dataOnly)
		res = res & REQ_DATA;

	/* Mask it if we don't have a schema contribution */
	if (!te->defn || strlen(te->defn) == 0)
		res = res & ~REQ_SCHEMA;

	/* Finally, if there's a per-ID filter, limit based on that as well */
	if (ropt->idWanted && !ropt->idWanted[te->dumpId - 1])
		return 0;

	return res;
}

/*
 * Identify TOC entries that are ACLs.
 */
static bool
_tocEntryIsACL(TocEntry *te)
{
	/* "ACL LANGUAGE" was a crock emitted only in PG 7.4 */
	if (strcmp(te->desc, "ACL") == 0 ||
		strcmp(te->desc, "ACL LANGUAGE") == 0 ||
		strcmp(te->desc, "DEFAULT ACL") == 0)
		return true;
	return false;
}

/*
 * Issue SET commands for parameters that we want to have set the same way
 * at all times during execution of a restore script.
 */
static void
_doSetFixedOutputState(ArchiveHandle *AH)
{
	/* Disable statement_timeout in archive for pg_restore/psql  */
	ahprintf(AH, "SET statement_timeout = 0;\n");

	/* Select the correct character set encoding */
	ahprintf(AH, "SET client_encoding = '%s';\n",
			 pg_encoding_to_char(AH->public.encoding));

	/* Select the correct string literal syntax */
	ahprintf(AH, "SET standard_conforming_strings = %s;\n",
			 AH->public.std_strings ? "on" : "off");

	/* Select the role to be used during restore */
	if (AH->connParams.use_role)
		ahprintf(AH, "SET ROLE %s;\n", fmtId(AH->connParams.use_role));

	/* Make sure function checking is disabled */
	ahprintf(AH, "SET check_function_bodies = false;\n");

	/* Avoid annoying notices etc */
	ahprintf(AH, "SET client_min_messages = warning;\n");
	if (!AH->public.std_strings)
		ahprintf(AH, "SET escape_string_warning = off;\n");

	ahprintf(AH, "\n");
}

/*
 * Issue a SET SESSION AUTHORIZATION command.  Caller is responsible
 * for updating state if appropriate.  If user is NULL or an empty string,
 * the specification DEFAULT will be used.
 */
static void
_doSetSessionAuth(ArchiveHandle *AH, const char *user)
{
	PQExpBuffer cmd = createPQExpBuffer();

	appendPQExpBuffer(cmd, "SET SESSION AUTHORIZATION ");

	/*
	 * SQL requires a string literal here.	Might as well be correct.
	 */
	if (user && *user)
		appendStringLiteralAHX(cmd, user, AH);
	else
		appendPQExpBuffer(cmd, "DEFAULT");
	appendPQExpBuffer(cmd, ";");

	if (RestoringToDB(AH))
	{
		PGresult   *res;

		res = PQexec(AH->connection, cmd->data);

		if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
			/* NOT warn_or_die_horribly... use -O instead to skip this. */
			die_horribly(AH, modulename, "could not set session user to \"%s\": %s",
						 user, PQerrorMessage(AH->connection));

		PQclear(res);
	}
	else
		ahprintf(AH, "%s\n\n", cmd->data);

	destroyPQExpBuffer(cmd);
}


/*
 * Issue a SET default_with_oids command.  Caller is responsible
 * for updating state if appropriate.
 */
static void
_doSetWithOids(ArchiveHandle *AH, const bool withOids)
{
	PQExpBuffer cmd = createPQExpBuffer();

	appendPQExpBuffer(cmd, "SET default_with_oids = %s;", withOids ?
					  "true" : "false");

	if (RestoringToDB(AH))
	{
		PGresult   *res;

		res = PQexec(AH->connection, cmd->data);

		if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
			warn_or_die_horribly(AH, modulename,
								 "could not set default_with_oids: %s",
								 PQerrorMessage(AH->connection));

		PQclear(res);
	}
	else
		ahprintf(AH, "%s\n\n", cmd->data);

	destroyPQExpBuffer(cmd);
}


/*
 * Issue the commands to connect to the specified database.
 *
 * If we're currently restoring right into a database, this will
 * actually establish a connection. Otherwise it puts a \connect into
 * the script output.
 *
 * NULL dbname implies reconnecting to the current DB (pretty useless).
 */
static void
_reconnectToDB(ArchiveHandle *AH, const char *dbname)
{
	if (RestoringToDB(AH))
		ReconnectToServer(AH, dbname, NULL);
	else
	{
		PQExpBuffer qry = createPQExpBuffer();

		appendPQExpBuffer(qry, "\\connect %s\n\n",
						  dbname ? fmtId(dbname) : "-");
		ahprintf(AH, "%s", qry->data);
		destroyPQExpBuffer(qry);
	}

	/*
	 * NOTE: currUser keeps track of what the imaginary session user in our
	 * script is.  It's now effectively reset to the original userID.
	 */
	if (AH->currUser)
		free(AH->currUser);
	AH->currUser = NULL;

	/* don't assume we still know the output schema, tablespace, etc either */
	if (AH->currSchema)
		free(AH->currSchema);
	AH->currSchema = NULL;
	if (AH->currTablespace)
		free(AH->currTablespace);
	AH->currTablespace = NULL;
	AH->currWithOids = -1;

	/* re-establish fixed state */
	_doSetFixedOutputState(AH);
}

/*
 * Become the specified user, and update state to avoid redundant commands
 *
 * NULL or empty argument is taken to mean restoring the session default
 */
static void
_becomeUser(ArchiveHandle *AH, const char *user)
{
	if (!user)
		user = "";				/* avoid null pointers */

	if (AH->currUser && strcmp(AH->currUser, user) == 0)
		return;					/* no need to do anything */

	_doSetSessionAuth(AH, user);

	/*
	 * NOTE: currUser keeps track of what the imaginary session user in our
	 * script is
	 */
	if (AH->currUser)
		free(AH->currUser);
	AH->currUser = strdup(user);
}

/*
 * Become the owner of the given TOC entry object.	If
 * changes in ownership are not allowed, this doesn't do anything.
 */
static void
_becomeOwner(ArchiveHandle *AH, TocEntry *te)
{
	if (AH->ropt && (AH->ropt->noOwner || !AH->ropt->use_setsessauth))
		return;

	_becomeUser(AH, te->owner);
}


/*
 * Set the proper default_with_oids value for the table.
 */
static void
_setWithOids(ArchiveHandle *AH, TocEntry *te)
{
	if (AH->currWithOids != te->withOids)
	{
		_doSetWithOids(AH, te->withOids);
		AH->currWithOids = te->withOids;
	}
}


/*
 * Issue the commands to select the specified schema as the current schema
 * in the target database.
 */
static void
_selectOutputSchema(ArchiveHandle *AH, const char *schemaName)
{
	PQExpBuffer qry;

	if (!schemaName || *schemaName == '\0' ||
		(AH->currSchema && strcmp(AH->currSchema, schemaName) == 0))
		return;					/* no need to do anything */

	qry = createPQExpBuffer();

	appendPQExpBuffer(qry, "SET search_path = %s",
					  fmtId(schemaName));
	if (strcmp(schemaName, "pg_catalog") != 0)
		appendPQExpBuffer(qry, ", pg_catalog");

	if (RestoringToDB(AH))
	{
		PGresult   *res;

		res = PQexec(AH->connection, qry->data);

		if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
			warn_or_die_horribly(AH, modulename,
								 "could not set search_path to \"%s\": %s",
								 schemaName, PQerrorMessage(AH->connection));

		PQclear(res);
	}
	else
		ahprintf(AH, "%s;\n\n", qry->data);

	if (AH->currSchema)
		free(AH->currSchema);
	AH->currSchema = strdup(schemaName);

	destroyPQExpBuffer(qry);
}

/*
 * Issue the commands to select the specified tablespace as the current one
 * in the target database.
 */
static void
_selectTablespace(ArchiveHandle *AH, const char *tablespace)
{
	PQExpBuffer qry;
	const char *want,
			   *have;

	/* do nothing in --no-tablespaces mode */
	if (AH->ropt->noTablespace)
		return;

	have = AH->currTablespace;
	want = tablespace;

	/* no need to do anything for non-tablespace object */
	if (!want)
		return;

	if (have && strcmp(want, have) == 0)
		return;					/* no need to do anything */

	qry = createPQExpBuffer();

	if (strcmp(want, "") == 0)
	{
		/* We want the tablespace to be the database's default */
		appendPQExpBuffer(qry, "SET default_tablespace = ''");
	}
	else
	{
		/* We want an explicit tablespace */
		appendPQExpBuffer(qry, "SET default_tablespace = %s", fmtId(want));
	}

	if (RestoringToDB(AH))
	{
		PGresult   *res;

		res = PQexec(AH->connection, qry->data);

		if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
			warn_or_die_horribly(AH, modulename,
								 "could not set default_tablespace to %s: %s",
								 fmtId(want), PQerrorMessage(AH->connection));

		PQclear(res);
	}
	else
		ahprintf(AH, "%s;\n\n", qry->data);

	if (AH->currTablespace)
		free(AH->currTablespace);
	AH->currTablespace = strdup(want);

	destroyPQExpBuffer(qry);
}

/*
 * Extract an object description for a TOC entry, and append it to buf.
 *
 * This is not quite as general as it may seem, since it really only
 * handles constructing the right thing to put into ALTER ... OWNER TO.
 *
 * The whole thing is pretty grotty, but we are kind of stuck since the
 * information used is all that's available in older dump files.
 */
static void
_getObjectDescription(PQExpBuffer buf, TocEntry *te, ArchiveHandle *AH)
{
	const char *type = te->desc;

	/* Use ALTER TABLE for views and sequences */
	if (strcmp(type, "VIEW") == 0 || strcmp(type, "SEQUENCE") == 0)
		type = "TABLE";

	/* objects named by a schema and name */
	if (strcmp(type, "COLLATION") == 0 ||
		strcmp(type, "CONVERSION") == 0 ||
		strcmp(type, "DOMAIN") == 0 ||
		strcmp(type, "TABLE") == 0 ||
		strcmp(type, "TYPE") == 0 ||
		strcmp(type, "FOREIGN TABLE") == 0 ||
		strcmp(type, "TEXT SEARCH DICTIONARY") == 0 ||
		strcmp(type, "TEXT SEARCH CONFIGURATION") == 0)
	{
		appendPQExpBuffer(buf, "%s ", type);
		if (te->namespace && te->namespace[0])	/* is null pre-7.3 */
			appendPQExpBuffer(buf, "%s.", fmtId(te->namespace));

		/*
		 * Pre-7.3 pg_dump would sometimes (not always) put a fmtId'd name
		 * into te->tag for an index. This check is heuristic, so make its
		 * scope as narrow as possible.
		 */
		if (AH->version < K_VERS_1_7 &&
			te->tag[0] == '"' &&
			te->tag[strlen(te->tag) - 1] == '"' &&
			strcmp(type, "INDEX") == 0)
			appendPQExpBuffer(buf, "%s", te->tag);
		else
			appendPQExpBuffer(buf, "%s", fmtId(te->tag));
		return;
	}

	/* objects named by just a name */
	if (strcmp(type, "DATABASE") == 0 ||
		strcmp(type, "PROCEDURAL LANGUAGE") == 0 ||
		strcmp(type, "SCHEMA") == 0 ||
		strcmp(type, "FOREIGN DATA WRAPPER") == 0 ||
		strcmp(type, "SERVER") == 0 ||
		strcmp(type, "USER MAPPING") == 0)
	{
		appendPQExpBuffer(buf, "%s %s", type, fmtId(te->tag));
		return;
	}

	/* BLOBs just have a name, but it's numeric so must not use fmtId */
	if (strcmp(type, "BLOB") == 0)
	{
		appendPQExpBuffer(buf, "LARGE OBJECT %s", te->tag);
		return;
	}

	/*
	 * These object types require additional decoration.  Fortunately, the
	 * information needed is exactly what's in the DROP command.
	 */
	if (strcmp(type, "AGGREGATE") == 0 ||
		strcmp(type, "FUNCTION") == 0 ||
		strcmp(type, "OPERATOR") == 0 ||
		strcmp(type, "OPERATOR CLASS") == 0 ||
		strcmp(type, "OPERATOR FAMILY") == 0)
	{
		/* Chop "DROP " off the front and make a modifiable copy */
		char	   *first = strdup(te->dropStmt + 5);
		char	   *last;

		/* point to last character in string */
		last = first + strlen(first) - 1;

		/* Strip off any ';' or '\n' at the end */
		while (last >= first && (*last == '\n' || *last == ';'))
			last--;
		*(last + 1) = '\0';

		appendPQExpBufferStr(buf, first);

		free(first);
		return;
	}

	write_msg(modulename, "WARNING: don't know how to set owner for object type %s\n",
			  type);
}

static void
_printTocEntry(ArchiveHandle *AH, TocEntry *te, RestoreOptions *ropt, bool isData, bool acl_pass)
{
	/* ACLs are dumped only during acl pass */
	if (acl_pass)
	{
		if (!_tocEntryIsACL(te))
			return;
	}
	else
	{
		if (_tocEntryIsACL(te))
			return;
	}

	/*
	 * Avoid dumping the public schema, as it will already be created ...
	 * unless we are using --clean mode, in which case it's been deleted and
	 * we'd better recreate it.  Likewise for its comment, if any.
	 */
	if (!ropt->dropSchema)
	{
		if (strcmp(te->desc, "SCHEMA") == 0 &&
			strcmp(te->tag, "public") == 0)
			return;
		/* The comment restore would require super-user privs, so avoid it. */
		if (strcmp(te->desc, "COMMENT") == 0 &&
			strcmp(te->tag, "SCHEMA public") == 0)
			return;
	}

	/* Select owner, schema, and tablespace as necessary */
	_becomeOwner(AH, te);
	_selectOutputSchema(AH, te->namespace);
	_selectTablespace(AH, te->tablespace);

	/* Set up OID mode too */
	if (strcmp(te->desc, "TABLE") == 0)
		_setWithOids(AH, te);

	/* Emit header comment for item */
	if (!AH->noTocComments)
	{
		const char *pfx;

		if (isData)
			pfx = "Data for ";
		else
			pfx = "";

		ahprintf(AH, "--\n");
		if (AH->public.verbose)
		{
			ahprintf(AH, "-- TOC entry %d (class %u OID %u)\n",
					 te->dumpId, te->catalogId.tableoid, te->catalogId.oid);
			if (te->nDeps > 0)
			{
				int			i;

				ahprintf(AH, "-- Dependencies:");
				for (i = 0; i < te->nDeps; i++)
					ahprintf(AH, " %d", te->dependencies[i]);
				ahprintf(AH, "\n");
			}
		}
		ahprintf(AH, "-- %sName: %s; Type: %s; Schema: %s; Owner: %s",
				 pfx, te->tag, te->desc,
				 te->namespace ? te->namespace : "-",
				 ropt->noOwner ? "-" : te->owner);
		if (te->tablespace && !ropt->noTablespace)
			ahprintf(AH, "; Tablespace: %s", te->tablespace);
		ahprintf(AH, "\n");

		if (AH->PrintExtraTocPtr !=NULL)
			(*AH->PrintExtraTocPtr) (AH, te);
		ahprintf(AH, "--\n\n");
	}

	/*
	 * Actually print the definition.
	 *
	 * Really crude hack for suppressing AUTHORIZATION clause that old pg_dump
	 * versions put into CREATE SCHEMA.  We have to do this when --no-owner
	 * mode is selected.  This is ugly, but I see no other good way ...
	 */
	if (ropt->noOwner && strcmp(te->desc, "SCHEMA") == 0)
	{
		ahprintf(AH, "CREATE SCHEMA %s;\n\n\n", fmtId(te->tag));
	}
	else
	{
		if (strlen(te->defn) > 0)
			ahprintf(AH, "%s\n\n", te->defn);
	}

	/*
	 * If we aren't using SET SESSION AUTH to determine ownership, we must
	 * instead issue an ALTER OWNER command.  We assume that anything without
	 * a DROP command is not a separately ownable object.  All the categories
	 * with DROP commands must appear in one list or the other.
	 */
	if (!ropt->noOwner && !ropt->use_setsessauth &&
		strlen(te->owner) > 0 && strlen(te->dropStmt) > 0)
	{
		if (strcmp(te->desc, "AGGREGATE") == 0 ||
			strcmp(te->desc, "BLOB") == 0 ||
			strcmp(te->desc, "COLLATION") == 0 ||
			strcmp(te->desc, "CONVERSION") == 0 ||
			strcmp(te->desc, "DATABASE") == 0 ||
			strcmp(te->desc, "DOMAIN") == 0 ||
			strcmp(te->desc, "FUNCTION") == 0 ||
			strcmp(te->desc, "OPERATOR") == 0 ||
			strcmp(te->desc, "OPERATOR CLASS") == 0 ||
			strcmp(te->desc, "OPERATOR FAMILY") == 0 ||
			strcmp(te->desc, "PROCEDURAL LANGUAGE") == 0 ||
			strcmp(te->desc, "SCHEMA") == 0 ||
			strcmp(te->desc, "TABLE") == 0 ||
			strcmp(te->desc, "TYPE") == 0 ||
			strcmp(te->desc, "VIEW") == 0 ||
			strcmp(te->desc, "SEQUENCE") == 0 ||
			strcmp(te->desc, "FOREIGN TABLE") == 0 ||
			strcmp(te->desc, "TEXT SEARCH DICTIONARY") == 0 ||
			strcmp(te->desc, "TEXT SEARCH CONFIGURATION") == 0 ||
			strcmp(te->desc, "FOREIGN DATA WRAPPER") == 0 ||
			strcmp(te->desc, "SERVER") == 0)
		{
			PQExpBuffer temp = createPQExpBuffer();

			appendPQExpBuffer(temp, "ALTER ");
			_getObjectDescription(temp, te, AH);
			appendPQExpBuffer(temp, " OWNER TO %s;", fmtId(te->owner));
			ahprintf(AH, "%s\n\n", temp->data);
			destroyPQExpBuffer(temp);
		}
		else if (strcmp(te->desc, "CAST") == 0 ||
				 strcmp(te->desc, "CHECK CONSTRAINT") == 0 ||
				 strcmp(te->desc, "CONSTRAINT") == 0 ||
				 strcmp(te->desc, "DEFAULT") == 0 ||
				 strcmp(te->desc, "FK CONSTRAINT") == 0 ||
				 strcmp(te->desc, "INDEX") == 0 ||
				 strcmp(te->desc, "RULE") == 0 ||
				 strcmp(te->desc, "TRIGGER") == 0 ||
				 strcmp(te->desc, "USER MAPPING") == 0)
		{
			/* these object types don't have separate owners */
		}
		else
		{
			write_msg(modulename, "WARNING: don't know how to set owner for object type %s\n",
					  te->desc);
		}
	}

	/*
	 * If it's an ACL entry, it might contain SET SESSION AUTHORIZATION
	 * commands, so we can no longer assume we know the current auth setting.
	 */
	if (acl_pass)
	{
		if (AH->currUser)
			free(AH->currUser);
		AH->currUser = NULL;
	}
}

void
WriteHead(ArchiveHandle *AH)
{
	struct tm	crtm;

	(*AH->WriteBufPtr) (AH, "PGDMP", 5);		/* Magic code */
	(*AH->WriteBytePtr) (AH, AH->vmaj);
	(*AH->WriteBytePtr) (AH, AH->vmin);
	(*AH->WriteBytePtr) (AH, AH->vrev);
	(*AH->WriteBytePtr) (AH, AH->intSize);
	(*AH->WriteBytePtr) (AH, AH->offSize);
	(*AH->WriteBytePtr) (AH, AH->format);

#ifndef HAVE_LIBZ
	if (AH->compression != 0)
	{
		write_msg(modulename, "WARNING: requested compression not available in this "
				  "installation -- archive will be uncompressed\n");

		AH->compression = 0;
	}
#endif

	WriteInt(AH, AH->compression);

	crtm = *localtime(&AH->createDate);
	WriteInt(AH, crtm.tm_sec);
	WriteInt(AH, crtm.tm_min);
	WriteInt(AH, crtm.tm_hour);
	WriteInt(AH, crtm.tm_mday);
	WriteInt(AH, crtm.tm_mon);
	WriteInt(AH, crtm.tm_year);
	WriteInt(AH, crtm.tm_isdst);
	WriteStr(AH, PQdb(AH->connection));
	WriteStr(AH, AH->public.remoteVersionStr);
	WriteStr(AH, PG_VERSION);
}

void
ReadHead(ArchiveHandle *AH)
{
	char		tmpMag[7];
	int			fmt;
	struct tm	crtm;

	/*
	 * If we haven't already read the header, do so.
	 *
	 * NB: this code must agree with _discoverArchiveFormat().	Maybe find a
	 * way to unify the cases?
	 */
	if (!AH->readHeader)
	{
		if ((*AH->ReadBufPtr) (AH, tmpMag, 5) != 5)
			die_horribly(AH, modulename, "unexpected end of file\n");

		if (strncmp(tmpMag, "PGDMP", 5) != 0)
			die_horribly(AH, modulename, "did not find magic string in file header\n");

		AH->vmaj = (*AH->ReadBytePtr) (AH);
		AH->vmin = (*AH->ReadBytePtr) (AH);

		if (AH->vmaj > 1 || ((AH->vmaj == 1) && (AH->vmin > 0)))		/* Version > 1.0 */
			AH->vrev = (*AH->ReadBytePtr) (AH);
		else
			AH->vrev = 0;

		AH->version = ((AH->vmaj * 256 + AH->vmin) * 256 + AH->vrev) * 256 + 0;

		if (AH->version < K_VERS_1_0 || AH->version > K_VERS_MAX)
			die_horribly(AH, modulename, "unsupported version (%d.%d) in file header\n",
						 AH->vmaj, AH->vmin);

		AH->intSize = (*AH->ReadBytePtr) (AH);
		if (AH->intSize > 32)
			die_horribly(AH, modulename, "sanity check on integer size (%lu) failed\n",
						 (unsigned long) AH->intSize);

		if (AH->intSize > sizeof(int))
			write_msg(modulename, "WARNING: archive was made on a machine with larger integers, some operations might fail\n");

		if (AH->version >= K_VERS_1_7)
			AH->offSize = (*AH->ReadBytePtr) (AH);
		else
			AH->offSize = AH->intSize;

		fmt = (*AH->ReadBytePtr) (AH);

		if (AH->format != fmt)
			die_horribly(AH, modulename, "expected format (%d) differs from format found in file (%d)\n",
						 AH->format, fmt);
	}

	if (AH->version >= K_VERS_1_2)
	{
		if (AH->version < K_VERS_1_4)
			AH->compression = (*AH->ReadBytePtr) (AH);
		else
			AH->compression = ReadInt(AH);
	}
	else
		AH->compression = Z_DEFAULT_COMPRESSION;

#ifndef HAVE_LIBZ
	if (AH->compression != 0)
		write_msg(modulename, "WARNING: archive is compressed, but this installation does not support compression -- no data will be available\n");
#endif

	if (AH->version >= K_VERS_1_4)
	{
		crtm.tm_sec = ReadInt(AH);
		crtm.tm_min = ReadInt(AH);
		crtm.tm_hour = ReadInt(AH);
		crtm.tm_mday = ReadInt(AH);
		crtm.tm_mon = ReadInt(AH);
		crtm.tm_year = ReadInt(AH);
		crtm.tm_isdst = ReadInt(AH);

		AH->archdbname = ReadStr(AH);

		AH->createDate = mktime(&crtm);

		if (AH->createDate == (time_t) -1)
			write_msg(modulename, "WARNING: invalid creation date in header\n");
	}

	if (AH->version >= K_VERS_1_10)
	{
		AH->archiveRemoteVersion = ReadStr(AH);
		AH->archiveDumpVersion = ReadStr(AH);
	}
}


/*
 * checkSeek
 *	  check to see if ftell/fseek can be performed.
 */
bool
checkSeek(FILE *fp)
{
	pgoff_t		tpos;

	/*
	 * If pgoff_t is wider than long, we must have "real" fseeko and not an
	 * emulation using fseek.  Otherwise report no seek capability.
	 */
#ifndef HAVE_FSEEKO
	if (sizeof(pgoff_t) > sizeof(long))
		return false;
#endif

	/* Check that ftello works on this file */
	errno = 0;
	tpos = ftello(fp);
	if (errno)
		return false;

	/*
	 * Check that fseeko(SEEK_SET) works, too.	NB: we used to try to test
	 * this with fseeko(fp, 0, SEEK_CUR).  But some platforms treat that as a
	 * successful no-op even on files that are otherwise unseekable.
	 */
	if (fseeko(fp, tpos, SEEK_SET) != 0)
		return false;

	return true;
}


/*
 * dumpTimestamp
 */
static void
dumpTimestamp(ArchiveHandle *AH, const char *msg, time_t tim)
{
	char		buf[256];

	/*
	 * We don't print the timezone on Win32, because the names are long and
	 * localized, which means they may contain characters in various random
	 * encodings; this has been seen to cause encoding errors when reading the
	 * dump script.
	 */
	if (strftime(buf, sizeof(buf),
#ifndef WIN32
				 "%Y-%m-%d %H:%M:%S %Z",
#else
				 "%Y-%m-%d %H:%M:%S",
#endif
				 localtime(&tim)) != 0)
		ahprintf(AH, "-- %s %s\n\n", msg, buf);
}


/*
 * Main engine for parallel restore.
 *
 * Work is done in three phases.
 * First we process all SECTION_PRE_DATA tocEntries, in a single connection,
 * just as for a standard restore.	Second we process the remaining non-ACL
 * steps in parallel worker children (threads on Windows, processes on Unix),
 * each of which connects separately to the database.  Finally we process all
 * the ACL entries in a single connection (that happens back in
 * RestoreArchive).
 */
static void
restore_toc_entries_parallel(ArchiveHandle *AH, ParallelState *pstate)
{
	RestoreOptions *ropt = AH->ropt;
	ParallelSlot   *slots;
	int			work_status;
	bool		skipped_some;
	TocEntry	pending_list;
	TocEntry	ready_list;
	TocEntry   *next_work_item;
	int			ret_child;
	TocEntry   *te;

	ahlog(AH, 2, "entering restore_toc_entries_parallel\n");

	/* XXX merged */
	/* we haven't got round to making this work for all archive formats */
	if (AH->ClonePtr == NULL || AH->ReopenPtr == NULL)
		die_horribly(AH, modulename, "parallel restore is not supported with this archive file format\n");

	/* doesn't work if the archive represents dependencies as OIDs, either */
	if (AH->version < K_VERS_1_8)
		die_horribly(AH, modulename, "parallel restore is not supported with archives made by pre-8.0 pg_dump\n");

	slots = (ParallelSlot *) calloc(sizeof(ParallelSlot), AH->public.numWorkers);

	/* Adjust dependency information */
	fix_dependencies(AH);

	/*
	 * Do all the early stuff in a single connection in the parent. There's no
	 * great point in running it in parallel, in fact it will actually run
	 * faster in a single connection because we avoid all the connection and
	 * setup overhead.	Also, pg_dump is not currently very good about showing
	 * all the dependencies of SECTION_PRE_DATA items, so we do not risk
	 * trying to process them out-of-order.
	 */
	skipped_some = false;
	for (next_work_item = AH->toc->next; next_work_item != AH->toc; next_work_item = next_work_item->next)
	{
		/* NB: process-or-continue logic must be the inverse of loop below */
		if (next_work_item->section != SECTION_PRE_DATA)
		{
			/* DATA and POST_DATA items are just ignored for now */
			if (next_work_item->section == SECTION_DATA ||
				next_work_item->section == SECTION_POST_DATA)
			{
				skipped_some = true;
				continue;
			}
			else
			{
				/*
				 * SECTION_NONE items, such as comments, can be processed now
				 * if we are still in the PRE_DATA part of the archive.  Once
				 * we've skipped any items, we have to consider whether the
				 * comment's dependencies are satisfied, so skip it for now.
				 */
				if (skipped_some)
					continue;
			}
		}

		ahlog(AH, 1, "processing item %d %s %s\n",
			  next_work_item->dumpId,
			  next_work_item->desc, next_work_item->tag);

		(void) restore_toc_entry(AH, next_work_item, ropt, false);

		/* there should be no touch of ready_list here, so pass NULL */
		reduce_dependencies(AH, next_work_item, NULL);
	}

	/*
	 * Now close parent connection in prep for parallel steps.	We do this
	 * mainly to ensure that we don't exceed the specified number of parallel
	 * connections.
	 */
	PQfinish(AH->connection);
	AH->connection = NULL;

	/* blow away any transient state from the old connection */
	if (AH->currUser)
		free(AH->currUser);
	AH->currUser = NULL;
	if (AH->currSchema)
		free(AH->currSchema);
	AH->currSchema = NULL;
	if (AH->currTablespace)
		free(AH->currTablespace);
	AH->currTablespace = NULL;
	AH->currWithOids = -1;

	/*
	 * Initialize the lists of pending and ready items.  After this setup, the
	 * pending list is everything that needs to be done but is blocked by one
	 * or more dependencies, while the ready list contains items that have no
	 * remaining dependencies.	Note: we don't yet filter out entries that
	 * aren't going to be restored.  They might participate in dependency
	 * chains connecting entries that should be restored, so we treat them as
	 * live until we actually process them.
	 */
	par_list_header_init(&pending_list);
	par_list_header_init(&ready_list);
	skipped_some = false;
	for (next_work_item = AH->toc->next; next_work_item != AH->toc; next_work_item = next_work_item->next)
	{
		/* NB: process-or-continue logic must be the inverse of loop above */
		if (next_work_item->section == SECTION_PRE_DATA)
		{
			/* All PRE_DATA items were dealt with above */
			continue;
		}
		if (next_work_item->section == SECTION_DATA ||
			next_work_item->section == SECTION_POST_DATA)
		{
			/* set this flag at same point that previous loop did */
			skipped_some = true;
		}
		else
		{
			/* SECTION_NONE items must be processed if previous loop didn't */
			if (!skipped_some)
				continue;
		}

		if (next_work_item->depCount > 0)
			par_list_append(&pending_list, next_work_item);
		else
			par_list_append(&ready_list, next_work_item);
	}

	/*
	 * main parent loop
	 *
	 * Keep going until there is no worker still running AND there is no work
	 * left to be done.
	 */

	ahlog(AH, 1, "entering main parallel loop\n");

	while ((next_work_item = get_next_work_item(AH, &ready_list,
												pstate)) != NULL ||
		   !IsEveryWorkerIdle(pstate))
	{
		if (next_work_item != NULL)
		{
			teReqs		reqs;

			/* If not to be dumped, don't waste time launching a worker */
			reqs = _tocEntryRequired(next_work_item, AH->ropt, false);
			if ((reqs & (REQ_SCHEMA | REQ_DATA)) == 0)
			{
				ahlog(AH, 1, "skipping item %d %s %s\n",
					  next_work_item->dumpId,
					  next_work_item->desc, next_work_item->tag);

				par_list_remove(next_work_item);
				reduce_dependencies(AH, next_work_item, &ready_list);

				continue;
			}

			ahlog(AH, 1, "launching item %d %s %s\n",
				  next_work_item->dumpId,
				  next_work_item->desc, next_work_item->tag);

			par_list_remove(next_work_item);

			Assert(GetIdleWorker(pstate) != NO_SLOT);
			DispatchJobForTocEntry(AH, pstate, next_work_item, ACT_RESTORE);
		}
		else
		{
			/* at least one child is working and we have nothing ready. */
			Assert(!IsEveryWorkerIdle(pstate));
		}

		for (;;)
		{
			int nTerm = 0;

			/*
			 * In order to reduce dependencies as soon as possible and
			 * especially to reap the status of children who are working on
			 * items that pending items depend on, we do a non-blocking check
			 * for ended children first.
			 *
			 * However, if we do not have any other work items currently that
			 * children can work on, we do not busy-loop here but instead
			 * really wait for at least one child to terminate. Hence we call
			 * ListenToWorkers(..., ..., true) in this case.
			 */
			ListenToWorkers(AH, pstate, !next_work_item);

			while ((ret_child = ReapworkerStatus(pstate, &work_status)) != NO_SLOT)
			{
				nTerm++;
				printf("Marking the child's work as done\n");
				mark_work_done(AH, &ready_list, ret_child, work_status, pstate);
			}

			/* We need to make sure that we have an idle child before re-running the
			 * loop. If nTerm > 0 we already have that (quick check). */
			if (nTerm > 0)
				break;

			/* explicit check for an idle child */
			if (GetIdleWorker(pstate) != NO_SLOT)
				break;

			/*
			 * If we have no idle child, read the result of one or more
			 * children and loop the loop to call ReapworkerStatus() on them
			 */
			ListenToWorkers(AH, pstate, true);
		}
	}

	ahlog(AH, 1, "finished main parallel loop\n");

	/*
	 * Now reconnect the single parent connection.
	 */
	CloneDatabaseConnection((Archive *) AH);

	_doSetFixedOutputState(AH);

	/*
	 * Make sure there is no non-ACL work left due to, say, circular
	 * dependencies, or some other pathological condition. If so, do it in the
	 * single parent connection.
	 */
	for (te = pending_list.par_next; te != &pending_list; te = te->par_next)
	{
		ahlog(AH, 1, "processing missed item %d %s %s\n",
			  te->dumpId, te->desc, te->tag);
		(void) restore_toc_entry(AH, te, ropt, false);
	}

	/* The ACLs will be handled back in RestoreArchive. */
}

/*
 * Check if te1 has an exclusive lock requirement for an item that te2 also
 * requires, whether or not te2's requirement is for an exclusive lock.
 */
static bool
has_lock_conflicts(TocEntry *te1, TocEntry *te2)
{
	int			j,
				k;

	for (j = 0; j < te1->nLockDeps; j++)
	{
		for (k = 0; k < te2->nDeps; k++)
		{
			if (te1->lockDeps[j] == te2->dependencies[k])
				return true;
		}
	}
	return false;
}


/*
 * Initialize the header of a parallel-processing list.
 *
 * These are circular lists with a dummy TocEntry as header, just like the
 * main TOC list; but we use separate list links so that an entry can be in
 * the main TOC list as well as in a parallel-processing list.
 */
static void
par_list_header_init(TocEntry *l)
{
	l->par_prev = l->par_next = l;
}

/* Append te to the end of the parallel-processing list headed by l */
static void
par_list_append(TocEntry *l, TocEntry *te)
{
	te->par_prev = l->par_prev;
	l->par_prev->par_next = te;
	l->par_prev = te;
	te->par_next = l;
}

/* Remove te from whatever parallel-processing list it's in */
static void
par_list_remove(TocEntry *te)
{
	te->par_prev->par_next = te->par_next;
	te->par_next->par_prev = te->par_prev;
	te->par_prev = NULL;
	te->par_next = NULL;
}


/*
 * Find the next work item (if any) that is capable of being run now.
 *
 * To qualify, the item must have no remaining dependencies
 * and no requirements for locks that are incompatible with
 * items currently running.  Items in the ready_list are known to have
 * no remaining dependencies, but we have to check for lock conflicts.
 *
 * Note that the returned item has *not* been removed from ready_list.
 * The caller must do that after successfully dispatching the item.
 *
 * pref_non_data is for an alternative selection algorithm that gives
 * preference to non-data items if there is already a data load running.
 * It is currently disabled.
 */
static TocEntry *
get_next_work_item(ArchiveHandle *AH, TocEntry *ready_list,
				   ParallelState *pstate)
{
	bool		pref_non_data = false;	/* or get from AH->ropt */
	TocEntry   *data_te = NULL;
	TocEntry   *te;
	int			i,
				k;

	/*
	 * Bogus heuristics for pref_non_data
	 */
	if (pref_non_data)
	{
		int			count = 0;

		for (k = 0; k < pstate->numWorkers; k++)
			if (pstate->parallelSlot[k].args->te != NULL &&
				pstate->parallelSlot[k].args->te->section == SECTION_DATA)
				count++;
		if (pstate->numWorkers == 0 || count * 4 < pstate->numWorkers)
			pref_non_data = false;
	}

	/*
	 * Search the ready_list until we find a suitable item.
	 */
	for (te = ready_list->par_next; te != ready_list; te = te->par_next)
	{
		bool		conflicts = false;

		/*
		 * Check to see if the item would need exclusive lock on something
		 * that a currently running item also needs lock on, or vice versa. If
		 * so, we don't want to schedule them together.
		 */
		for (i = 0; i < pstate->numWorkers && !conflicts; i++)
		{
			TocEntry   *running_te;

			if (pstate->parallelSlot[i].workerStatus != WRKR_WORKING)
				continue;
			running_te = pstate->parallelSlot[i].args->te;

			if (has_lock_conflicts(te, running_te) ||
				has_lock_conflicts(running_te, te))
			{
				conflicts = true;
				break;
			}
		}

		if (conflicts)
			continue;

		if (pref_non_data && te->section == SECTION_DATA)
		{
			if (data_te == NULL)
				data_te = te;
			continue;
		}

		/* passed all tests, so this item can run */
		return te;
	}

	if (data_te != NULL)
		return data_te;

	ahlog(AH, 2, "no item ready\n");
	return NULL;
}


/*
 * Restore a single TOC item in parallel with others
 *
 * this is run in the worker, i.e. in a thread (Windows) or a separate process
 * (everything else). A worker process executes several such work items during
 * a parallel backup or restore. Once we terminate here and report back that
 * our work is finished, the master process will assign us a new work item.
 */
int
parallel_restore(ParallelArgs *args)
{
	ArchiveHandle *AH = args->AH;
	TocEntry   *te = args->te;
	RestoreOptions *ropt = AH->ropt;
	int			status;

	_doSetFixedOutputState(AH);

	Assert(AH->connection != NULL);

	AH->public.n_errors = 0;

	/* Restore the TOC item */
	status = restore_toc_entry(AH, te, ropt, true);

	return status;
}


/*
 * Housekeeping to be done after a step has been parallel restored.
 *
 * Clear the appropriate slot, free all the extra memory we allocated,
 * update status, and reduce the dependency count of any dependent items.
 */
static void
mark_work_done(ArchiveHandle *AH, TocEntry *ready_list,
			   int worker, int status,
			   ParallelState *pstate)
{
	TocEntry   *te = NULL;

	te = pstate->parallelSlot[worker].args->te;

	if (te == NULL)
		die_horribly(AH, modulename, "could not find slot of finished worker\n");

	ahlog(AH, 1, "finished item %d %s %s\n",
		  te->dumpId, te->desc, te->tag);

	if (status == WORKER_CREATE_DONE)
		mark_create_done(AH, te);
	else if (status == WORKER_INHIBIT_DATA)
	{
		inhibit_data_for_failed_table(AH, te);
		AH->public.n_errors++;
	}
	else if (status == WORKER_IGNORED_ERRORS)
		AH->public.n_errors++;
	else if (status != 0)
		die_horribly(AH, modulename, "worker process failed: exit code %d\n",
					 status);

	reduce_dependencies(AH, te, ready_list);
}


/*
 * Process the dependency information into a form useful for parallel restore.
 *
 * This function takes care of fixing up some missing or badly designed
 * dependencies, and then prepares subsidiary data structures that will be
 * used in the main parallel-restore logic, including:
 * 1. We build the tocsByDumpId[] index array.
 * 2. We build the revDeps[] arrays of incoming dependency dumpIds.
 * 3. We set up depCount fields that are the number of as-yet-unprocessed
 * dependencies for each TOC entry.
 *
 * We also identify locking dependencies so that we can avoid trying to
 * schedule conflicting items at the same time.
 */
static void
fix_dependencies(ArchiveHandle *AH)
{
	TocEntry   *te;
	int			i;

	/*
	 * It is convenient to have an array that indexes the TOC entries by dump
	 * ID, rather than searching the TOC list repeatedly.  Entries for dump
	 * IDs not present in the TOC will be NULL.
	 *
	 * NOTE: because maxDumpId is just the highest dump ID defined in the
	 * archive, there might be dependencies for IDs > maxDumpId.  All uses of
	 * this array must guard against out-of-range dependency numbers.
	 *
	 * Also, initialize the depCount/revDeps/nRevDeps fields, and make sure
	 * the TOC items are marked as not being in any parallel-processing list.
	 */
	maxDumpId = AH->maxDumpId;
	tocsByDumpId = (TocEntry **) calloc(maxDumpId, sizeof(TocEntry *));
	for (te = AH->toc->next; te != AH->toc; te = te->next)
	{
		tocsByDumpId[te->dumpId - 1] = te;
		te->depCount = te->nDeps;
		te->revDeps = NULL;
		te->nRevDeps = 0;
		te->par_prev = NULL;
		te->par_next = NULL;
	}

	/*
	 * POST_DATA items that are shown as depending on a table need to be
	 * re-pointed to depend on that table's data, instead.  This ensures they
	 * won't get scheduled until the data has been loaded.  We handle this by
	 * first finding TABLE/TABLE DATA pairs and then scanning all the
	 * dependencies.
	 *
	 * Note: currently, a TABLE DATA should always have exactly one
	 * dependency, on its TABLE item.  So we don't bother to search, but look
	 * just at the first dependency.  We do trouble to make sure that it's a
	 * TABLE, if possible.	However, if the dependency isn't in the archive
	 * then just assume it was a TABLE; this is to cover cases where the table
	 * was suppressed but we have the data and some dependent post-data items.
	 *
	 * XXX this is O(N^2) if there are a lot of tables.  We ought to fix
	 * pg_dump to produce correctly-linked dependencies in the first place.
	 */
	for (te = AH->toc->next; te != AH->toc; te = te->next)
	{
		if (strcmp(te->desc, "TABLE DATA") == 0 && te->nDeps > 0)
		{
			DumpId		tableId = te->dependencies[0];

			if (tableId > maxDumpId ||
				tocsByDumpId[tableId - 1] == NULL ||
				strcmp(tocsByDumpId[tableId - 1]->desc, "TABLE") == 0)
			{
				repoint_table_dependencies(AH, tableId, te->dumpId);
			}
		}
	}

	/*
	 * Pre-8.4 versions of pg_dump neglected to set up a dependency from BLOB
	 * COMMENTS to BLOBS.  Cope.  (We assume there's only one BLOBS and only
	 * one BLOB COMMENTS in such files.)
	 */
	if (AH->version < K_VERS_1_11)
	{
		for (te = AH->toc->next; te != AH->toc; te = te->next)
		{
			if (strcmp(te->desc, "BLOB COMMENTS") == 0 && te->nDeps == 0)
			{
				TocEntry   *te2;

				for (te2 = AH->toc->next; te2 != AH->toc; te2 = te2->next)
				{
					if (strcmp(te2->desc, "BLOBS") == 0)
					{
						te->dependencies = (DumpId *) malloc(sizeof(DumpId));
						te->dependencies[0] = te2->dumpId;
						te->nDeps++;
						te->depCount++;
						break;
					}
				}
				break;
			}
		}
	}

	/*
	 * At this point we start to build the revDeps reverse-dependency arrays,
	 * so all changes of dependencies must be complete.
	 */

	/*
	 * Count the incoming dependencies for each item.  Also, it is possible
	 * that the dependencies list items that are not in the archive at all.
	 * Subtract such items from the depCounts.
	 */
	for (te = AH->toc->next; te != AH->toc; te = te->next)
	{
		for (i = 0; i < te->nDeps; i++)
		{
			DumpId		depid = te->dependencies[i];

			if (depid <= maxDumpId && tocsByDumpId[depid - 1] != NULL)
				tocsByDumpId[depid - 1]->nRevDeps++;
			else
				te->depCount--;
		}
	}

	/*
	 * Allocate space for revDeps[] arrays, and reset nRevDeps so we can use
	 * it as a counter below.
	 */
	for (te = AH->toc->next; te != AH->toc; te = te->next)
	{
		if (te->nRevDeps > 0)
			te->revDeps = (DumpId *) malloc(te->nRevDeps * sizeof(DumpId));
		te->nRevDeps = 0;
	}

	/*
	 * Build the revDeps[] arrays of incoming-dependency dumpIds.  This had
	 * better agree with the loops above.
	 */
	for (te = AH->toc->next; te != AH->toc; te = te->next)
	{
		for (i = 0; i < te->nDeps; i++)
		{
			DumpId		depid = te->dependencies[i];

			if (depid <= maxDumpId && tocsByDumpId[depid - 1] != NULL)
			{
				TocEntry   *otherte = tocsByDumpId[depid - 1];

				otherte->revDeps[otherte->nRevDeps++] = te->dumpId;
			}
		}
	}

	/*
	 * Lastly, work out the locking dependencies.
	 */
	for (te = AH->toc->next; te != AH->toc; te = te->next)
	{
		te->lockDeps = NULL;
		te->nLockDeps = 0;
		identify_locking_dependencies(te);
	}
}

/*
 * Change dependencies on tableId to depend on tableDataId instead,
 * but only in POST_DATA items.
 */
static void
repoint_table_dependencies(ArchiveHandle *AH,
						   DumpId tableId, DumpId tableDataId)
{
	TocEntry   *te;
	int			i;

	for (te = AH->toc->next; te != AH->toc; te = te->next)
	{
		if (te->section != SECTION_POST_DATA)
			continue;
		for (i = 0; i < te->nDeps; i++)
		{
			if (te->dependencies[i] == tableId)
			{
				te->dependencies[i] = tableDataId;
				ahlog(AH, 2, "transferring dependency %d -> %d to %d\n",
					  te->dumpId, tableId, tableDataId);
			}
		}
	}
}

/*
 * Identify which objects we'll need exclusive lock on in order to restore
 * the given TOC entry (*other* than the one identified by the TOC entry
 * itself).  Record their dump IDs in the entry's lockDeps[] array.
 */
static void
identify_locking_dependencies(TocEntry *te)
{
	DumpId	   *lockids;
	int			nlockids;
	int			i;

	/* Quick exit if no dependencies at all */
	if (te->nDeps == 0)
		return;

	/* Exit if this entry doesn't need exclusive lock on other objects */
	if (!(strcmp(te->desc, "CONSTRAINT") == 0 ||
		  strcmp(te->desc, "CHECK CONSTRAINT") == 0 ||
		  strcmp(te->desc, "FK CONSTRAINT") == 0 ||
		  strcmp(te->desc, "RULE") == 0 ||
		  strcmp(te->desc, "TRIGGER") == 0))
		return;

	/*
	 * We assume the item requires exclusive lock on each TABLE DATA item
	 * listed among its dependencies.  (This was originally a dependency on
	 * the TABLE, but fix_dependencies repointed it to the data item. Note
	 * that all the entry types we are interested in here are POST_DATA, so
	 * they will all have been changed this way.)
	 */
	lockids = (DumpId *) malloc(te->nDeps * sizeof(DumpId));
	nlockids = 0;
	for (i = 0; i < te->nDeps; i++)
	{
		DumpId		depid = te->dependencies[i];

		if (depid <= maxDumpId && tocsByDumpId[depid - 1] &&
			strcmp(tocsByDumpId[depid - 1]->desc, "TABLE DATA") == 0)
			lockids[nlockids++] = depid;
	}

	if (nlockids == 0)
	{
		free(lockids);
		return;
	}

	te->lockDeps = realloc(lockids, nlockids * sizeof(DumpId));
	te->nLockDeps = nlockids;
}

/*
 * Remove the specified TOC entry from the depCounts of items that depend on
 * it, thereby possibly making them ready-to-run.  Any pending item that
 * becomes ready should be moved to the ready list.
 */
static void
reduce_dependencies(ArchiveHandle *AH, TocEntry *te, TocEntry *ready_list)
{
	int			i;

	ahlog(AH, 2, "reducing dependencies for %d\n", te->dumpId);

	for (i = 0; i < te->nRevDeps; i++)
	{
		TocEntry   *otherte = tocsByDumpId[te->revDeps[i] - 1];

		otherte->depCount--;
		if (otherte->depCount == 0 && otherte->par_prev != NULL)
		{
			/* It must be in the pending list, so remove it ... */
			par_list_remove(otherte);
			/* ... and add to ready_list */
			par_list_append(ready_list, otherte);
		}
	}
}

/*
 * Set the created flag on the DATA member corresponding to the given
 * TABLE member
 */
static void
mark_create_done(ArchiveHandle *AH, TocEntry *te)
{
	TocEntry   *tes;

	for (tes = AH->toc->next; tes != AH->toc; tes = tes->next)
	{
		if (strcmp(tes->desc, "TABLE DATA") == 0 &&
			strcmp(tes->tag, te->tag) == 0 &&
			strcmp(tes->namespace ? tes->namespace : "",
				   te->namespace ? te->namespace : "") == 0)
		{
			tes->created = true;
			break;
		}
	}
}

/*
 * Mark the DATA member corresponding to the given TABLE member
 * as not wanted
 */
static void
inhibit_data_for_failed_table(ArchiveHandle *AH, TocEntry *te)
{
	RestoreOptions *ropt = AH->ropt;
	TocEntry   *tes;

	ahlog(AH, 1, "table \"%s\" could not be created, will not restore its data\n",
		  te->tag);

	for (tes = AH->toc->next; tes != AH->toc; tes = tes->next)
	{
		if (strcmp(tes->desc, "TABLE DATA") == 0 &&
			strcmp(tes->tag, te->tag) == 0 &&
			strcmp(tes->namespace ? tes->namespace : "",
				   te->namespace ? te->namespace : "") == 0)
		{
			/* mark it unwanted; we assume idWanted array already exists */
			ropt->idWanted[tes->dumpId - 1] = false;
			break;
		}
	}
}


/*
 * Clone and de-clone routines used in parallel restoration.
 *
 * Enough of the structure is cloned to ensure that there is no
 * conflict between different threads each with their own clone.
 */
ArchiveHandle *
CloneArchive(ArchiveHandle *AH)
{
	ArchiveHandle *clone;

	/* Make a "flat" copy */
	clone = (ArchiveHandle *) malloc(sizeof(ArchiveHandle));
	if (clone == NULL)
		die_horribly(AH, modulename, "out of memory\n");
	memcpy(clone, AH, sizeof(ArchiveHandle));

	/* Handle format-independent fields ... none at the moment */

	/* The clone will have its own connection, so disregard connection state */
	clone->connection = NULL;
	clone->currUser = NULL;
	clone->currSchema = NULL;
	clone->currTablespace = NULL;
	clone->currWithOids = -1;

	/* clone has its own error count, too */
	clone->public.n_errors = 0;

	/* Let the format-specific code have a chance too */
	(clone->ClonePtr) (clone);

	return clone;
}

/*
 * Release clone-local storage.
 *
 * Note: we assume any clone-local connection was already closed.
 */
void
DeCloneArchive(ArchiveHandle *AH)
{
	/* Clear format-specific state */
	(AH->DeClonePtr) (AH);

	/* Clear state allocated by CloneArchive ... none at the moment */

	/* Clear any connection-local state */
	if (AH->currUser)
		free(AH->currUser);
	if (AH->currSchema)
		free(AH->currSchema);
	if (AH->currTablespace)
		free(AH->currTablespace);

	if (AH->connParams.savedPassword)
		free(AH->connParams.savedPassword);

	free(AH);
}

/*
 * This function is called by both UNIX and Windows variants to set up a
 * worker process.
 */
static void
SetupWorker(ArchiveHandle *AH, int pipefd[2], int worker,
		   RestoreOptions *ropt, PGconn **conn)
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
	_SetupWorker((Archive *) AH, ropt, conn);

	Assert(AH->connection != NULL);
	Assert(*conn == AH->connection);

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

	free(wi);

	printf("My thread handle: %d, master: %d\n", GetCurrentThreadId(), tMasterThreadId);

	SetupWorker(AH, pipefd, worker, ropt, conn);

	DeCloneArchive(AH);
//	_endthreadex(0);
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

	pstate = (ParallelState *) malloc(sizeof(ParallelState));

	pstate->numWorkers = AH->public.numWorkers;
	pstate->parallelSlot = NULL;

	if (AH->public.numWorkers == 1)
		return pstate;

	pstate->parallelSlot = (ParallelSlot *) malloc(slotSize);
	memset((void *) pstate->parallelSlot, 0, slotSize);

#ifdef WIN32
	tMasterThreadId = GetCurrentThreadId();
	termEvent = CreateEvent(NULL, true, false, "Terminate");
#endif

	for (i = 0; i < pstate->numWorkers; i++)
	{
#ifdef WIN32
		WorkerInfo *wi;
		uintptr_t	handle;
#else
		pid_t		pid;
#endif
		int	pipeMW[2], pipeWM[2];

		if (pgpipe(pipeMW) < 0 || pgpipe(pipeWM) < 0)
			die_horribly(AH, modulename, "Cannot create communication channels: %s",
						 strerror(errno));
#ifdef WIN32
		/* Allocate a new structure for every worker */
		wi = (WorkerInfo *) malloc(sizeof(WorkerInfo));

		wi->ropt = ropt;
		wi->worker = i;
		wi->AH = CloneArchive(AH);
		wi->pipeRead = pipeMW[PIPE_READ];
		wi->pipeWrite = pipeWM[PIPE_WRITE];
		wi->conn = &pstate->parallelSlot[i].conn;

		handle = _beginthreadex(NULL, 0, &init_spawned_worker_win32,
								wi, 0, NULL);
		pstate->parallelSlot[i].hThread = handle;
#else
		pid = fork();
		if (pid == 0)
		{
			/* we are the worker */
			int j;
			int pipefd[2] = { pipeMW[PIPE_READ], pipeWM[PIPE_WRITE] };

			/* Forget the parent's connection and set up our signal handler */
			worker_conn = NULL;
			/* XXX */
			signal(SIGTERM, WorkerExit);

			closesocket(pipeWM[PIPE_READ]);		/* close read end of Worker -> Master */
			closesocket(pipeMW[PIPE_WRITE]);	/* close write end of Master -> Worker */

			/*
			 * Close all inherited fd's for communication of the master with
			 * the other workers.
			 */
			for (j = 0; j < i; j++)
			{
				closesocket(pstate->parallelSlot[j].pipeRead);
				closesocket(pstate->parallelSlot[j].pipeWrite);
			}

			/* We don't need pstate in the worker */
			free(pstate->parallelSlot);
			free(pstate);

			SetupWorker(AH, pipefd, i, ropt, &worker_conn);

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

		pstate->parallelSlot[i].args = (ParallelArgs *) malloc(sizeof(ParallelArgs));
		pstate->parallelSlot[i].args->AH = AH;
		pstate->parallelSlot[i].args->te = NULL;
		pstate->parallelSlot[i].workerStatus = WRKR_IDLE;
	}

	return pstate;
}

/*
 * Tell all of our workers to terminate.
 *
 * Pretty straightforward routine, first we tell everyone to
 * terminate, then we listen to the workers' replies and
 * finally close the sockets that we have used for
 * communication.
 */
void
ParallelBackupEnd(ArchiveHandle *AH, ParallelState *pstate)
{
	int i;

	if (pstate->numWorkers == 1)
		return;

	PrintStatus(pstate);
	Assert(IsEveryWorkerIdle(pstate));

	/* no hard shutdown, let workers exit by themselves */
	ShutdownWorkersSoft(AH, pstate);

	PrintStatus(pstate);

	for (i = 0; i < pstate->numWorkers; i++)
	{
		closesocket(pstate->parallelSlot[i].pipeRead);
		closesocket(pstate->parallelSlot[i].pipeWrite);
	}

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
 * arg = (StartMasterParallelPtr)()
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
 * status = (EndMasterParallelPtr)(info)
 *
 * In ReapworkerStatus(&ptr):
 * *ptr = status;
 * [ Worker is IDLE ]
 */

void
DispatchJobForTocEntry(ArchiveHandle *AH, ParallelState *pstate, TocEntry *te,
					   T_Action act)
{
	int		worker;
	char   *arg;

	Assert(GetIdleWorker(pstate) != NO_SLOT);

	/* our caller must make sure that at least one worker is idle */
	worker = GetIdleWorker(pstate);
	Assert(worker != NO_SLOT);

	arg = (AH->StartMasterParallelPtr)(AH, te, act);

	sendMessageToWorker(AH, pstate, worker, arg);

	pstate->parallelSlot[worker].workerStatus = WRKR_WORKING;
	pstate->parallelSlot[worker].args->te = te;
	PrintStatus(pstate);
}


static void
PrintStatus(ParallelState *pstate)
{
	int i;
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
 * find the first free parallel slot (if any).
 */
static int
GetIdleWorker(ParallelState *pstate)
{
	int i;
	for (i = 0; i < pstate->numWorkers; i++)
		if (pstate->parallelSlot[i].workerStatus == WRKR_IDLE)
			return i;
	return NO_SLOT;
}

static bool
HasEveryWorkerTerminated(ParallelState *pstate)
{
	int i;
	for (i = 0; i < pstate->numWorkers; i++)
		if (pstate->parallelSlot[i].workerStatus != WRKR_TERMINATED)
			return false;
	return true;
}

static bool
IsEveryWorkerIdle(ParallelState *pstate)
{
	int i;
	for (i = 0; i < pstate->numWorkers; i++)
		if (pstate->parallelSlot[i].workerStatus != WRKR_IDLE)
			return false;
	return true;
}

static void
ShutdownWorkersSoft(ArchiveHandle *AH, ParallelState *pstate)
{
	int i;

	/* soft shutdown */
	for (i = 0; i < pstate->numWorkers; i++)
	{
		sendMessageToWorker(AH, pstate, i, "TERMINATE");
		pstate->parallelSlot[i].workerStatus = WRKR_WORKING;
	}

	printf("Waiting for children to terminate\n");
	while (!HasEveryWorkerTerminated(pstate))
	{
		PrintStatus(pstate);
		CheckForWorkerTermination(pstate, true);
	}

	printf("All children terminated\n");
}

#ifdef WIN32
static unsigned __stdcall
#else
static int
#endif
ShutdownConnection(PGconn **conn)
{
	PGcancel   *cancel;
	char		errbuf[1];
	int		i;

	Assert(conn != NULL);
	Assert(*conn != NULL);

	/*
	 * Do some effort to gracefully shut down the connection, but not too much
	 * as the parent is waiting for us to terminate. The cancellation is
	 * asynchronous, so if we just cancelled and immediately afterwards called
	 * PQfinish, we would still abort active busy connections.
	 */
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
		pg_usleep(500000);
	}
	printf("Finishing in [%d], busy: %d\n", getpid(), PQisBusy(*conn));
	PQfinish(*conn);
	*conn = NULL;
	return 0;
}

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
		die_horribly(AH, modulename, "could not lock table %s: %s",
					 qualId, PQerrorMessage(AH->connection));

	PQclear(res);
	destroyPQExpBuffer(query);
}

/*
 * At the moment we do not have format specific arguments so we can do the whole
 * argument parsing here and need not pass them to the format-specific routine.
 *
 * On the other hand we do have format specific return values (the directory format
 * returns the length of the file created) so we return a string in this case to be
 * open for future improvement. We could also return a checksum or the like in the
 * future.
 */
#define messageStartsWith(msg, prefix) \
	(strncmp(msg, prefix, strlen(prefix)) == 0)
#define messageEquals(msg, pattern) \
	(strcmp(msg, pattern) == 0)
static void
WaitForCommands(ArchiveHandle *AH, int pipefd[2], int worker)
{
	char	   *command;
	DumpId		dumpId;
	int			nBytes;
	char	   *str = NULL;
	TocEntry   *te;

	for(;;)
	{
		command = getMessageFromMaster(AH, pipefd);

		if (worker == 5) {
			PQfinish(worker_conn);
			die_horribly(AH, modulename, "Null bock mehr\n");
		}

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
			 * lock, then somebody else has requested an exclusive lock since
			 * then. lockTableNoWait dies in this case to prevent deadlock.
			 */
			if (strcmp(te->desc, "BLOBS") != 0)
				lockTableNoWait(AH, te);

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
 * ReapworkerStatus				WRKR_FINISHED -> WRKR_IDLE
 *
 * Just calling ReapworkerStatus() when all workers are working might or might
 * not give you an idle worker because you need to call ListenToWorkers() in
 * between and only thereafter ReapworkerStatus(). This is necessary in order to
 * get and deal with the status (=result) of the worker's execution.
 */
static void
ListenToWorkers(ArchiveHandle *AH, ParallelState *pstate, bool do_wait)
{
	int		worker;
	char   *msg;
	int		i;

	if ((i = CheckForWorkerTermination(pstate, false)) != NO_SLOT)
	{
		printf("Error, process %d has died, shutting down all child processes\n", i);
		ShutdownWorkersHard(AH, pstate);
		die_horribly(AH, modulename,
					 "A worker died while working on %s %s\n",
					 pstate->parallelSlot[i].args->te->desc,
					 pstate->parallelSlot[i].args->te->tag);
	}

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

		printf("Got OK with information from worker %d (%s)\n", worker, msg);

		pstate->parallelSlot[worker].workerStatus = WRKR_FINISHED;
		te = pstate->parallelSlot[worker].args->te;
		if (messageStartsWith(msg, "OK RESTORE "))
		{
			statusString = msg + strlen("OK RESTORE ");
			pstate->parallelSlot[worker].status =
				(AH->EndMasterParallelPtr)
					(AH, te, statusString, ACT_RESTORE);
		}
		else if (messageStartsWith(msg, "OK DUMP "))
		{
			statusString = msg + strlen("OK DUMP ");
			printf("Got status string %s from worker %d\n", statusString, worker);
			pstate->parallelSlot[worker].status =
				(AH->EndMasterParallelPtr)
					(AH, te, statusString, ACT_DUMP);
			printf("Making that a status of %d for worker %d\n", pstate->parallelSlot[worker].status, worker);
		}
		else
		{
			write_msg(NULL, "Invalid message received from worker: %s", msg);
			Assert(false);
			die_horribly(AH, modulename, "Invalid message received from worker: %s", msg);
		}
	}
	else
	{
		die_horribly(AH, modulename, "Invalid message received from worker: %s", msg);
	}
	PrintStatus(pstate);

	/* both Unix and Win32 return malloc()ed space */
	free(msg);
}

static int
ReapworkerStatus(ParallelState *pstate, int *status)
{
	int i;

	for (i = 0; i < pstate->numWorkers; i++)
	{
		if (pstate->parallelSlot[i].workerStatus == WRKR_FINISHED)
		{
			*status = pstate->parallelSlot[i].status;
			printf("Reaping worker status %d from %d\n", *status, i);
			pstate->parallelSlot[i].status = 0;
			pstate->parallelSlot[i].workerStatus = WRKR_IDLE;
			PrintStatus(pstate);
			return i;
		}
	}
	return NO_SLOT;
}

static char *
getMessageFromMaster(ArchiveHandle *AH, int pipefd[2])
{
	return readMessageFromPipe(pipefd[PIPE_READ], true);
}

static void
sendMessageToMaster(ArchiveHandle *AH, int pipefd[2], const char *str)
{
	int len = strlen(str) + 1;

	printf("Sending message %s to master on fd %d\n", str, pipefd[PIPE_WRITE]);

	if (writesocket(pipefd[PIPE_WRITE], str, len) != len)
		die_horribly(AH, modulename,
					 "Error writing to the communication channel: %s",
					 strerror(errno));
}

static void
sendMessageToWorker(ArchiveHandle *AH, ParallelState *pstate, int worker, const char *str)
{
	int len = strlen(str) + 1;

	printf("Sending message %s to worker %d on fd %d\n", str, worker, pstate->parallelSlot[worker].pipeWrite);

	if (writesocket(pstate->parallelSlot[worker].pipeWrite, str, len) != len)
		die_horribly(AH, modulename,
					 "Error writing to the communication channel: %s",
					 strerror(errno));
}

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
			ShutdownWorkersHard(AH, pstate);
			die_horribly(AH, modulename,
						 "A worker died while working on %s %s\n",
						 pstate->parallelSlot[i].args->te->desc,
						 pstate->parallelSlot[i].args->te->tag);
		}
	}
	printf("Nobody was ready...\n");
	Assert(false);
	return NULL;
}

static char *
readMessageFromPipe(int fd, bool do_wait)
{
	char   *msg;
	int		msgsize, bufsize;
	int		ret;

	/*
	 * The problem here is that we need to deal with several possibilites:
	 * we could receive only a partial message or several messages at once.
	 * The caller expects us to return exactly one message however.
	 *
	 * We could either read in as much as we can and keep track of what we
	 * delivered back to the caller or we just read byte by byte. Once we see
	 * (char) 0, we know that it's the message's end. This is quite inefficient
	 * but since we are reading only on the command channel, the performance
	 * loss does not seem worth the trouble of keeping internal states for
	 * different file descriptors.
	 */

	/* XXX set this to some reasonable initial value for a release */
	bufsize = 1;
	msg = (char *) malloc(bufsize);

	msgsize = 0;
	for (;;)
	{
		Assert(msgsize <= bufsize);
		/*
		 * If we do non-blocking read, only set the channel non-blocking for
		 * the very first character. We trust in our messages to terminate, if
		 * there is any character, we read the message and will find a \0 at
		 * the end.
		 */
		if (msgsize == 0 && !do_wait) {
			setnonblocking(fd);
		}

		if (do_wait)
		{
			fd_set read_set;
			int i;

			FD_ZERO(&read_set);
			FD_SET(fd, &read_set);

			/* abort on error, on Windows checks if we should terminate */
			i = select_loop(fd, &read_set);
			Assert(i != 0);
		}

		ret = readsocket(fd, msg + msgsize, 1);

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
			write_msg(NULL, "the communication partner died\n");
			exit(1);
		}
		if (ret < 0)
		{
			write_msg(NULL, "error reading from communication partner: %s\n",
					  strerror(errno));
			exit(1);
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

