/*-------------------------------------------------------------------------
 *
 * parallel.h
 *
 *	Parallel support header file for the pg_dump archiver
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	The author is not responsible for loss or damages that may
 *	result from its use.
 *
 * IDENTIFICATION
 *		src/bin/pg_dump/parallel.h
 *
 *-------------------------------------------------------------------------
 */

#include "pg_backup_db.h"

struct _archiveHandle;
struct _tocEntry;

typedef enum
{
	WRKR_TERMINATED = 0,
	WRKR_IDLE,
	WRKR_WORKING,
	WRKR_FINISHED
} T_WorkerStatus;

typedef enum _action
{
	ACT_DUMP,
	ACT_RESTORE,
} T_Action;

/* Arguments needed for a worker process */
typedef struct _parallel_args
{
	struct _archiveHandle *AH;
	struct _tocEntry	  *te;
} ParallelArgs;

/* State for each parallel activity slot */
typedef struct _parallel_slot
{
	ParallelArgs	   *args;
	T_WorkerStatus		workerStatus;
	int					status;
	int					pipeRead;
	int					pipeWrite;
	int					pipeRevRead;
	int					pipeRevWrite;
#ifdef WIN32
	uintptr_t			hThread;
	unsigned int		threadId;
#else
	pid_t				pid;
#endif
} ParallelSlot;

#define NO_SLOT (-1)

typedef struct _parallel_state
{
	int			numWorkers;
	ParallelSlot *parallelSlot;
} ParallelState;

extern int GetIdleWorker(ParallelState *pstate);
extern bool IsEveryWorkerIdle(ParallelState *pstate);
extern void ListenToWorkers(struct _archiveHandle *AH, ParallelState *pstate, bool do_wait);
extern int ReapWorkerStatus(ParallelState *pstate, int *status);
extern void EnsureIdleWorker(struct _archiveHandle *AH, ParallelState *pstate);
extern void EnsureWorkersFinished(struct _archiveHandle *AH, ParallelState *pstate);

extern ParallelState *ParallelBackupStart(struct _archiveHandle *AH,
										  RestoreOptions *ropt);
extern void DispatchJobForTocEntry(struct _archiveHandle *AH,
								   ParallelState *pstate,
								   struct _tocEntry *te, T_Action act);
extern void ParallelBackupEnd(struct _archiveHandle *AH, ParallelState *pstate);

extern void checkAborting(struct _archiveHandle *AH);

