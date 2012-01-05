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

extern void myExit(int status);
extern int GetIdleWorker(ParallelState *pstate);
extern bool IsEveryWorkerIdle(ParallelState *pstate);
extern void ListenToWorkers(ArchiveHandle *AH, ParallelState *pstate, bool do_wait);
extern int ReapWorkerStatus(ParallelState *pstate, int *status);
extern void EnsureIdleWorker(ArchiveHandle *AH, ParallelState *pstate);
extern void EnsureWorkersFinished(ArchiveHandle *AH, ParallelState *pstate);

