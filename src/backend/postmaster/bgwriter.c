/*-------------------------------------------------------------------------
 *
 * bgwriter.c
 *
 * The background writer (bgwriter) is new as of Postgres 8.0.	It attempts
 * to keep regular backends from having to write out dirty shared buffers
 * (which they would only do when needing to free a shared buffer to read in
 * another page).  In the best scenario all writes from shared buffers will
 * be issued by the background writer process.	However, regular backends are
 * still empowered to issue writes if the bgwriter fails to maintain enough
 * clean shared buffers.
 *
 * As of Postgres 9.2 the bgwriter no longer handles checkpoints.
 *
 * The bgwriter is started by the postmaster as soon as the startup subprocess
 * finishes, or as soon as recovery begins if we are doing archive recovery.
 * It remains alive until the postmaster commands it to terminate.
 * Normal termination is by SIGUSR2, which instructs the bgwriter to exit(0).
 * Emergency termination is by SIGQUIT; like any
 * backend, the bgwriter will simply abort and exit on SIGQUIT.
 *
 * If the bgwriter exits unexpectedly, the postmaster treats that the same
 * as a backend crash: shared memory may be corrupted, so remaining backends
 * should be killed by SIGQUIT and then a recovery cycle started.  (Even if
 * shared memory isn't corrupted, we have lost information about which
 * files need to be fsync'd for the next checkpoint, and so a system
 * restart needs to be forced.)
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/bgwriter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "access/xlog_internal.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"


/*
 * GUC parameters
 */
int			BgWriterDelay = 200;

/*
 * Time to sleep between bgwriter rounds, when it has no work to do.
 */
#define BGWRITER_HIBERNATE_MS			10000

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t shutdown_requested = false;

/*
 * Private state
 */
static bool am_bg_writer = false;

/* Prototypes for private functions */

static void BgWriterNap(bool hibernating);

/* Signal handlers */

static void bg_quickdie(SIGNAL_ARGS);
static void BgSigHupHandler(SIGNAL_ARGS);
static void ReqShutdownHandler(SIGNAL_ARGS);
static void bgwriter_sigusr1_handler(SIGNAL_ARGS);


/*
 * Main entry point for bgwriter process
 *
 * This is invoked from BootstrapMain, which has already created the basic
 * execution environment, but not enabled signals yet.
 */
void
BackgroundWriterMain(void)
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext bgwriter_context;
	bool		hibernating;

	am_bg_writer = true;

	/*
	 * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too.	(bgwriter probably never has any
	 * child processes, but for consistency we make all postmaster child
	 * processes do this.)
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(FATAL, "setsid() failed: %m");
#endif

	/*
	 * Properly accept or ignore signals the postmaster might send us.
	 *
	 * bgwriter doesn't participate in ProcSignal signalling, but a SIGUSR1
	 * handler is still needed for latch wakeups.
	 */
	pqsignal(SIGHUP, BgSigHupHandler);	/* set flag to read config file */
	pqsignal(SIGINT, SIG_IGN);			/* as of 9.2 no longer requests checkpoint */
	pqsignal(SIGTERM, ReqShutdownHandler); 	/* shutdown */
	pqsignal(SIGQUIT, bg_quickdie);		/* hard crash time */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, bgwriter_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);
	pqsignal(SIGTTIN, SIG_DFL);
	pqsignal(SIGTTOU, SIG_DFL);
	pqsignal(SIGCONT, SIG_DFL);
	pqsignal(SIGWINCH, SIG_DFL);

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

	/*
	 * Advertise our latch that backends can use to wake us up while we're
	 * sleeping.
	 */
	ProcGlobal->bgwriterLatch = &MyProc->procLatch;

	/*
	 * Create a resource owner to keep track of our resources (currently only
	 * buffer pins).
	 */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "Background Writer");

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.  Formerly this code just ran in
	 * TopMemoryContext, but resetting that would be a really bad idea.
	 */
	bgwriter_context = AllocSetContextCreate(TopMemoryContext,
											 "Background Writer",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(bgwriter_context);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * See notes in postgres.c about the design of this coding.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * These operations are really just a minimal subset of
		 * AbortTransaction().	We don't have very many resources to worry
		 * about in bgwriter, but we do have LWLocks, buffers, and temp files.
		 */
		LWLockReleaseAll();
		AbortBufferIO();
		UnlockBuffers();
		/* buffer pins are released here: */
		ResourceOwnerRelease(CurrentResourceOwner,
							 RESOURCE_RELEASE_BEFORE_LOCKS,
							 false, true);
		/* we needn't bother with the other ResourceOwnerRelease phases */
		AtEOXact_Buffers(false);
		AtEOXact_Files();
		AtEOXact_HashTables(false);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(bgwriter_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(bgwriter_context);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  A write error is likely
		 * to be repeated, and we don't want to be filling the error logs as
		 * fast as we can.
		 */
		pg_usleep(1000000L);

		/*
		 * Close all open files after any error.  This is helpful on Windows,
		 * where holding deleted files open causes various strange errors.
		 * It's not clear we need it elsewhere, but shouldn't hurt.
		 */
		smgrcloseall();
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	/*
	 * Use the recovery target timeline ID during recovery
	 */
	if (RecoveryInProgress())
		ThisTimeLineID = GetRecoveryTargetTLI();

	/*
	 * Loop forever
	 */
	hibernating = false;
	for (;;)
	{
		bool		lapped;

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (!PostmasterIsAlive())
			exit(1);

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
			/* update global shmem state for sync rep */
		}
		if (shutdown_requested)
		{
			/*
			 * From here on, elog(ERROR) should end with exit(1), not send
			 * control back to the sigsetjmp block above
			 */
			ExitOnAnyError = true;
			/* Normal exit from the bgwriter is here */
			proc_exit(0);		/* done */
		}

		/*
		 * Do one cycle of dirty-buffer writing.
		 */
		if (hibernating && bgwriter_lru_maxpages > 0)
			ResetLatch(&MyProc->procLatch);
		lapped = BgBufferSync();

		if (lapped && !hibernating)
		{
			/*
			 * BgBufferSync did nothing. Since there doesn't seem to be any
			 * work for the bgwriter to do, go into slower-paced
			 * "hibernation" mode, where we sleep for much longer times than
			 * bgwriter_delay says. Fewer wakeups saves electricity. If a
			 * backend starts dirtying pages again, it will wake us up by
			 * setting our latch.
			 *
			 * The latch is kept set during productive cycles where buffers
			 * are written, and only reset before going into a longer sleep.
			 * That ensures that when there's a constant trickle of activity,
			 * the SetLatch() calls that backends have to do will see the
			 * latch as already set, and are not slowed down by having to
			 * actually set the latch and signal us.
			 */
			hibernating = true;

			/*
			 * Take one more short nap and perform one more bgwriter cycle -
			 * someone might've dirtied a buffer just after we finished the
			 * previous bgwriter cycle, while the latch was still set. If
			 * we still find nothing to do after this cycle, the next sleep
			 * will be longer.
			 */
			BgWriterNap(false);
			continue;
		}
		else if (!lapped && hibernating)
		{
			/*
			 * Woken up from hibernation. Set the latch just in case it's
			 * not set yet (usually we wake up from hibernation because a
			 * backend already set the latch, but not necessarily).
			 */
			SetLatch(&MyProc->procLatch);
			hibernating = false;
		}

		/*
		 * Take a short or long nap, depending on whether there was any work
		 * to do.
		 */
		BgWriterNap(hibernating);
	}
}

/*
 * BgWriterNap -- Nap for the configured time or until a signal is received.
 *
 * If 'hibernating' is false, sleeps for bgwriter_delay milliseconds.
 * Otherwise sleeps longer, but also wakes up if the process latch is set.
 */
static void
BgWriterNap(bool hibernating)
{
	long		udelay;

	/*
	 * Send off activity statistics to the stats collector
	 */
	pgstat_send_bgwriter();

	/*
	 * If there was no work to do in the previous bgwriter cycle, take a
	 * longer nap.
	 */
	if (hibernating)
	{
		/*
		 * We wake on a buffer being dirtied. It's possible that some
		 * useful work will become available for the bgwriter to do without
		 * a buffer actually being dirtied, like when a dirty buffer's usage
		 * count is decremented to zero or it's unpinned. This corner case
		 * is judged as too marginal to justify adding additional SetLatch()
		 * calls in very hot code paths, cheap though those calls may be.
		 *
		 * We still wake up periodically, so that BufferAlloc stats are
		 * updated reasonably promptly.
		 */
		int res = WaitLatch(&MyProc->procLatch,
							WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
							BGWRITER_HIBERNATE_MS);

		/*
		 * Only do a quick return if timeout was reached (or postmaster died)
		 * to ensure that no less than BgWriterDelay ms has passed between
		 * BgBufferSyncs - WaitLatch() might have returned instantaneously.
		 */
		if (res & (WL_TIMEOUT | WL_POSTMASTER_DEATH))
			return;
	}

	/*
	 * Nap for the configured time.
	 *
	 * On some platforms, signals won't interrupt the sleep.  To ensure we
	 * respond reasonably promptly when someone signals us, break down the
	 * sleep into 1-second increments, and check for interrupts after each
	 * nap.
	 */
	udelay = BgWriterDelay * 1000L;

	while (udelay > 999999L)
	{
		if (got_SIGHUP || shutdown_requested)
			break;
		pg_usleep(1000000L);
		udelay -= 1000000L;
	}

	if (!(got_SIGHUP || shutdown_requested))
		pg_usleep(udelay);
}

/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */

/*
 * bg_quickdie() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm,
 * so we need to stop what we're doing and exit.
 */
static void
bg_quickdie(SIGNAL_ARGS)
{
	PG_SETMASK(&BlockSig);

	/*
	 * We DO NOT want to run proc_exit() callbacks -- we're here because
	 * shared memory may be corrupted, so we don't want to try to clean up our
	 * transaction.  Just nail the windows shut and get out of town.  Now that
	 * there's an atexit callback to prevent third-party code from breaking
	 * things by calling exit() directly, we have to reset the callbacks
	 * explicitly to make this work as intended.
	 */
	on_exit_reset();

	/*
	 * Note we do exit(2) not exit(0).	This is to force the postmaster into a
	 * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
	 * backend.  This is necessary precisely because we don't clean up our
	 * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
	 * should ensure the postmaster sees this as a crash, too, but no harm in
	 * being doubly sure.)
	 */
	exit(2);
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
BgSigHupHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/* SIGTERM: set flag to shutdown and exit */
static void
ReqShutdownHandler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	shutdown_requested = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/* SIGUSR1: used for latch wakeups */
static void
bgwriter_sigusr1_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	latch_sigusr1_handler();

	errno = save_errno;
}
