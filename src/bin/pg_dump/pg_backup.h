/*-------------------------------------------------------------------------
 *
 * pg_backup.h
 *
 *	Public interface to the pg_dump archiver routines.
 *
 *	See the headers to pg_restore for more details.
 *
 * Copyright (c) 2000, Philip Warner
 *		Rights are granted to use this software in any way so long
 *		as this notice is not removed.
 *
 *	The author is not responsible for loss or damages that may
 *	result from it's use.
 *
 *
 * IDENTIFICATION
 *		src/bin/pg_dump/pg_backup.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_BACKUP_H
#define PG_BACKUP_H

#include "postgres_fe.h"

#include "pg_dump.h"

#include "libpq-fe.h"


#define atooid(x)  ((Oid) strtoul((x), NULL, 10))
#define oidcmp(x,y) ( ((x) < (y) ? -1 : ((x) > (y)) ?  1 : 0) )
#define oideq(x,y) ( (x) == (y) )
#define oidle(x,y) ( (x) <= (y) )
#define oidge(x,y) ( (x) >= (y) )
#define oidzero(x) ( (x) == 0 )

enum trivalue
{
	TRI_DEFAULT,
	TRI_NO,
	TRI_YES
};

typedef enum _archiveFormat
{
	archUnknown = 0,
	archCustom = 1,
	archFiles = 2,
	archTar = 3,
	archNull = 4,
	archDirectory = 5
} ArchiveFormat;

typedef enum _archiveMode
{
	archModeAppend,
	archModeWrite,
	archModeRead
} ArchiveMode;

typedef enum _teSection
{
	SECTION_NONE = 1,			/* COMMENTs, ACLs, etc; can be anywhere */
	SECTION_PRE_DATA,			/* stuff to be processed before data */
	SECTION_DATA,				/* TABLE DATA, BLOBS, BLOB COMMENTS */
	SECTION_POST_DATA			/* stuff to be processed after data */
} teSection;

/*
 *	We may want to have some more user-readable data, but in the mean
 *	time this gives us some abstraction and type checking.
 */
typedef struct _Archive
{
	int			verbose;
	char	   *remoteVersionStr;		/* server's version string */
	int			remoteVersion;	/* same in numeric form */

	int			minRemoteVersion;		/* allowable range */
	int			maxRemoteVersion;

	int			numWorkers;		/* number of parallel processes */

	/* info needed for string escaping */
	int			encoding;		/* libpq code for client_encoding */
	bool		std_strings;	/* standard_conforming_strings */

	/* error handling */
	bool		exit_on_error;	/* whether to exit on SQL errors... */
	int			n_errors;		/* number of errors (if no die) */

	/* The rest is private */
} Archive;

typedef int (*DataDumperPtr) (Archive *AH, void *userArg);

typedef struct _connParams
{
	char	   *dbname;
	char	   *pgport;
	char	   *pghost;
	char	   *username;
	char       *savedPassword;
	char	   *use_role;		/* Issue SET ROLE to this */
	char	   *encoding;
	char	   *sync_snapshot_id;
	bool		is_clone;
} ConnParams;

typedef struct _restoreOptions
{
	int			createDB;		/* Issue commands to create the database */
	int			noOwner;		/* Don't try to match original object owner */
	int			noTablespace;	/* Don't issue tablespace-related commands */
	int			disable_triggers;		/* disable triggers during data-only
										 * restore */
	int			use_setsessauth;/* Use SET SESSION AUTHORIZATION commands
								 * instead of OWNER TO */
	int			no_security_labels;		/* Skip security label entries */
	char	   *superuser;		/* Username to use as superuser */
	int			dataOnly;
	int			dropSchema;
	char	   *filename;
	int			schemaOnly;
	int			verbose;
	int			aclsSkip;
	int			tocSummary;
	char	   *tocFile;
	int			format;
	char	   *formatName;

	int			selTypes;
	int			selIndex;
	int			selFunction;
	int			selTrigger;
	int			selTable;
	char	   *indexNames;
	char	   *functionNames;
	char	   *tableNames;
	char	   *schemaNames;
	char	   *triggerNames;

	ConnParams	connParams;
	enum trivalue promptPassword;

	int			useDB;
	int			noDataForFailedTables;
	int			exit_on_error;
	int			compression;
	int			suppressDumpWarnings;	/* Suppress output of WARNING entries
										 * to stderr */
	bool		single_txn;

	bool	   *idWanted;		/* array showing which dump IDs to emit */
} RestoreOptions;

/*
 * Main archiver interface.
 */

extern void
exit_horribly(Archive *AH, const char *modulename, const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 4)));


/* Lets the archive know we have a DB connection to shutdown if it dies */

PGconn *ConnectDatabase(Archive *AHX, const char *dbname, const char *pghost,
						const char *pgport, const char *username,
						enum trivalue prompt_password);
PGconn *CloneDatabaseConnection(Archive *AHX);

/* Called to add a TOC entry */
extern void ArchiveEntry(Archive *AHX,
			 CatalogId catalogId, DumpId dumpId,
			 const char *tag,
			 const char *namespace, const char *tablespace,
			 const char *owner,
			 unsigned long int relpages, bool withOids,
			 const char *desc, teSection section,
			 const char *defn,
			 const char *dropStmt, const char *copyStmt,
			 const DumpId *deps, int nDeps,
			 DataDumperPtr dumpFn, void *dumpArg);

/* Called to write *data* to the archive */
extern size_t WriteData(Archive *AH, const void *data, size_t dLen);

extern int	StartBlob(Archive *AH, Oid oid);
extern int	EndBlob(Archive *AH, Oid oid);

extern void CloseArchive(Archive *AH);

extern void RestoreArchive(Archive *AH, RestoreOptions *ropt);

/* Open an existing archive */
extern Archive *OpenArchive(const char *FileSpec, const ArchiveFormat fmt);

/* Create a new archive */
extern Archive *CreateArchive(const char *FileSpec, const ArchiveFormat fmt,
			  const int compression, ArchiveMode mode);

/* The --list option */
extern void PrintTOCSummary(Archive *AH, RestoreOptions *ropt);

extern RestoreOptions *NewRestoreOptions(void);

/* We have one in pg_dump.c and another one in pg_restore.c */
void _SetupWorker(Archive *AHX, RestoreOptions *ropt);

/* Rearrange and filter TOC entries */
extern void SortTocFromFile(Archive *AHX, RestoreOptions *ropt);
extern void InitDummyWantedList(Archive *AHX, RestoreOptions *ropt);

/* Convenience functions used only when writing DATA */
extern int	archputs(const char *s, Archive *AH);
extern int
archprintf(Archive *AH, const char *fmt,...)
/* This extension allows gcc to check the format string */
__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

#define appendStringLiteralAH(buf,str,AH) \
	appendStringLiteral(buf, str, (AH)->encoding, (AH)->std_strings)

#endif   /* PG_BACKUP_H */
