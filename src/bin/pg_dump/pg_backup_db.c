/*-------------------------------------------------------------------------
 *
 * pg_backup_db.c
 *
 *	Implements the basic DB functions used by the archiver.
 *
 * IDENTIFICATION
 *	  src/bin/pg_dump/pg_backup_db.c
 *
 *-------------------------------------------------------------------------
 */

#include "pg_backup_db.h"
#include "dumputils.h"

#include <unistd.h>
#include <ctype.h>
#ifdef HAVE_TERMIOS_H
#include <termios.h>
#endif


#define DB_MAX_ERR_STMT 128

static const char *modulename = gettext_noop("archiver (db)");

static void _check_database_version(ArchiveHandle *AH);
static PGconn *_connectDB(ArchiveHandle *AH, const char *newdbname, const char *newUser);
static void notice_processor(void *arg, const char *message);
static char* get_synchronized_snapshot(ArchiveHandle *AH);
static void ExecuteSqlCommand(ArchiveHandle *AH, const char *qry, const char *desc);

static int
_parse_version(ArchiveHandle *AH, const char *versionString)
{
	int			v;

	v = parse_version(versionString);
	if (v < 0)
		die_horribly(AH, modulename, "could not parse version string \"%s\"\n", versionString);

	return v;
}

static void
_check_database_version(ArchiveHandle *AH)
{
	int			myversion;
	const char *remoteversion_str;
	int			remoteversion;

	myversion = _parse_version(AH, PG_VERSION);

	remoteversion_str = PQparameterStatus(AH->connection, "server_version");
	if (!remoteversion_str)
		die_horribly(AH, modulename, "could not get server_version from libpq\n");

	remoteversion = _parse_version(AH, remoteversion_str);

	AH->public.remoteVersionStr = strdup(remoteversion_str);
	AH->public.remoteVersion = remoteversion;
	if (!AH->archiveRemoteVersion)
		AH->archiveRemoteVersion = AH->public.remoteVersionStr;

	if (myversion != remoteversion
		&& (remoteversion < AH->public.minRemoteVersion ||
			remoteversion > AH->public.maxRemoteVersion))
	{
		write_msg(NULL, "server version: %s; %s version: %s\n",
				  remoteversion_str, progname, PG_VERSION);
		die_horribly(AH, NULL, "aborting because of server version mismatch\n");
	}
}

/*
 * Reconnect to the server.  If dbname is not NULL, use that database,
 * else the one associated with the archive handle.  If username is
 * not NULL, use that user name, else the one from the handle.	If
 * both the database and the user match the existing connection already,
 * nothing will be done.
 *
 * Returns 1 in any case.
 */
int
ReconnectToServer(ArchiveHandle *AH, const char *dbname, const char *username)
{
	PGconn	   *newConn;
	const char *newdbname;
	const char *newusername;

	if (!dbname)
		newdbname = PQdb(AH->connection);
	else
		newdbname = dbname;

	if (!username)
		newusername = PQuser(AH->connection);
	else
		newusername = username;

	/* Let's see if the request is already satisfied */
	if (strcmp(newdbname, PQdb(AH->connection)) == 0 &&
		strcmp(newusername, PQuser(AH->connection)) == 0)
		return 1;

	newConn = _connectDB(AH, newdbname, newusername);

	PQfinish(AH->connection);
	AH->connection = newConn;

	return 1;
}

/*
 * Connect to the db again.
 *
 * Note: it's not really all that sensible to use a single-entry password
 * cache if the username keeps changing.  In current usage, however, the
 * username never does change, so one savedPassword is sufficient.	We do
 * update the cache on the off chance that the password has changed since the
 * start of the run.
 */
static PGconn *
_connectDB(ArchiveHandle *AH, const char *reqdb, const char *requser)
{
	PGconn	   *newConn;
	const char *newdb;
	const char *newuser;
	char	   *password = AH->connParams.savedPassword;
	bool		new_pass;

	if (!reqdb)
		newdb = PQdb(AH->connection);
	else
		newdb = reqdb;

	if (!requser || strlen(requser) == 0)
		newuser = PQuser(AH->connection);
	else
		newuser = requser;

	ahlog(AH, 1, "connecting to database \"%s\" as user \"%s\"\n",
		  newdb, newuser);

	if (AH->promptPassword == TRI_YES && password == NULL)
	{
		password = simple_prompt("Password: ", 100, false);
		if (password == NULL)
			die_horribly(AH, modulename, "out of memory\n");
	}

	do
	{
#define PARAMS_ARRAY_SIZE	7
		const char **keywords = malloc(PARAMS_ARRAY_SIZE * sizeof(*keywords));
		const char **values = malloc(PARAMS_ARRAY_SIZE * sizeof(*values));

		if (!keywords || !values)
			die_horribly(AH, modulename, "out of memory\n");

		keywords[0] = "host";
		values[0] = PQhost(AH->connection);
		keywords[1] = "port";
		values[1] = PQport(AH->connection);
		keywords[2] = "user";
		values[2] = newuser;
		keywords[3] = "password";
		values[3] = password;
		keywords[4] = "dbname";
		values[4] = newdb;
		keywords[5] = "fallback_application_name";
		values[5] = progname;
		keywords[6] = NULL;
		values[6] = NULL;

		new_pass = false;
		newConn = PQconnectdbParams(keywords, values, true);

		free(keywords);
		free(values);

		if (!newConn)
			die_horribly(AH, modulename, "failed to reconnect to database\n");

		if (PQstatus(newConn) == CONNECTION_BAD)
		{
			if (!PQconnectionNeedsPassword(newConn))
				die_horribly(AH, modulename, "could not reconnect to database: %s",
							 PQerrorMessage(newConn));
			PQfinish(newConn);

			if (password)
				fprintf(stderr, "Password incorrect\n");

			fprintf(stderr, "Connecting to %s as %s\n",
					newdb, newuser);

			if (password)
				free(password);

			if (AH->promptPassword != TRI_NO)
				password = simple_prompt("Password: ", 100, false);
			else
				die_horribly(AH, modulename, "connection needs password\n");

			if (password == NULL)
				die_horribly(AH, modulename, "out of memory\n");
			new_pass = true;
		}
	} while (new_pass);

	AH->connParams.savedPassword = password;

	/* check for version mismatch */
	_check_database_version(AH);

	PQsetNoticeProcessor(newConn, notice_processor, NULL);

	return newConn;
}


/*
 * Make a database connection with the given parameters.  The
 * connection handle is returned, the parameters are stored in AHX.
 * An interactive password prompt is automatically issued if required.
 *
 * Note: it's not really all that sensible to use a single-entry password
 * cache if the username keeps changing.  In current usage, however, the
 * username never does change, so one savedPassword is sufficient.
 */
PGconn *
ConnectDatabase(Archive *AHX,
				const char *dbname,
				const char *pghost,
				const char *pgport,
				const char *username,
				enum trivalue prompt_password)
{
	ArchiveHandle *AH = (ArchiveHandle *) AHX;
	char	   *password = AH->connParams.savedPassword;
	bool		new_pass;

	if (AH->connection)
		die_horribly(AH, modulename, "already connected to a database\n");

	if (prompt_password == TRI_YES && password == NULL)
	{
		password = simple_prompt("Password: ", 100, false);
		if (password == NULL)
			die_horribly(AH, modulename, "out of memory\n");
	}
	AH->promptPassword = prompt_password;

	/*
	 * Start the connection.  Loop until we have a password if requested by
	 * backend.
	 */
	do
	{
#define PARAMS_ARRAY_SIZE	7
		const char **keywords = malloc(PARAMS_ARRAY_SIZE * sizeof(*keywords));
		const char **values = malloc(PARAMS_ARRAY_SIZE * sizeof(*values));

		if (!keywords || !values)
			die_horribly(AH, modulename, "out of memory\n");

		keywords[0] = "host";
		values[0] = pghost;
		keywords[1] = "port";
		values[1] = pgport;
		keywords[2] = "user";
		values[2] = username;
		keywords[3] = "password";
		values[3] = password;
		keywords[4] = "dbname";
		values[4] = dbname;
		keywords[5] = "fallback_application_name";
		values[5] = progname;
		keywords[6] = NULL;
		values[6] = NULL;

		new_pass = false;
		AH->connection = PQconnectdbParams(keywords, values, true);

		free(keywords);
		free(values);

		if (!AH->connection)
			die_horribly(AH, modulename, "failed to connect to database\n");

		if (PQstatus(AH->connection) == CONNECTION_BAD &&
			PQconnectionNeedsPassword(AH->connection) &&
			password == NULL &&
			prompt_password != TRI_NO)
		{
			PQfinish(AH->connection);
			password = simple_prompt("Password: ", 100, false);
			if (password == NULL)
				die_horribly(AH, modulename, "out of memory\n");
			new_pass = true;
		}
	} while (new_pass);

	if (dbname)
		AH->connParams.dbname = strdup(dbname);
	if (pgport)
		AH->connParams.pgport = strdup(pgport);
	if (pghost)
		AH->connParams.pghost = strdup(pghost);
	if (username)
		AH->connParams.username = strdup(username);
	if (password)
		AH->connParams.savedPassword = strdup(password);

	/* check to see that the backend connection was successfully made */
	if (PQstatus(AH->connection) == CONNECTION_BAD)
		die_horribly(AH, modulename, "connection to database \"%s\" failed: %s",
					 PQdb(AH->connection), PQerrorMessage(AH->connection));

	/* check for version mismatch */
	_check_database_version(AH);

	PQsetNoticeProcessor(AH->connection, notice_processor, NULL);

	return AH->connection;
}

PGconn *
CloneDatabaseConnection(Archive *AHX)
{
	ArchiveHandle *AH = (ArchiveHandle *) AHX;

	AH->connection = NULL;
	AH->connParams.is_clone = 1;

	printf("Connecting: Db: %s host %s port %s user %s\n",
					AH->connParams.dbname,
					AH->connParams.pghost ? AH->connParams.pghost : "(null)",
					AH->connParams.pgport ? AH->connParams.pgport : "(null)",
					AH->connParams.username ? AH->connParams.username : "(null)");

	/* savedPassword must be local in case we change it while connecting */
	if (AH->connParams.savedPassword)
		AH->connParams.savedPassword = strdup(AH->connParams.savedPassword);

	return ConnectDatabase(AHX,
						   AH->connParams.dbname,
						   AH->connParams.pghost,
						   AH->connParams.pgport,
						   AH->connParams.username,
						   TRI_NO);
}

void
SetupConnection(Archive *AHX, const char *dumpencoding, const char *use_role, int serializable_deferrable)
{
#define do_sql_command(conn, sql) \
		ExecuteSqlCommand(AH, sql, "could not set up the connection")
	ArchiveHandle *AH = (ArchiveHandle *) AHX;
	const char *std_strings;
	PGconn *conn = AH->connection;

	/* Set the client encoding if requested */
	if (!dumpencoding && AH->connParams.encoding)
		dumpencoding = AH->connParams.encoding;

	if (dumpencoding)
	{
		if (PQsetClientEncoding(AH->connection, dumpencoding) < 0)
		{
			write_msg(NULL, "invalid client encoding \"%s\" specified\n",
					  dumpencoding);
			exit(1);
		}

		/* save this for later use on parallel connections */
		if (!AH->connParams.encoding)
			AH->connParams.encoding = strdup(dumpencoding);
	}

	/*
	 * Get the active encoding and the standard_conforming_strings setting, so
	 * we know how to escape strings.
	 */
	AHX->encoding = PQclientEncoding(conn);

	std_strings = PQparameterStatus(conn, "standard_conforming_strings");
	AHX->std_strings = (std_strings && strcmp(std_strings, "on") == 0);

	/* Set the role if requested */
	if (!use_role && AH->connParams.use_role)
		use_role = AH->connParams.use_role;

	if (use_role && AHX->remoteVersion >= 80100)
	{
		PQExpBuffer query = createPQExpBuffer();

		appendPQExpBuffer(query, "SET ROLE %s", fmtId(use_role));
		do_sql_command(conn, query->data);
		destroyPQExpBuffer(query);

		/* save this for later use on parallel connections */
		if (!AH->connParams.use_role)
			AH->connParams.use_role = strdup(use_role);
	}

	/* Set the datestyle to ISO to ensure the dump's portability */
	do_sql_command(conn, "SET DATESTYLE = ISO");

	/* Likewise, avoid using sql_standard intervalstyle */
	if (AHX->remoteVersion >= 80400)
		do_sql_command(conn, "SET INTERVALSTYLE = POSTGRES");

	/*
	 * If supported, set extra_float_digits so that we can dump float data
	 * exactly (given correctly implemented float I/O code, anyway)
	 */
	if (AHX->remoteVersion >= 80500)
		do_sql_command(conn, "SET extra_float_digits TO 3");
	else if (AHX->remoteVersion >= 70400)
		do_sql_command(conn, "SET extra_float_digits TO 2");

	/*
	 * If synchronized scanning is supported, disable it, to prevent
	 * unpredictable changes in row ordering across a dump and reload.
	 */
	if (AHX->remoteVersion >= 80300)
		do_sql_command(conn, "SET synchronize_seqscans TO off");

	/*
	 * Quote all identifiers, if requested.
	 */
	if (quote_all_identifiers && AHX->remoteVersion >= 90100)
		do_sql_command(conn, "SET quote_all_identifiers = true");

	/*
	 * Disable timeouts if supported.
	 */
	if (AHX->remoteVersion >= 70300)
		do_sql_command(conn, "SET statement_timeout = 0");

	/*
	 * Start serializable transaction to dump consistent data.
	 */
	do_sql_command(conn, "BEGIN");

	do_sql_command(conn, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");

	/*
	 * Quote all identifiers, if requested.
	 */
	if (quote_all_identifiers && AHX->remoteVersion >= 90100)
		do_sql_command(conn, "SET quote_all_identifiers = true");

	/*
	 * Start transaction-snapshot mode transaction to dump consistent data.
	 */
	do_sql_command(conn, "BEGIN");
	if (AHX->remoteVersion >= 90100)
	{
		if (serializable_deferrable)
			do_sql_command(conn,
						   "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, "
						   "READ ONLY, DEFERRABLE");
		else
			do_sql_command(conn,
						   "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
	}
	else
	{
		do_sql_command(conn, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
	}

	if (AHX->numWorkers > 1 && AHX->remoteVersion >= 90200)
	{
		if (AH->connParams.is_clone)
		{
			PQExpBuffer query = createPQExpBuffer();
			appendPQExpBuffer(query, "SET TRANSACTION SNAPSHOT ");
			appendStringLiteralConn(query, AH->connParams.sync_snapshot_id, conn);
			destroyPQExpBuffer(query);
		}
		else {
			/*
			 * If the version is lower and we don't have synchronized snapshots
			 * yet, we will error out earlier already. So either we have the
			 * feature or the user has given the explicit command not to use it.
			 * Note: If we have it, we always use it, you cannot switch it off
			 * then.
			 */
			if (AHX->remoteVersion >= 90200)
				AH->connParams.sync_snapshot_id = get_synchronized_snapshot(AH);
		}
	}
#undef do_sql_command
}

static char*
get_synchronized_snapshot(ArchiveHandle *AH)
{
	const char *query = "select pg_export_snapshot()";
	char	   *result;
	int			ntups;
	PGconn	   *conn = AH->connection;
	PGresult   *res = PQexec(conn, query);

//	check_sql_result(res, conn, query, PGRES_TUPLES_OK);

	/* Expecting a single result only */
	ntups = PQntuples(res);
	if (ntups != 1)
	{
		die_horribly(AH, modulename, ngettext("query returned %d row instead of one: %s\n",
							   "query returned %d rows instead of one: %s\n",
								 ntups),
				  ntups, query);
	}

	result = strdup(PQgetvalue(res, 0, 0));
	PQclear(res);

	return result;
}

static void
notice_processor(void *arg, const char *message)
{
	write_msg(NULL, "%s", message);
}

/*
 * Convenience function to send a query.
 * Monitors result to detect COPY statements
 */
static void
ExecuteSqlCommand(ArchiveHandle *AH, const char *qry, const char *desc)
{
	PGconn	   *conn = AH->connection;
	PGresult   *res;
	char		errStmt[DB_MAX_ERR_STMT];

#ifdef NOT_USED
	fprintf(stderr, "Executing: '%s'\n\n", qry);
#endif
	res = PQexec(conn, qry);

	switch (PQresultStatus(res))
	{
		case PGRES_COMMAND_OK:
		case PGRES_TUPLES_OK:
		case PGRES_EMPTY_QUERY:
			/* A-OK */
			break;
		case PGRES_COPY_IN:
			/* Assume this is an expected result */
			AH->pgCopyIn = true;
			break;
		default:
			/* trouble */
			strncpy(errStmt, qry, DB_MAX_ERR_STMT);
			if (errStmt[DB_MAX_ERR_STMT - 1] != '\0')
			{
				errStmt[DB_MAX_ERR_STMT - 4] = '.';
				errStmt[DB_MAX_ERR_STMT - 3] = '.';
				errStmt[DB_MAX_ERR_STMT - 2] = '.';
				errStmt[DB_MAX_ERR_STMT - 1] = '\0';
			}
			warn_or_die_horribly(AH, modulename, "%s: %s    Command was: %s\n",
								 desc, PQerrorMessage(conn), errStmt);
			break;
	}

	PQclear(res);
}


/*
 * Implement ahwrite() for direct-to-DB restore
 */
int
ExecuteSqlCommandBuf(ArchiveHandle *AH, const char *buf, size_t bufLen)
{
	if (AH->writingCopyData)
	{
		/*
		 * We drop the data on the floor if libpq has failed to enter COPY
		 * mode; this allows us to behave reasonably when trying to continue
		 * after an error in a COPY command.
		 */
		if (AH->pgCopyIn &&
			PQputCopyData(AH->connection, buf, bufLen) <= 0)
			die_horribly(AH, modulename, "error returned by PQputCopyData: %s",
						 PQerrorMessage(AH->connection));
	}
	else
	{
		/*
		 * In most cases the data passed to us will be a null-terminated
		 * string, but if it's not, we have to add a trailing null.
		 */
		if (buf[bufLen] == '\0')
			ExecuteSqlCommand(AH, buf, "could not execute query");
		else
		{
			char   *str = (char *) malloc(bufLen + 1);

			if (!str)
				die_horribly(AH, modulename, "out of memory\n");
			memcpy(str, buf, bufLen);
			str[bufLen] = '\0';
			ExecuteSqlCommand(AH, str, "could not execute query");
			free(str);
		}
	}

	return 1;
}

/*
 * Terminate a COPY operation during direct-to-DB restore
 */
void
EndDBCopyMode(ArchiveHandle *AH, TocEntry *te)
{
	if (AH->pgCopyIn)
	{
		PGresult   *res;

		if (PQputCopyEnd(AH->connection, NULL) <= 0)
			die_horribly(AH, modulename, "error returned by PQputCopyEnd: %s",
						 PQerrorMessage(AH->connection));

		/* Check command status and return to normal libpq state */
		res = PQgetResult(AH->connection);
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			warn_or_die_horribly(AH, modulename, "COPY failed for table \"%s\": %s",
								 te->tag, PQerrorMessage(AH->connection));
		PQclear(res);

		AH->pgCopyIn = false;
	}
}

void
StartTransaction(ArchiveHandle *AH)
{
	ExecuteSqlCommand(AH, "BEGIN", "could not start database transaction");
}

void
CommitTransaction(ArchiveHandle *AH)
{
	ExecuteSqlCommand(AH, "COMMIT", "could not commit database transaction");
}

void
DropBlobIfExists(ArchiveHandle *AH, Oid oid)
{
	/*
	 * If we are not restoring to a direct database connection, we have to
	 * guess about how to detect whether the blob exists.  Assume new-style.
	 */
	if (AH->connection == NULL ||
		PQserverVersion(AH->connection) >= 90000)
	{
		ahprintf(AH,
				 "SELECT pg_catalog.lo_unlink(oid) "
				 "FROM pg_catalog.pg_largeobject_metadata "
				 "WHERE oid = '%u';\n",
				 oid);
	}
	else
	{
		/* Restoring to pre-9.0 server, so do it the old way */
		ahprintf(AH,
				 "SELECT CASE WHEN EXISTS("
				 "SELECT 1 FROM pg_catalog.pg_largeobject WHERE loid = '%u'"
				 ") THEN pg_catalog.lo_unlink('%u') END;\n",
				 oid, oid);
	}
}

