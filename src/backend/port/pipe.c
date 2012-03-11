/*-------------------------------------------------------------------------
 *
 * pipe.c
 *	  pgpipe()
 *    piperead()
 *
 * Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *	This is a wrapper to call pgwin32_pipe() in Windows and report any
 *  errors via ereport(LOG, ...). The backend checks the return code and
 *  can throw a more fatal error if necessary.
 *
 * IDENTIFICATION
 *	  src/backend/port/pipe.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef WIN32
int
pgpipe(int handles[2])
{
	char	   *error_string;
	int			error_code;
	int			ret;

	if ((ret = pgwin32_pipe(handles, &error_string, &error_code)) != 0)
		ereport(LOG, (errmsg_internal("%s: %ui", error_string, error_code)));

	return ret;
}

int
piperead(int s, char *buf, int len)
{
	return pgwin32_piperead(s, buf, len);
}

#endif
