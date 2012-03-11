/*-------------------------------------------------------------------------
 *
 * win32pipe.c
 *	  pgwin32_pipe()
 *
 * Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *	This is a replacement version of pipe for Win32 which allows
 *	returned handles to be used in select(). Note that read/write calls
 *	must be replaced with recv/send.
 *
 * IDENTIFICATION
 *	  src/port/win32pipe.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

int
pgwin32_pipe(int handles[2], char **error_string, int *error_code)
{
	SOCKET		s;
	struct sockaddr_in serv_addr;
	int			len = sizeof(serv_addr);

	handles[0] = handles[1] = INVALID_SOCKET;

	if ((s = socket(AF_INET, SOCK_STREAM, 0)) == INVALID_SOCKET)
	{
		*error_code = WSAGetLastError();
		*error_string = "pgwin32_pipe could not create socket";
		return -1;
	}

	memset((void *) &serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(0);
	serv_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	if (bind(s, (SOCKADDR *) &serv_addr, len) == SOCKET_ERROR)
	{
		*error_code = WSAGetLastError();
		*error_string = "pgwin32_pipe could not bind";
		closesocket(s);
		return -1;
	}
	if (listen(s, 1) == SOCKET_ERROR)
	{
		*error_code = WSAGetLastError();
		*error_string = "pgwin32_pipe could not listen";
		closesocket(s);
		return -1;
	}
	if (getsockname(s, (SOCKADDR *) &serv_addr, &len) == SOCKET_ERROR)
	{
		*error_code = WSAGetLastError();
		*error_string = "pgwin32_pipe could not getsockname";
		closesocket(s);
		return -1;
	}
	if ((handles[1] = socket(PF_INET, SOCK_STREAM, 0)) == INVALID_SOCKET)
	{
		*error_code = WSAGetLastError();
		*error_string = "pgwin32_pipe could not create socket";
		closesocket(s);
		return -1;
	}

	if (connect(handles[1], (SOCKADDR *) &serv_addr, len) == SOCKET_ERROR)
	{
		*error_code = WSAGetLastError();
		*error_string = "pgwin32_pipe could not connect socket";
		closesocket(s);
		return -1;
	}
	if ((handles[0] = accept(s, (SOCKADDR *) &serv_addr, &len)) == INVALID_SOCKET)
	{
		*error_code = WSAGetLastError();
		*error_string = "pgwin32_pipe could not accept socket";
		closesocket(handles[1]);
		handles[1] = INVALID_SOCKET;
		closesocket(s);
		return -1;
	}
	closesocket(s);
	return 0;
}


int
pgwin32_piperead(int s, char *buf, int len)
{
	int			ret = recv(s, buf, len, 0);

	if (ret < 0 && WSAGetLastError() == WSAECONNRESET)
		/* EOF on the pipe! (win32 socket based implementation) */
		ret = 0;
	return ret;
}

#endif
