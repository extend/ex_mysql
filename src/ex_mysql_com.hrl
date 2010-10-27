-define(COM_QUIT, 1).
-define(COM_INIT_DB, 2).
-define(COM_QUERY, 3).
-define(COM_FIELD_LIST, 4).
-define(COM_REFRESH, 7).
-define(COM_SHUTDOWN, 8).
-define(COM_STATISTICS, 9).
-define(COM_PROCESS_INFO, 10).
-define(COM_PROCESS_KILL, 12).
-define(COM_DEBUG, 13).
-define(COM_PING, 14).
-define(COM_CHANGE_USER, 17).
-define(COM_STMT_PREPARE, 22).
-define(COM_STMT_EXECUTE, 23).
-define(COM_STMT_SEND_LONG_DATA, 24).
-define(COM_STMT_CLOSE, 25).
-define(COM_STMT_RESET, 26).
-define(COM_SET_OPTION, 27).
-define(COM_STMT_FETCH, 28).

-define(CLIENT_LONG_PASSWORD, 1).
-define(CLIENT_FOUND_ROWS, 2).
-define(CLIENT_LONG_FLAG, 4).
-define(CLIENT_CONNECT_WITH_DB, 8).
-define(CLIENT_NO_SCHEMA, 16).
-define(CLIENT_COMPRESS, 32).
-define(CLIENT_LOCAL_FILES, 128).
-define(CLIENT_IGNORE_SPACE, 256).
-define(CLIENT_PROTOCOL_41, 512).
-define(CLIENT_INTERACTIVE, 1024).
-define(CLIENT_SSL, 2048).
-define(CLIENT_TRANSACTIONS, 8192).
-define(CLIENT_RESERVED, 16384).
-define(CLIENT_SECURE_CONNECTION, 32768).

-define(DEFAULT_CAPS, ?CLIENT_LONG_PASSWORD).

-define(SERVER_STATUS_NO_BACKSLASH_ESCAPES, 512).

-define(FIELD_TYPE_DECIMAL, 0).
-define(FIELD_TYPE_TINY, 1).
-define(FIELD_TYPE_SHORT, 2).
-define(FIELD_TYPE_LONG, 3).
-define(FIELD_TYPE_FLOAT, 4).
-define(FIELD_TYPE_DOUBLE, 5).
-define(FIELD_TYPE_NULL, 6).
-define(FIELD_TYPE_TIMESTAMP, 7).
-define(FIELD_TYPE_LONGLONG, 8).
-define(FIELD_TYPE_INT24, 9).
-define(FIELD_TYPE_DATE, 10).
-define(FIELD_TYPE_TIME, 11).
-define(FIELD_TYPE_DATETIME, 12).
-define(FIELD_TYPE_YEAR, 13).
-define(FIELD_TYPE_NEWDATE, 14).
-define(FIELD_TYPE_VARCHAR, 15).
-define(FIELD_TYPE_BIT, 16).
-define(FIELD_TYPE_NEWDECIMAL, 17).
-define(FIELD_TYPE_ENUM, 247).
-define(FIELD_TYPE_SET, 248).
-define(FIELD_TYPE_TINY_BLOB, 249).
-define(FIELD_TYPE_MEDIUM_BLOB, 250).
-define(FIELD_TYPE_LONG_BLOB, 251).
-define(FIELD_TYPE_BLOB, 252).
-define(FIELD_TYPE_VAR_STRING, 253).
-define(FIELD_TYPE_STRING, 254).
-define(FIELD_TYPE_GEOMETRY, 255).

-define(SET_FLAG, 2048).

-define(REFRESH_GRANT, 1).
-define(REFRESH_LOG, 2).
-define(REFRESH_TABLES, 4).
-define(REFRESH_HOSTS, 8).
-define(REFRESH_STATUS, 16).
-define(REFRESH_THREADS, 32).

-define(SHUTDOWN_DEFAULT, 0).
-define(SHUTDOWN_WAIT_CONNECTIONS, 1).
-define(SHUTDOWN_WAIT_TRANSACTIONS, 2).
-define(SHUTDOWN_WAIT_UPDATES, 8).
-define(SHUTDOWN_WAIT_ALL_BUFFERS, 16).
-define(SHUTDOWN_WAIT_CRITICAL_BUFFERS, 17).
-define(KILL_QUERY, 254).
-define(KILL_CONNECTION, 255).

-define(MYSQL_OPTION_MULTI_STATEMENTS_ON, 0).
-define(MYSQL_OPTION_MULTI_STATEMENTS_OFF, 0).
