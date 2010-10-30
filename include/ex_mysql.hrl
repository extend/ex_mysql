-define(ex_mysql_PORT, 3306).

-record(ex_mysql_ok, {rows, insert_id, message}).
-record(ex_mysql_field, {table, name, type, length}).
-record(ex_mysql_stmt, {params, columns}).
