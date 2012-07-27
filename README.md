JdbcPgBackup
============

A Java tool to backup and restore PostgreSQL databases using JDBC.

Usage:  
java jdbcpgbackup.JdbcPgBackup -m dump|restore [-h hostname] [-p port] [-t (timing)] 
[-d database] [-U user] [-P password] [-f filename] [-o (schema only)] 
[-s schema[,schema...]] [-n schema[,schema...]] [-b batchsize]

Options:  
-m mode, dump or restore, required;  
-h hostname, defaults to localhost;  
-p port, defaults to 5432;  
-t collect and show timing for each step and other debug info;  
-d database, defaults to the username if not supplied;  
-U username, defaults to postgres;  
-P password;  
-f filename, if absent defaults to stdin/stdout;  
-o do not dump data, schema definitions only;  
-s schemas to dump, comma separated list;  
-n schema names to restore to, if present must be of same length as the -s;  
-b batch size when doing a full dump, defaults to 10000 schemas in a batch.  


This application was developed to handle the backup of our PostgreSQL 
databases at Nabble, http://www.nabble.com , because the standard 
pg_dump backup tool (as of PostgreSQL 9.0.8) could not handle our special 
use case of having thousands of schemas per database. While pg_dump is 
likely to be fixed or at least improved in some future version, at 
present it is unusably slow for databases containing so many schemas 
(10-30k schemas in our case). Thus the need to create our own backup and 
restore tool.

JdbcPgBackup is implemented in Java and has no external dependencies 
other than a recent JDBC PostgreSQL driver (the version we currently use 
is postgresql-9.1-902.jdbc4.jar). It has been developed against a 
PostgreSQL 9.0.7 database and has not been tested with any older 
versions. Since it relies extensively on internal PostgreSQL 
implementation details, the system tables in the pg_catalog schema, it 
is not guaranteed to continue to work with future PostgreSQL versions as 
those are subject to change. The backup file it creates, however, uses 
standard SQL to restore all database objects, and is thus much more 
likely to be future compatible and possible to restore on a newer 
PostgreSQL version.

The reason JdbcPgBackup uses the pg_catalog schema so extensively is 
that the standard JDBC API for getting database metadata is 
unfortunately too slow to be useful, at least in our special use case of 
thousands of schemas. As a consequence, JdbcPgBackup is not designed to 
be used with other relational databases and is very PostgreSQL specific.

JdbcPgBackup uses the standard zip file format to store its backup. It 
should be noted that at least Java 1.7 is needed to handle backup files 
exceeding 4 GB, because such files require support for the ZIP64 
extensions, which is not present in Java 1.6 or older. Such Java 
versions will silently create a corrupted zip file when it exceeds 4 GB, 
so it is always important to test that the backup created can actually 
be read and restored with the version of Java you are using.

This standard zip file format is another advantage of JdbcPgBackup, as 
it can be manipulated with standard zip and filesystem tools, and if 
needed even modified, with schemas or tables moved or edited. The 
structure of the backup file is:

    pg_backup/  
    pg_backup/schemas.sql  
    pg_backup/schemas/  
    pg_backup/schemas/<schema1>/  
    pg_backup/schemas/<schema1>/indexes.sql  
    pg_backup/schemas/<schema1>/views.sql  
    pg_backup/schemas/<schema1>/constraints.sql  
    pg_backup/schemas/<schema1>/sequences.sql  
    pg_backup/schemas/<schema1>/tables.sql  
    pg_backup/schemas/<schema1>/tables/  
    pg_backup/schemas/<schema1>/tables/<table1>  
    pg_backup/schemas/<schema1>/tables/<table2>  
    ...  
    pg_backup/schemas/<schema2>/  
    ...  

where each *.sql file is a plain text file, containing the SQL DDL 
statements needed to create the corresponding database objects - tables, 
indexes, constraints, etc. The table data itself is stored under the 
tables/ directory under each schema, one file per table, as produced by 
the PostgreSQL COPY OUT command. Thus it should also be possible to even 
restore such a backup manually, by executing the *.sql scripts in the 
appropriate order and then reimporting the data from the table files 
using COPY IN.


JdbcPgBackup can be used to backup either a full database, or a set of 
one or a few schemas only. For performance reasons, the backup of a full 
database is NOT transactionally safe. It does extensive caching of 
database objects in Java, uses autocommit=true setting, and even has to 
close and reopen the connection between each batch of schemas (required 
to work around an out of memory error, most likely caused by a memory 
leak in postgresql or the jdbc driver). As a consequence, the backup of 
a full database is only guaranteed to produce a self-consistent snapshot 
of the data if the database is not modified during the backup. Since 
this is exactly our use case - we run the backup on a hot-standby 
replication instance with the replication suspended during the backup - 
such a limitation is perfectly acceptable to us. And it is a very 
common use case, to have the backup run on a replication instance rather 
than the production database, because of the extra load it causes. But 
if you do need to run a backup on a live production database, JdbcPgBackup 
is not the tool to use, you are stuck with pg_dump.

For backing up only one or a few schemas at a time, however, JdbcPgBackup 
performs a transactionally safe backup, by executing the whole backup in 
a single transaction, with transaction isolation level set to 
SERIALIZABLE. Thus, changes to the database performed by other concurrent 
transactions will not be reflected in such a backup. Such a partial 
backup can also be restored to other schemas or databases, thus 
providing a useful tool for moving a schema from one database to 
another.

JdbcPgBackup also has an option to backup only the database schema 
definition, without the table data itself, which is useful for 
recreating a completely empty schema with just the definitions of 
tables, indexes, etc.

JdbcPgBackup does not support the full range of database objects supported
by PostgreSQL, but only the most commonly used subset. Currently what is 
supported is: schemas, tables, indexes, views, sequences, constraints 
(those associated with a table only), and triggers (those defined as 
constraints only). In particular, JdbcPgBackup does not attempt to save and 
restore users and permissions, as those are shared across all databases in 
a cluster and a better tool to handle the backup of such global data is 
pg_dumpall. JdbcPgBackup does, however, save and restore the ownership of 
schemas and all supported database objects, but does not attempt to preserve 
any non-default permissions on their access. 
Other PostgreSQL features not currently supported include: user defined 
data types, table inheritance, tablespaces, descriptions, casts, enums, 
text search, foreign data, custom operators, stored procedures and languages.
Support for each of those may be added as needed, but as of now JdbcPgBackup 
supports all the PostgreSQL features that we actually use at Nabble, which 
should cover the most common use cases.

Finally, JdbcPgBackup does not support quoted SQL identifiers and special
characters in database object names (schema, table and column names, etc), 
because those are used directly as directory names or filenames in the backup 
file. Again, this covers the most common use case and practice.
