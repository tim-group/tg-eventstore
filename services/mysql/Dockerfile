FROM mysql:8.0

COPY create_local_user.sql /docker-entrypoint-initdb.d/02create_local_user.sql
COPY create_database.sql /docker-entrypoint-initdb.d/10create_database.sql
