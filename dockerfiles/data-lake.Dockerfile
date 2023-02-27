FROM postgres:13
ADD sql/create-tables.sql /docker-entrypoint-initdb.d/
