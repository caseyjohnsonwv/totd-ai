FROM postgres:13
ADD sql/*.sql /docker-entrypoint-initdb.d/
