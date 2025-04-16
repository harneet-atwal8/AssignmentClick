# Bidirectional-ClickHouse-Flat-File-Data-Ingestion-Tool
This project provides a web-based tool for bidirectional data ingestion between ClickHouse databases and flat files (CSV). It allows users to:
Ingest data from CSV files into ClickHouse tables.
Export data from ClickHouse tables (with optional joins) to CSV files.
Preview data before ingestion or export.
The tool features a React-based frontend and a Spring Boot backend, with ClickHouse as the database, all containerized using Docker Compose.
Features
Source Selection: Choose between ClickHouse or flat file (CSV) as the data source.
Table/Column Management:

Load tables from ClickHouse or columns from uploaded CSVs.

Select specific columns for ingestion or export.
to Start this project foolow these steps:- 
1) Start your docker desktop
2) run this command:- docker run -d -p 8123:8123 -p 9000:9000 --name clickhouse-server yandex/clickhouse-server
->> above will run clickhouse in detached mode
3) cd backend
4) mvn clean package
5) cd ..
6) docker-compose down
7) docker-compose up --build 
the above command will start your backend
8) go to google and type: - http://localhost:80 your website will start
to close backend press ctrl+c 3 times

->>  before using clickhouse service add simple csv files provided in uploads which can help you make tables
->> for noe join condition has some problem rest everything is working

---------------------------------------------
to check if tables are being created on your clickhouse 
write thise command:-
1) docker-compose up --build -d
2) docker exec -it clickhouse clickhouse-client
->>all your tables will be formed in default database
