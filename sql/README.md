## Running SQL queries on PostgresSQL via Docker

To run the queries, execute the following steps:

1. Setup a PostgresSQL Server by just running: 
    ```bash
    cd docker
    docker-compose -f docker-compose-postgres.yml up
    ```
2. Stop the PostgresSQL Server by just running: 
    ```bash
    cd docker
    docker-compose -f docker-compose-postgres.yml down
    ```
