## Running the application on a Spark standalone cluster via Docker

To run the application, execute the following steps:

1. Build the Docker image:
    ```bash
    cd spark 
    bash build.sh drugs .
    ```
2. Setup a Spark cluster by just running: 
    ```bash
    cd docker
    docker-compose -f docker-compose-spark.yml up
    ```
3. Stop the Spark cluster by just running: 
    ```bash
    cd docker
    docker-compose -f docker-compose-spark.yml down
    ```
    go to step 1 if updated the code.
