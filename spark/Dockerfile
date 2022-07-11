FROM bde2020/spark-python-template:3.3.0-hadoop3.3
	  
COPY etl_job.py /app/

COPY dependencies/__init__.py /app/spark/dependencies/
COPY dependencies/logging.py /app/spark/dependencies/
COPY dependencies/spark.py /app/spark/dependencies/

COPY configs/etl_config.json /app/spark/configs/

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/etl_job.py
ENV SPARK_APPLICATION_ARGS "/spark/data/pubmed.csv" "/spark/data/clinical_trials.csv" "/spark/data/drugs.csv" "/tmp/Drugs"