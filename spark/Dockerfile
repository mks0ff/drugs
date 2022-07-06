FROM bde2020/spark-python-template:3.3.0-hadoop3.3
	  
COPY etl_job.py /app/

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/etl_job.py
ENV SPARK_APPLICATION_ARGS "/spark/data/pubmed.csv" "/spark/data/clinical_trials.csv" "/spark/data/drugs.csv" "/tmp/Drugs"