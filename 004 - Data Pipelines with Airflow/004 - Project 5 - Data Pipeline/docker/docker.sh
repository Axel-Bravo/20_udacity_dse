docker run -d -p 8080:8080 -v "$(pwd)"/dags:/usr/local/airflow/dags -v "$(pwd)"/requirements.txt:/requirements.txt \
-v "$(pwd)"/plugins:/usr/local/airflow/plugins --name airflow_solo puckel/docker-airflow webserver
