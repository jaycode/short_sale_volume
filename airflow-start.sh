export AIRFLOW_HOME=$(pwd)/airflow
airflow initdb
x-terminal-emulator -hold -e airflow scheduler
airflow webserver -p 8080