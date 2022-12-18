Quick demonstration of various operators and multiple Apache Airflow features.

In order to start the application run command 'docker composer up' in the main folder. Once all services will start & will be running try to visit http://localhost:8080. The dag which is interesting to us will be called Main_DAG try to run it to test the application.

The for_file_sensor should have a file called 'run' (no extension) in order to allow Main_DAG to finish successfully.