ScyllaDB Job Interview
======================

Preconditions
------------
1. Make sure that your Docker Agent/Daemon is running
2. Download ScyllaDB docker image `docker pull scylladb/scylla`
3. Start scylla server instance `docker run --name some-scylla --hostname some-scylla -d scylladb/scylla`.

Installation and Running
------------
1. Download or clone repository
2. Navigate to the directory where you downloaded the repository (main catalog)
3. Install required packages by typing following command `pip install -r requirements.txt`
4. **Run the program by typing `python scylla_stress --number_of_runs X` where X is the number of concurrent Cassandra 
stress commands for ScyllaDB**

Extra Features
--------------
1. There are some extra arguments which can be passed to scylla_stress:
   - export_json - will save stress results into /results folder in main project folder
   - show_cassandra_logs - will show full output from Cassandra stress runs
   - container_name - will allow you to type non-default container name (other than '*some-scylla*')
2. Run logs are saved after each run in  /logs folder in main project folder.
