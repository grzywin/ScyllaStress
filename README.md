ScyllaDB Job Interview
======================

Preconditions
------------
1. Make sure that your Docker Agent/Daemon is always running
2. Download ScyllaDB docker image `docker pull scylladb/scylla`
3. Start scylla server instance `docker run --name some-scylla --hostname some-scylla -d scylladb/scylla`.

Installation and Running
------------
1. Download or clone repository
2. Navigate to the directory where you downloaded the repository (main catalog)
3. Install required packages by typing following command `pip install -r requirements.txt`
4. Run the program by typing `python scylla_stress --number-of-runs-and-duration X Y` where X is the number of 
concurrent Cassandra stress commands for ScyllaDB and Y is their duration in `[0-9]+[smh]` format, e.g. 5 10s
5. Alternatively the program can be run by typing `python scylla_stress --durations X`, where X is stands for durations 
of each Cassandra stress command in `[0-9]+[smh]` format, e.g. `1s 2s 10s 5s 1m`

Extra Features
--------------
1. There are some extra arguments which can be passed to scylla_stress:
   - export-to-json - will save stress results into /results folder in main project folder
   - cassandra-logs - will show full output from Cassandra stress runs in console and also log them into log file
   - container-name - will allow you to type non-default container name (other than '*some-scylla*')
2. Run logs are saved after each run in  /logs folder in main project folder.
3. Sample commands with all available arguments:

`python scylla_stress --number-of-runs-and-duration 5 10s --export-to-json --cassandra-logs --container-name some-scylla`

`python scylla_stress --durations 1s 2s 1m 10s --export-to-json --cassandra-logs --container-name some-scylla`
