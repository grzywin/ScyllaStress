#!/usr/bin/env python

import asyncio
import subprocess
import re
import os
import json
import argparse
import backoff
from datetime import datetime

from logger import logger
from stats_calculator import StatsCalculator
from exceptions import RegexNotFound, DockerDaemonOff


class CassandraStressRunner:
    """
    A class for running Cassandra stress tests for ScyllaDB
    """

    def __init__(self, container_name: str, extra_params_from_cassandra_log: list = None) -> None:
        """
        Initializes the CassandraStressRunner object.
        :param container_name: Name of the container
        :param extra_params_from_cassandra_log: In case we would like to get some more params from Cassandra logs we
        can add them here and script will try to collect them using regex
        """
        self.params_to_collect = ["Op rate", "Latency mean", "Latency 99th percentile", "Latency max"]
        self.container_name = container_name
        self.command = self.construct_cassandra_stress_command(container_name)
        if extra_params_from_cassandra_log is not None:
            self.params_to_collect += extra_params_from_cassandra_log
        self.stdouts_from_cassandra = []
        self.number_of_runs = None

    def construct_cassandra_stress_command(self, container_name: str) -> str:
        """
        Construct Cassandra stress command by taking container name from the constructor or getting ip address of the
        Cassandra node from nodetools command
        :param container_name: Name of the container
        :return Cassandra stress command
        """
        node_ip_address = self.check_container(container_name)
        return (f"docker exec {self.container_name} cassandra-stress write duration=10s -rate threads=10 "
                f"-node {node_ip_address}")

    async def run_cassandra_stress(self, command: str, show_cassandra_logs: bool) -> None:
        """
        Run single cassandra-stress command
        :param command: Content of command to be triggered
        :param show_cassandra_logs: Flag to tell if we want to show Cassandra logs in output or not
        :return None
        """
        start_time = datetime.now()
        # noinspection PyTypeChecker
        process = await asyncio.to_thread(subprocess.Popen, command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = await asyncio.to_thread(process.communicate)
        stdout_decoded, stderr_decoded = stdout.decode("utf-8"), stderr.decode("utf-8")
        if stderr:
            logger.warning(stderr)
        end_time = datetime.now()
        duration = end_time - start_time
        # [-4] To reduce milliseconds precision
        timing = {"start_time": start_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-4],
                  "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-4],
                  "duration": f"{round(duration.total_seconds(), 2)} sec"}
        # [-1] To retrieve only the "Results:" section from Cassandra logs instead of processing the entire output
        self.stdouts_from_cassandra.append({"stdout": stdout_decoded.split("Results:")[-1], "timing": timing})
        if show_cassandra_logs:
            logger.info(f"Command '{command}' executed with output:\n{stdout_decoded}")

    async def trigger_command(self, number_of_runs: str, show_cassandra_logs: bool = False) -> None:
        """
        Run cassandra-stress command asynchronously with asyncio library
        :param number_of_runs: How many times concurrently stress test command will be triggered
        :param show_cassandra_logs: Flag to tell if we want to show Cassandra logs in output or not
        :return None
        """
        self.number_of_runs = int(number_of_runs)
        logger.info(f"Executing command: {self.command}, {number_of_runs} time(s)")
        commands = [self.command] * self.number_of_runs
        await asyncio.gather(*(self.run_cassandra_stress(command, show_cassandra_logs) for command in commands))

    def get_param_from_cassandra_logs(self, param_name: str) -> list:
        """
        Gets parameter values from multiple Cassandra stress test command logs
        :param param_name: Parameter name for which we want to get value from Cassandra logs
        :return List of parameter values for each concurrent run
        """
        pattern = fr"{param_name}\s*:\s*([\d,.]+)"
        values = []
        for st in self.stdouts_from_cassandra:
            match = re.search(pattern, st.get("stdout"))
            if match:
                value = match.group(1).replace(",", "")
                values.append(float(value))
            else:
                logger.warning(f"Parameter '{param_name}' was not found in Cassandra stress test output")
        return values

    def generate_stats_summary(self, show_cassandra_stats: bool = False, export_json: bool = False) -> dict:
        """
        Calculate all needed stats of Cassandra parallel stress runs
        :param: show_collected_cassandra_stats: Adds to final stats also values collected from Cassandra logs
        :param: export_json:  Export stats to json
        :return Dictionary with desired values
        """
        gathered_stats, end_stats = dict(), dict()
        for param in self.params_to_collect:
            gathered_stats[param] = self.get_param_from_cassandra_logs(param)
        if show_cassandra_stats:
            end_stats.update(gathered_stats)
        end_stats["Stress processes ran"] = self.number_of_runs
        end_stats["Op rates sum"] = StatsCalculator.calculate_sum(gathered_stats.get('Op rate'))
        end_stats["Average latency mean"] = StatsCalculator.calculate_average(gathered_stats['Latency mean'])
        end_stats["Average latency 99th percentile"] = (
            StatsCalculator.calculate_average(gathered_stats['Latency 99th percentile']))
        end_stats["Standard deviation latency max"] = (
            StatsCalculator.calculate_standard_deviation(gathered_stats['Latency max']))
        end_stats["Timings"] = {f"Stress command {index}": elem.get("timing")
                                for index, elem in enumerate(self.stdouts_from_cassandra, 1)}
        if export_json:
            CassandraStressRunner.export_json(end_stats)
        return end_stats

    @backoff.on_predicate(backoff.constant, lambda x: x, max_time=120, interval=10)
    def _wait_for_cassandra_node_up(self) -> str:
        """
        Check and wait for Cassandra to be up
        :return Stderr of triggered subprocess command
        """
        command = f"docker exec {self.container_name} cqlsh"
        logger.info(f"Waiting for Cassandra to be up and running with command: {command}")
        return subprocess.run(command, capture_output=True, text=True).stderr

    def _get_ip(self) -> str:
        """
        Get an IP address from nodetool status
        :return Node IP address in form of a string
        """
        logger.info("Getting node IP from nodetool status")
        node_status = subprocess.run(f"docker exec {self.container_name} nodetool status", capture_output=True,
                                     text=True)
        ip_pattern = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
        match = re.search(ip_pattern, node_status.stdout)
        if not match:
            raise RegexNotFound(ip_pattern)
        return match.group()

    def check_container(self, container_name: str) -> str:
        """
        Check if docker container is ready for testing
        :param container_name: Name of the container
        :return Cassandra node IP address in form of a string
        """
        running_containers = subprocess.run("docker ps", capture_output=True, text=True)
        if container_name not in running_containers.stdout:
            run_docker_start = subprocess.run(f"docker start {container_name}", capture_output=True, text=True)
            if run_docker_start.stderr:
                raise DockerDaemonOff(run_docker_start.stderr)
        self._wait_for_cassandra_node_up()
        return self._get_ip()

    @staticmethod
    def export_json(stats: dict) -> None:
        json_string = json.dumps(stats, indent=4)
        relative_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        results_file = os.path.join(relative_path, "results",
                                    f"scylla_stats_{datetime.now().strftime('%H_%M_%S_%y_%m_%d')}.json")
        with open(results_file, 'w') as file:
            file.write(json_string)


def main() -> None:
    """
    Run Cassandra stress tests for ScyllaDB

    Parse command-line arguments to determine the number of parallel runs to execute and then runs Cassandra stress
    tests asynchronously using `CassandraStressRunner` class.
    It also collects and calculates statistics summary based on the test results.
    """
    parser = argparse.ArgumentParser(description="Run Cassandra stress test")
    parser.add_argument("--number_of_runs", required=True, help="Number of parallel runs to execute")
    parser.add_argument("--show_cassandra_logs", action="store_true", help="Show detailed Cassandra logs values")
    parser.add_argument("--export_json", action="store_true", help="Export generated stats to json file")
    parser.add_argument("--container_name", required=False, default='some-scylla', help="Non-default container name")
    args = parser.parse_args()

    cassandra_stress_runner = CassandraStressRunner(args.container_name)
    asyncio.run(cassandra_stress_runner.trigger_command(args.number_of_runs, args.show_cassandra_logs))
    stats_summary = cassandra_stress_runner.generate_stats_summary(show_cassandra_stats=args.show_cassandra_logs,
                                                                   export_json=args.export_json)

    logger.note(f"Stress tests statistics:\n{json.dumps(stats_summary, indent=4)}")
