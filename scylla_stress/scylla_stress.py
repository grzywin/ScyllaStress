#!/usr/bin/env python

"""
Cassandra stress runner
"""

import asyncio
import subprocess
import re
import json
import argparse
from datetime import datetime
import backoff

from logger import logger
from stats_calculator import StatsCalculator
from exceptions import RegexNotFound, DockerDaemonOff
from dict_exporter import DictExporter


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
        self.command = self._construct_basic_cassandra_stress_command(container_name)
        if extra_params_from_cassandra_log is not None:
            self.params_to_collect += extra_params_from_cassandra_log
        self.stdouts_from_cassandra = []

    async def run_cassandra_stress(self, command: str, cassandra_logs: bool) -> None:
        """
        Run single cassandra-stress command
        :param command: Content of command to be triggered
        :param cassandra_logs: Flag to tell if we want to show Cassandra logs in output and save it to log file or not
        :return None
        """
        start_time = datetime.now()
        # noinspection PyTypeChecker
        process = await asyncio.to_thread(subprocess.Popen, command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = await asyncio.to_thread(process.communicate)
        stdout_decoded, stderr_decoded = stdout.decode("utf-8"), stderr.decode("utf-8")
        if stderr_decoded:
            logger.warning(stderr_decoded)
        end_time = datetime.now()
        duration = end_time - start_time
        # [-4] To reduce milliseconds precision
        timing = {"start_time": start_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-4],
                  "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-4],
                  "duration": f"{round(duration.total_seconds(), 2)} sec"}
        # [-1] To retrieve only the "Results:" section from Cassandra logs instead of processing the entire output
        self.stdouts_from_cassandra.append({"stdout": stdout_decoded.split("Results:")[-1], "timing": timing})
        if cassandra_logs:
            logger.info(f"Command '{command}' executed with output:\n{stdout_decoded}")

    def _construct_basic_cassandra_stress_command(self, container_name: str) -> str:
        """
        Construct basic Cassandra stress command by taking container name from the constructor or getting ip address of
        the Cassandra node from nodetools command
        :param container_name: Name of the container
        :return Cassandra stress command
        """
        node_ip_address = self._check_container(container_name)
        return (f"docker exec {self.container_name} cassandra-stress write duration={{DURATION}} -rate threads=10 "
                f"-node {node_ip_address}")

    async def trigger_command(self, commands: list, cassandra_logs: bool = False) -> None:
        """
        Run cassandra-stress command asynchronously with asyncio library
        :param commands: Cassandra stress commands that will be executed
        :param cassandra_logs: Flag to tell if we want to show Cassandra logs in output and save it to log file or not
        :return None
        """
        await asyncio.gather(*(self.run_cassandra_stress(command, cassandra_logs) for command in commands))

    def compose_full_commands(self, args: argparse.Namespace) -> list:
        """
        Compose all Cassandra stress commands which will be triggered
        :param args: Arguments passed by user from argparse needed to compose Cassandra stress command
        :return
        """
        commands = []
        pattern = r"[0-9]+[smh]"
        if args.number_of_runs_and_duration:
            number_of_runs, duration = args.number_of_runs_and_duration
            if not number_of_runs.isnumeric():
                raise ValueError("Number of runs must be a positive integer")
            match = re.search(pattern, duration)
            if not match:
                raise RegexNotFound(f"Duration must match pattern: {pattern}, but it was {duration}")
            temp_command = self.command.replace("{DURATION}", duration)
            commands = [temp_command] * int(number_of_runs)
            logger.info(f"Executed command: {temp_command}, {number_of_runs} time(s)")
        elif args.durations:
            for duration in args.durations:
                match = re.search(pattern, duration)
                if not match:
                    raise RegexNotFound(f"Durations must match pattern: {pattern}, but one of them was {duration}")
                command = self.command.replace("{DURATION}", duration)
                commands.append(command)
            joined_commands = '\n'.join(commands)
            logger.info(f"Executed commands:\n{joined_commands}")
        return commands

    def _scrap_param_from_cassandra_logs(self, param_name: str) -> list:
        """
        Scraps parameter values from multiple Cassandra stress test command logs
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

    def generate_stats_summary(self, args: argparse.Namespace) -> dict:
        """
        Calculate all needed stats of Cassandra parallel stress runs
        :param args: Arguments passed by user from argparse needed to generate stats summary
        :return Dictionary with desired stats values
        """
        number_of_runs = len(args.durations) if args.durations else int(args.number_of_runs_and_duration[0])
        durations = args.durations if args.durations else [args.number_of_runs_and_duration[1]] * number_of_runs

        stats = {}
        for param in self.params_to_collect:
            stats[param] = self._scrap_param_from_cassandra_logs(param)
        stats["Stress processes durations"] = durations
        stats["Stress processes ran"] = number_of_runs
        stats["Op rates sum"] = StatsCalculator.calculate_sum(stats.get("Op rate"))
        stats["Average latency mean"] = StatsCalculator.calculate_average(stats.get("Latency mean"))
        stats["Average latency 99th percentile"] = (
            StatsCalculator.calculate_average(stats.get("Latency 99th percentile")))
        stats["Standard deviation latency max"] = (
            StatsCalculator.calculate_standard_deviation(stats.get("Latency max")))
        stats["Timings"] = {f"Stress command {index}": elem.get("timing")
                            for index, elem in enumerate(self.stdouts_from_cassandra, 1)}
        if args.export_to_json:
            DictExporter.export_dict_to_json_file(stats)
        return stats

    @backoff.on_predicate(backoff.constant, lambda x: x, max_time=150, interval=10)
    def _wait_for_cassandra_node_up(self) -> str:
        """
        Check and wait for Cassandra cqlsh to be up/available
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
        ip_pattern = r"\b(?:\d{1,3}\.){3}\d{1,3}\b"
        match = re.search(ip_pattern, node_status.stdout)
        if not match:
            raise RegexNotFound(ip_pattern)
        return match.group()

    def _check_container(self, container_name: str) -> str:
        """
        Check if docker container is started and ready for testing
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


def main() -> None:
    """
    Run Cassandra stress tests for ScyllaDB

    Parse command-line arguments to determine the number of parallel runs to execute and then runs Cassandra stress
    tests asynchronously using `CassandraStressRunner` class.
    It also collects, calculates and prints statistics summary based on the test results.
    """
    parser = argparse.ArgumentParser(description="Run Cassandra stress tests")
    parser.add_argument("--number-of-runs-and-duration", nargs="+", help="Number of parallel runs and to execute and "
                                                                         "their duration")
    parser.add_argument("--durations", nargs="+", help="Duration of each run")
    parser.add_argument("--cassandra-logs", action="store_true", help="Show detailed Cassandra logs values")
    parser.add_argument("--export-to-json", action="store_true", help="Export generated stats to json file")
    parser.add_argument("--container-name", default="some-scylla", help="Non-default container name")
    args = parser.parse_args()
    if not bool(args.number_of_runs_and_duration) ^ bool(args.durations):
        parser.error("Expected only ONE of two arguments (--number-of-runs-and-duration OR --durations)")

    cassandra_stress_runner = CassandraStressRunner(container_name=args.container_name)
    composed_commands = cassandra_stress_runner.compose_full_commands(args=args)
    asyncio.run(cassandra_stress_runner.trigger_command(commands=composed_commands,
                                                        cassandra_logs=args.cassandra_logs))

    stats_summary = cassandra_stress_runner.generate_stats_summary(args=args)
    logger.note(f"Stress tests statistics:\n{json.dumps(stats_summary, indent=4)}")
