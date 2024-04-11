"""
Custom exceptions defined by the user
"""


class RegexNotFound(Exception):
    """
    A class for custom Regex exception
    """

    def __init__(self, value: str = ''):
        self.message = f"Searched regex expression was not found. Details:\n{value}"
        super().__init__(self.message)


class DockerDaemonOff(Exception):
    """
    A class for custom Docker Daemon Off exception
    """
    def __init__(self, value: str = ''):
        self.message = f"Docker daemon is probably not running or container name is invalid. Details:\n{value}"
        super().__init__(self.message)
