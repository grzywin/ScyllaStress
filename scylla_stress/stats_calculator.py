class StatsCalculator:
    """
    A class for basic statistical calculations
    """

    @staticmethod
    def calculate_average(values: list, unit: str = "ms") -> str:
        """
        Calculate average value from a list of values
        :param values: List of values
        :param unit: Unit of output value
        :return Average value
        """
        if values:
            return f"{round(sum(values) / len(values), 2)} {unit}"
        return "N/A"

    @staticmethod
    def calculate_standard_deviation(values: list, unit: str = "ms") -> str:
        """
        Calculate standard deviation value from a list of values
        :param values: List of values
        :param unit: Unit of output value
        :return Standard deviation value
        """
        if len(values) > 1:
            average = sum(values) / len(values)
            squared_diff = sum((x - average) ** 2 for x in values)
            return f"{round((squared_diff / len(values)) ** 0.5, 2)} {unit}"
        return 'N/A'

    @staticmethod
    def calculate_sum(values: list, unit: str = "op/s") -> str:
        """
        Calculate sum of values from a list of values
        :param values: List of values
        :param unit: Unit of output value
        :return Sum of values
        """
        if values:
            sum_of_values = sum(values)
            return f"{sum_of_values} {unit}"
        return 'N/A'
