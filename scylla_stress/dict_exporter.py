import json
import os
from datetime import datetime


class DictExporter:
    """
    A class for exporting dictionary data to JSON file
    """

    @staticmethod
    def export_dict_to_json_file(stats: dict) -> None:
        json_string = json.dumps(stats, indent=4)
        relative_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        results_file = os.path.join(relative_path, "results",
                                    f"scylla_stats_{datetime.now().strftime('%H_%M_%S_%y_%m_%d')}.json")
        with open(results_file, 'w') as file:
            file.write(json_string)
