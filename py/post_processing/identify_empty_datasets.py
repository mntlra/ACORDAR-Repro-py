from utils.utils import load_datasets
import json
from Logger import Logger

"""
This script creates a dictionary containing the identifiers of all the datasets with no content information, 
i.e. all data fields are empty.
"""

DATA_PATH = "../resources/ACORDAR/Data/datasets.json"
datasets = load_datasets(DATA_PATH)
# Logger
Logger = Logger("../output/logs/empty_datasets.log")
output_file = "../../output/results/empty_datasets.json"
empty_datasets = {}

for idx in datasets.keys():
    Logger.logger.info(f'*** Beginning post-processing of dataset: {idx} ***')
    input_file = "../output/content/dataset_" + str(idx) + ".json"
    with open(input_file, 'r') as fid:
        content = json.load(fid)
        content_fields = ['classes', 'properties', 'entities', 'literals']
        if all(len(content[field]) == 0 for field in content_fields):
            Logger.logger.info(f'*** Dataset {idx} has no content available ***')
            empty_datasets[idx] = 1


Logger.logger.info('--- Saving ids of empty_datasets ---')
with open(output_file, 'w') as fod:
    json.dump(empty_datasets, fod)



