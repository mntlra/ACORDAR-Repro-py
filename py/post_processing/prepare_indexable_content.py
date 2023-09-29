from utils.utils import load_datasets
import json
from Logger import Logger
"""
This script creates a dictionary containing data field information ready to be indexed. In particular, for each field 
we create a single document where each item is separated by a space.
"""

DATA_PATH = "../resources/ACORDAR/Data/datasets.json"
datasets = load_datasets(DATA_PATH)
# Logger
Logger = Logger("../output/logs/create_indexable_content.log")

for idx in datasets.keys():
    Logger.logger.info(f'*** Beginning post-processing of dataset: {idx} ***')
    input_file = "../output/content/dataset_" + str(idx) + ".json"
    output_file = "../output/indexable_content/dataset_" + str(idx) + ".json"
    indexable_content = {}
    with open(input_file, 'r') as fid:
        content = json.load(fid)
        indexable_content['classes'] = ' '.join(content['classes'])
        indexable_content['properties'] = ' '.join(content['properties'])
        indexable_content['entities'] = ' '.join(content['entities'])
        indexable_content['literals'] = ' '.join(content['literals'])

    Logger.logger.info('--- Saving indexable content ---')
    with open(output_file, 'w') as fod:
        json.dump(indexable_content, fod)

    Logger.logger.info(f'*** Finished post-processing of dataset: {idx} ***')

