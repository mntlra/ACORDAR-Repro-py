import json
from utils.utils import load_datasets
import os

DATA_PATH = "../resources/ACORDAR/Data/datasets.json"
datasets = load_datasets(DATA_PATH)
count = {}
for idx in datasets.keys():
    dirpath = "../output/downloads/dataset_" + str(idx) + "/"
    count[idx] = len(os.listdir(dirpath))

with open("../../output/results/download/count_downloaded_files.json", "w") as fd:
    json.dump(count, fd)
