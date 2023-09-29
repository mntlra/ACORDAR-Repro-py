from utils.utils import load_datasets
import os
import csv


DATA_PATH = "../resources/ACORDAR/Data/datasets.json"
datasets = load_datasets(DATA_PATH)
rows = [['dataset_id', 'file', 'dimensions']]
for idx in datasets.keys():
    dirpath = "../output/downloads/dataset_" + str(idx) + "/"
    for file in os.listdir(dirpath):
        if file == '.DS_Store':
            continue
        rows.append([idx, file, os.path.getsize(dirpath+file)/1048576])

with open("../../output/results/download/files_dimension.csv", 'w') as fd:
    csvwriter = csv.writer(fd, delimiter=';')
    for r in rows:
        csvwriter.writerow(r)
