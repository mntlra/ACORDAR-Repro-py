import json
import time
from download.TestDownloadWget import TestDownloadWget
from parse.ParseRDFlib import ParseRDFlib
from utils.utils import load_datasets
from Logger import Logger
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--mode', default='full', type=str, help='Whether to perform the full pipeline or a specific module.')
parser.add_argument('--signature', default='', type=str, help='download_infos file signature. used only if mode is \'parse\'')
parser.add_argument('--decompress', default=True, type=bool, help='Whether to decompress zipped files during parsing.')
parser.add_argument('--startId', default=1, type=int, help='First dataset id to process.')
parser.add_argument('--endId', default=89218, type=int, help='Last dataset id to process.')
args = parser.parse_args()

# Initialize useful variables
DATA_PATH = "../resources/ACORDAR/Data/datasets.json"
# Start and end offset
start_id = args.startId
end_id = args.endId
# Get current timestamp
timestr = time.strftime("%y%m%d%H%M")
signature = str(start_id) + "_" + str(end_id) + "_" + timestr

# Logger for full run
Logger = Logger("../output/logs/"+args.mode+"/main_" + signature + ".log")


def main():

    datasets = load_datasets(DATA_PATH)

    if args.mode == 'download' or args.mode == 'full':

        downloaded_dataset = TestDownloadWget().test_download_links(datasets, start_id, end_id, signature)

        # Save the downloaded files mapping
        with open('../output/results/download/downloaded_dataset_' + signature + '.json', 'w+') as fd:
            json.dump(downloaded_dataset, fd)

    if args.mode == 'parse' or args.mode == 'full':
        info_signature = args.signature if args.mode == "parse" else signature
        ParseRDFlib(info_signature).parse_datasets(datasets, start_id, end_id, signature, args.decompress)


if __name__ == "__main__":
    main()
