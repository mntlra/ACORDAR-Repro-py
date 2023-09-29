# An Analysis of the ACORDAR Test Collection: Retrieve Datasets Content

This repository contains the code we used to build the [ACORDAR Test Collection](https://github.com/nju-websoft/ACORDAR/). The program comprises two steps: (1) download of the collection to access the content of the datasets; and, (2) parsing of the datasets to build the index.

## Repository Structure
- `output/` contains all the results produced by the code: the program's log files, results, stats, the download files, and the content of the datasets ready to be indexed. 
- `py/` contains the source code we developed to download and parse the datasets.
- `resources/`: contains useful resources to run the code, including the ACORDAR Test Collection.

## Before Starting

### Acquire the Resources
The program downloads and parses datasets starting from the corpus of ACORDAR provided by the authors in their GitHub repository. To run our code, first clone the ACORDAR repository inside the resources directory.

Go to the resources directory
```bash
  cd resources
```

Clone the ACORDAR Test Collection
```bash
  git clone https://github.com/nju-websoft/ACORDAR/
```

### Prepare the environment
We provide a `requirements.txt`file in the `py` directory with a list of all required libraires. To install the packages according to the requirements files:

Go to the py directory
```bash
  cd py
```

Install the required packages
```bash
  pip install -r requirements.txt
```

**Wget log files language:** The code exploit the Wget program to download the datasets and extract the download statistics anlyzing the Wget log file, which may be produced in the system's preferred language. These statistics are necessary for the correct execution of the parsing phase. Please make sure Wget log files are in English.


## Usage
Users can deploy the code using `main.py` and setting the following parameters:

- `---mode` [default value: `full`]: specifies the run modality. Indeed, it is possible to perform the full pipeline (download + parse) or execute a specific module. Possible values are: `full`to run the full pipeline; `download`to download the datasets; `parse`to parse the datasets. Note that to  perform only the parsing phase you must have already downloaded the datasets. 

- `--decompress` [default value: `True`]: specifies whether the compressed files must be decompressed during the parsing phase.

- `--startId` [default value: `1`]: specifies the identifier of the first dataset to consider.

- `--endId` [default value: `89218`]: specifies the identifier of the first dataset to consider.


### Example

To download and parse the complete ACORDAR Test Collection run:

```bash
python main.py --mode="full" --decompress=True --startId=1 --endId=89218
```

## Post-Processing Scripts
The `py`directory comprises a package called `post_processing`, which includes two scripts used to extract the empty datasets and provide the data fields in a formt ready to be indexed.
- `identify_empty_datasets.py` creates a dictionary containing the identifiers of all the datasets with no content information, i.e. all data fields are empty. The dictionary is stored in `output/results/`.
- `prepare_indexable_content.py` creates a dictionary for each datasets containing data field information ready to be indexed. In particular, for each field we create a single document where each item is separated by a space. The dictionaries are stored in `output/indexable_content`.