import os
import datetime
import gc
import json
import pandas as pd
import rdflib
from rdflib.exceptions import ParserError
from Logger.Logger import Logger
from utils.utils import save_stats, save_output, save_content, decompress_zip, decompress_tar, decompress_gz
from .contentExtraction import extract_data


class ParseRDFlib:
    """
    A class used to parse downloaded datasets using rdflib library.
    """

    def __init__(self):
        """
        Initialization Function
        """
        self.output_path = '../output/'
        # Logger
        self.logger = Logger().logger
        # Mapping for downloaded files for each dataset
        self.datasets_content = {}
        self.file_content = {}
        self.stats = {}
        self.rows = [['dataset_id', 'filename', 'file', 'filetype', 'code', 'type',
                      'classes', 'properties', 'entities', 'literals']]
        self.parsed_files = 0
        self.exception_files = 0
        self.filetype_classification = {"[application/rdf\\+xml]": "xml", "[application/rdf+xml]": "xml"}
        self.compressed_formats = ['[application/gzip]', '[application/x-gzip]', '[application/zip]', '[zip]']
        self.invalid_types = ['[text/csv]', '[text/html]', '[text/plain]']
        # Load download information to access each downloaded file's type
        self.datasets_info = pd.read_csv(self.output_path+"results/download/download_infos_FULL.csv")

    def restrict_infos(self, start_id, end_id):
        """

        """
        restricted_df = self.datasets_info[self.datasets_info["dataset_id"].isin(range(start_id, end_id+1))]
        self.datasets_info = restricted_df

    def handle_compressed_files(self, d_id, filename, dirpath, decompress, files_list):
        """
        Handles compressed files and, if necessary, decompress them accordingly.

        :param d_id: (int) id of the considered dataset.
        :param filename: (str) name of the considered file.
        :param dirpath: (str) directory containing the considered file.
        :param decompress: (bool) whether to decompress zipped files.
        :param files_list: list(str) current list of files to parse.

        :return: none.
        """
        # Extract the file's type from download information
        filetype_df = self.datasets_info.loc[((self.datasets_info['dataset_id'] == d_id) &
                                              (self.datasets_info['filename'] == filename))]['filetype']
        # Check whether the compressed file is in datasets_info
        if not filetype_df.empty:
            filetype = filetype_df.iloc[0]
            if filetype in self.compressed_formats:
                # the considered file is compressed, check whether file has already been decompressed.
                if decompress:
                    self.logger.info(f'{filename} is a compressed file, extracting files...')
                else:
                    self.logger.info(f'{filename} is a compressed file, updating list accordingly...')
                # Remove compressed file from list of files to parse
                files_list.remove(filename)
                if filetype == '[application/zip]':
                    # Decompressing zip file
                    self.logger.info(f'...Decompressing .zip file')
                    destination_folder = filename.rstrip('.zip')
                    decompress_zip(self.logger, decompress, dirpath, filename, destination_folder, files_list)
                else:
                    if filename.split('.')[-2] == 'tar':
                        # Decompressing tar file
                        # set destination directory
                        destination_folder = dirpath + filename.strip('.')[:-2]
                        self.logger.info(f'...Decompressing .tar.gz file')
                        decompress_tar(self.logger, dirpath, filename, decompress, destination_folder, files_list)
                    else:
                        # Decompressing gz file
                        self.logger.info(f'...Decompressing .gz file')
                        new_file = filename.rstrip('.gz')
                        decompress_gz(self.logger, dirpath, filename, decompress, new_file, files_list)

    def preprocess_download_directory(self, d_id, dirpath, files_list, decompress):
        """
        Preprocesses the directory containing the downloaded files rto remove unrelated files and, if necessary,
        decompress zipped files.

        :param d_id: (int) id of the considered dataset.
        :param dirpath: (str) path to the directory containing the downloaded files.
        :param files_list: (list(str)) list of all files containing in the directory.
        :param decompress: (bool) whether to decompress the zipped files.

        :return: (list(str)) list of files to parse.
        """
        if '.DS_Store' in files_list:
            # remove unrelated file to the parsing list
            files_list.remove('.DS_Store')
        original_list = files_list.copy()
        # Handling compressed files
        for filename in original_list:
            self.handle_compressed_files(d_id, filename, dirpath, decompress, files_list)
        return files_list

    def check_file_compliance(self, d_id, filename):
        """
        Checks if the file's format is suitable for parsing.

        :param self: self object.
        :param d_id: (int) id of the considered dataset.
        :param filename: (str) name of the considered file.

        :return: (bool, str) whether the format is suitable for parsing, file format.
        """
        # Extract the file format from download information
        filetype_df = self.datasets_info.loc[((self.datasets_info['dataset_id'] == d_id) &
                                              (self.datasets_info['filename'] == filename))]['filetype']
        if filetype_df.empty:
            # unknown format
            return True, 'unknown'
        else:
            filetype = filetype_df.iloc[0]
            if filetype in self.invalid_types:
                # not rdf-like files are discarded
                return False, filetype
            else:
                return True, filetype

    def rename_weird_files(self, dataset_id, file):
        """
        Renames files if their name follows a weird format (ends with \'?accessType=DOWNLOAD\', or ends with a digit
        after the extension). This allos RDFLib to infer the format of the file using its extension.

        :param dataset_id: (int) id of the considered dataset
        :param file: (stR) name of the considered file.

        :return name of the considered file.
        """
        filepath = self.output_path + "downloads/dataset_" + str(dataset_id) + "/"
        if file.endswith('?accessType=DOWNLOAD'):
            # rename the file by removing ?accessType=DOWNLOAD at the end
            new_name = file.rstrip('?accessType=DOWNLOAD')
            os.rename(filepath+file, filepath+new_name)
            return new_name
        if file.split('.')[-1].isdigit() and len(file.split('.')) > 1:
            # file has a digit after its extension, rename the file by moving the digit before the extension
            digit = file.split('.')[-1]
            if len(file.split('.')) > 2:
                extension = file.split('.')[-2]
                new_name = file.rstrip(extension+'.'+digit)+'_'+digit+'.'+extension
            else:
                new_name = file.rstrip('.'+digit) + '_' + digit
            os.rename(filepath + file, filepath + new_name)
            return new_name
        return file

    def parse_file(self, dataset_id, filename, file, filetype):
        """
        Parse the file using RDFlib and, if parsing is successful, extract the data fields.

        :param self: self object.
        :param dataset_id: (int) id of the considered dataset.
        :param filename: (str) incremental name of the file to parse.
        :param file: (str) actual name of the file to parse.
        :param filetype: (str) the considered file's format.

        :return: none.
        """
        # initialize empty graph
        g = rdflib.Graph()
        filepath = self.output_path + "downloads/dataset_" + str(dataset_id) + "/"
        correctly_parsed = False
        # dictionary containing data fields' statistics
        count = {}
        try:
            g.parse(filepath+file)
            self.logger.info(f'Correctly parsed')
            self.parsed_files += 1
            correctly_parsed = True
        except ParserError as parsing_error:
            if str(parsing_error).startswith('Could not guess RDF format'):
                # try parsing the file with xml format
                try:
                    g.parse(filepath + file, format="xml")
                    self.logger.info(f'Correctly parsed')
                    self.parsed_files += 1
                    correctly_parsed = True
                except Exception as e_2:
                    self.logger.info(f'Exception occurred, {type(e_2)}: {str(e_2)}')
                    self.exception_files += 1
                    # rows follow the format:
                    # ['dataset_id', 'filename', 'file', 'filetype', 'code', 'type',
                    # 'classes', 'properties', 'entities', 'literals']
                    self.rows.append([dataset_id, filename, file, filetype, type(e_2), str(e_2), 0, 0, 0, 0])
            else:
                self.logger.info(f'Exception occurred, {type(parsing_error)}: {str(parsing_error)}')
                self.exception_files += 1
                # rows follow the format:
                # ['dataset_id', 'filename', 'file', 'filetype', 'code', 'type',
                # 'classes', 'properties', 'entities', 'literals']
                self.rows.append(
                    [dataset_id, filename, file, filetype, type(parsing_error), str(parsing_error), 0, 0, 0, 0])
        except Exception as e:
            self.logger.info(f'Exception occurred, {type(e)}: {str(e)}')
            self.exception_files += 1
            # rows follow the format:
            # ['dataset_id', 'filename', 'file', 'filetype', 'code', 'type',
            # 'classes', 'properties', 'entities', 'literals']
            self.rows.append([dataset_id, filename, file, filetype, type(e), str(e), 0, 0, 0, 0])
        if correctly_parsed:
            # if parsing was successful we extract the data fields
            for content_type in self.datasets_content.keys():
                data, count[content_type] = extract_data(g, content_type, 100000)
                self.file_content[content_type] = data
                self.datasets_content[content_type].extend(data)
            # rows follow the format:
            # ['dataset_id', 'filename', 'file', 'filetype', 'code', 'type',
            # 'classes', 'properties', 'entities', 'literals']
            self.rows.append([dataset_id, filename, file, filetype, 200, "OK",
                              count['classes'], count['properties'], count['entities'], count['literals']])
            # save file's content
            with open(self.output_path + 'files_level_content/dataset_' + str(dataset_id) + '/' + filename + '.json', "w") as fd:
                json.dump(self.file_content, fd)
            # free memory space
            del g
            gc.collect()

    def parse_datasets(self, datasets, start_id, end_id, signature, decompress):
        """
        Parse downloaded datasets using rdflib library.

        :param datasets: (dict) input datasets.
        :param start_id: (int) id of the first dataset to parse.
        :param end_id: (int) id of the last dataset to parse.
        :param signature: (str) signature string to differentiate runs.
        :param decompress: (bool) whether to decompress zipped files.

        :return: none.
        """
        self.logger.info(f'--- START OF RDFLIB PARSING --- run {datetime.datetime.now()} ---\n')
        self.logger.info(f"Parsing dataset from id {start_id} to {end_id}")
        if not decompress:
            self.logger.info(f'--- Compressed files have already been extracted ---')
        self.restrict_infos(start_id, end_id)

        for idx in range(start_id, end_id + 1):
            if idx in datasets.keys():
                # Initialize parser counters
                count_files = 1
                self.parsed_files = 0
                self.exception_files = 0
                self.datasets_content = {"classes": [], "properties": [], "entities": [], "literals": []}
                # Create directory for dataset's files
                dirname = self.output_path + 'files_level_content/dataset_' + str(idx) + '/'
                os.makedirs(dirname, exist_ok=True)
                dirpath = self.output_path + 'downloads/dataset_' + str(idx) + '/'
                self.logger.info(f'--- Beginning parsing of dataset: {str(idx)} ---')
                files_list = self.preprocess_download_directory(idx, dirpath, os.listdir(dirpath), decompress)
                self.logger.info(f'Number of files available --- {len(files_list)}')

                for file in files_list:
                    if count_files < 100:
                        self.file_content = {}
                        filename = 'file_' + str(count_files)
                        count_files += 1
                        self.logger.info(f'Trying to parse file: {file}')
                        compliance, filetype = self.check_file_compliance(idx, file)
                        if compliance:
                            self.logger.info(f'Parsing file {file} {filetype} (({os.path.getsize(dirpath+file)/1048576} MB))')
                            # check file's dimension
                            if os.path.getsize(dirpath+file)/1048576 < 150.0:
                                file = self.rename_weird_files(idx, file)
                                self.parse_file(idx, filename, file, filetype)
                            else:
                                self.exception_files += 1
                                self.logger.info(f'File too large --- ')
                                # rows follow the format:
                                # ['dataset_id', 'filename', 'file', 'filetype', 'code', 'type']
                                self.rows.append([idx, filename, file, filetype, '--', 'too_large'])
                        else:
                            self.logger.info(f'Unsuitable format --- {filetype}')
                            self.exception_files += 1
                            # rows follow the format:
                            # ['dataset_id', 'filename', 'file', 'filetype', 'code', 'type']
                            self.rows.append([idx, filename, file, filetype, '--', 'format'])
                    else:
                        # stop after parsing 100 files
                        break

                self.logger.info(f'Dataset {idx}: Number of parsed files {self.parsed_files}, '
                                 f'Number of broken files {self.exception_files}')

                self.stats[idx] = {'number_of_links': len(files_list), 'parsed_files': self.parsed_files,
                                   'exception_files': self.exception_files}

                self.logger.info(f'--- End of parsing of dataset: {idx} ---')
                # Saving the parsing statistics and information in two separate files
                save_stats(self.stats, self.output_path + "stats/parse/parsing_stats_" + signature + ".json")
                save_output(self.rows, self.output_path + "results/parse/parsing_infos_" + signature + ".csv")
                # Saving the dataset's content
                save_content(self.datasets_content, self.output_path + "content/dataset_" + str(idx) + ".json")

        self.logger.info(f'\n--- END OF RDFLIB PARSING --- run {datetime.datetime.now()} ---')

        return None
