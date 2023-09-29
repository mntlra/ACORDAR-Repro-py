import gzip
import os
import json
import csv
import tarfile
from zipfile import ZipFile


def load_datasets(input_path):
    """
    Load the test collection from the given filepath.

    :param input_path: (str) path to input file

    :return: (dict) dict of datasets entries where the key is the dataset_id.
    """

    with open(input_path) as fd:
        # strict set to False for parsing special characters (e.g., '\n')
        collection = json.load(fd, strict=False)
    new_datasets = {}
    for d in collection['datasets']:
        new_datasets[int(d["dataset_id"])] = d
    # return collection['datasets']
    return new_datasets


def save_stats(stats, filepath):
    """
    Saves the program's statistics in the given file.

    :param stats: (dict) dictionary comprising the program's statistics.
    :param filepath: (str) path to the file to write.

    :return: none.
    """
    with open(filepath, "w+") as fd:
        json.dump(stats, fd)


def save_content(content, filepath):
    """
    Saves the dataset's content in the given file.

    :param content: (dict) dictionary comprising the dataset's content.
    :param filepath: (str) path to the file to write.

    :return: none.
    """
    with open(filepath, "w+") as fd:
        json.dump(content, fd)


def save_output(rows, filepath):
    """
    Saves the program's results in the given file.

    :param rows: (list) list of rows composing the program's results.
    :param filepath: (str) path to the file to write.

    :return: none.
    """
    with open(filepath, 'w+') as fd:
        csvwriter = csv.writer(fd, delimiter=';')
        for r in rows:
            csvwriter.writerow(r)


def decompress_zip(logger, decompress, dirpath, filename, destination_folder, files_list):
    """
    Decompresses files with .zip extension.

    :param logger: (Logger.logger) the program's log.
    :param decompress: (bool) whether to decompress zipped files. Used to avoid duplicated files.
    :param dirpath: (str) path to the directory containing the compressed file.
    :param filename: (str) name of the compressed file.
    :param destination_folder: (str) path to the directory where we store the decompressed files.
    :param files_list: (list(str)) list of decompressed files.

    :returns: none
    """
    if decompress:
        with ZipFile(dirpath + filename, "r") as zObject:
            zObject.extractall(path=dirpath + destination_folder)
    logger.info(f'Extracted {len(os.listdir(dirpath + destination_folder))} files in directory: '
                f'{dirpath + destination_folder}.')
    for name in os.listdir(dirpath + destination_folder):
        # Add extracted files to list to be parsed
        files_list.append(destination_folder + '/' + name)


def decompress_tar(logger, dirpath, filename, decompress, destination_folder, files_list):
    """
    Decompresses files with .tar extension.

    :param logger: (Logger.logger) the program's log.
    :param dirpath: (str) path to the directory containing the compressed file.
    :param filename: (str) name of the compressed file.
    :param decompress: (bool) whether to decompress zipped files. Used to avoid duplicated files.
    :param destination_folder: (str) path to the directory where we store the decompressed files.
    :param files_list: (list(str)) list of decompressed files.

    :returns: none
    """
    # open file
    tar_extractor = tarfile.open(dirpath + filename)
    # print file names
    tmp = tar_extractor.getnames()
    logger.info(f'file content: {tmp}')
    if decompress:
        # extract files
        tar_extractor.extractall(destination_folder)
    logger.info(f'Extracted {len(tmp)} files in directory: {destination_folder}.')
    # close files
    tar_extractor.close()
    # Add extracted files to list to be parsed
    for name in tmp:
        files_list.append(filename.strip('.')[:-2] + '/' + name)


def decompress_gz(logger, dirpath, filename, decompress, new_file, files_list):
    """
    Decompresses files with .zip extension.

    :param logger: (Logger.logger) the program's log.
    :param dirpath: (str) path to the directory containing the compressed file.
    :param filename: (str) name of the compressed file.
    :param decompress: (bool) whether to decompress zipped files. Used to avoid duplicated files.
    :param new_file: (str) name of the decompressed file.
    :param files_list: (list(str)) list of decompressed files.

    :returns: none
    """
    if decompress:
        with gzip.open(dirpath + filename, 'rb') as fgzr:
            with open(dirpath + new_file, 'wb') as ofw:
                ofw.write(fgzr.read())
        logger.info(f'Decompressed file \'{new_file}\' in {dirpath}.')
        # Add extracted files to list to be parsed
        files_list.append(new_file)
    else:
        if new_file in files_list:
            logger.info(f'{new_file} already in the list')
        else:
            raise Exception('Decompress set to False but files are not decompressed!')