import os
import subprocess
import datetime
from Logger.Logger import Logger
from utils.utils import save_stats, save_output


class TestDownloadWget:
    """
    A class used to download datasets from links using wget API.
    """

    def __init__(self):
        """
        Initialization Function
        """
        self.output_path = '../output/'
        # Logger
        self.logger = Logger().logger
        # Mapping for downloaded files for each dataset
        self.downloaded_files = {}
        self.downloaded_dataset = {}
        self.stats = {}
        self.rows = [['dataset_id', 'linkname', 'link', 'code', 'type', 'filename', 'filetype']]
        self.downloaded_links = 0
        self.exception_links = 0
        self.accepted_extensions = ['rdf', 'xml', 'owl', 'n3', 'nt', 'nq', 'ttl', 'trig', 'trix', 'json', 'jsonld',
                                    'gz', 'bz2', 'zip', 'tgz', 'rdfs', 'tar', 'ntriples']
        self.compressed_extensions = ['gz', 'bz2', 'zip', 'tgz', 'tar']
        self.invalid_extensions = ['html', 'png']

    def check_file_compliance(self, link):
        """
        Check the file extension of a given URL and discard textual documents or images.

        :param link: (str) URL of the file to download.

        :return: (bool) whether the URL points to a file with a valid extension or not.
        """
        filename = link.split('/')[-1]
        if link.split('/')[-1].endswith('?accessType=DOWNLOAD'):
            filename = filename.rstrip('?accessType=DOWNLOAD')
        extension = filename.split('.')[-1]
        # download only rdf-like files
        if extension in self.invalid_extensions:
            return False
        else:
            return True

    def save_correct_download(self, dataset_id, link, linkname, lines):
        """
        Updates the download statistics when the file is successfully downloaded.

        :param dataset_id: (int) id of the dataset which comprises the considered URL.
        :param link: (str) download URL.
        :param linkname: (str) incremental link name used for debugging and statistics purposes.
        :param lines: (list(str)) list of tokens containing the path and the name of the downloaded file.

        :return: none.
        """
        # extract the type of file downloaded by the Wget program
        file_type = lines[1].split()[-1]
        # extract the name of the downloaded file
        filename = lines[2].split()[-1].replace(self.output_path + 'downloads/dataset_'+str(dataset_id)+'/', "")
        self.logger.info(f"{lines[2]}")
        # update download statistics accordingly
        self.rows.append([dataset_id, linkname, link, 200, "OK", filename, file_type])
        self.downloaded_dataset[dataset_id][link] = filename

    def save_exception_download(self, dataset_id, link, linkname, toks):
        """
        Updates the download statistics when the download procress raises an Exception.

        :param dataset_id: (int) id of the dataset which comprises the considered URL.
        :param link: (str) download URL.
        :param linkname: (str) incremental link name used for debugging and statistics purposes.
        :param toks: (list(str)) list of tokens containing the error code and the type of error.

        :return: none.
        """
        # extract the error code and type
        err_code = toks[0]
        if toks[0] == "other":
            err_type = ' '.join(toks[1:])
        else:
            err_type = ' '.join(toks)
        self.logger.info(f"{err_code}: {err_type}")
        # update download statistics accordingly
        self.rows.append([dataset_id, linkname, link, err_code, err_type, "--", "--"])
        self.downloaded_dataset[dataset_id][link] = '--'

    def analyze_output(self, prc_returncode, logfile, dataset_id, link, linkname):
        """
        Analyzes the output of the Wget program and saves statics and information about the download.

        :param prc_returncode: (int) whether the program finished without errors (code 0) or with an exception.
        :param logfile: (str) path to the log file storing Wget output.
        :param dataset_id: (int) id of the dataset which comprises the considered URL.
        :param link: (str) download URL.
        :param linkname: (str) incremental link name used for debugging and statistics purposes.

        :return: none.
        """
        with open(logfile, "r") as f:
            lines = f.readlines()
        # offset pointing at the line containing the download response
        offset = len(lines) - 2
        toks, sub_lines = [], []
        for i in range(0, len(lines) - 1):
            if "HTTP request sent, awaiting response..." in lines[offset-i]:
                # list of tokens containing the HTTP response code
                toks = lines[offset-i].split()[5:]
                # list of tokens containing the name of the downloaded file (if download was successful)
                sub_lines = lines[offset - i:]
                break
        if toks:
            # Process finished correctly, check if download was successful
            if toks[0] == "200":
                # File was downloaded correctly
                self.logger.info(f'Correctly Downloaded')
                # update the download counter
                self.downloaded_links += 1
                # update the download statistics
                self.save_correct_download(dataset_id, link, linkname, sub_lines)
            else:
                # Download process encountered an exception
                self.logger.info(f'Exception occurred')
                # update the exceptions counter
                self.exception_links += 1
                # update the download statics
                self.save_exception_download(dataset_id, link, linkname, toks)
        elif prc_returncode != 0:
            # process did not finish correctly, extract the type of error encountered
            if lines[-1] == '\n':
                if "Giving up" in lines[-2]:
                    # download failed after retrying multiple times
                    toks = ['other'] + lines[-3].split()[4:] + lines[-2].split()
                else:
                    toks = ['other'] + lines[-2].split()
            else:
                toks = ['other'] + lines[-1].split()
            self.logger.info(f'Exception occurred')
            # update the exceptions counter
            self.exception_links += 1
            # update the download statics
            self.save_exception_download(dataset_id, link, linkname, toks)
        else:
            #
            if lines[-1] == '\n':
                sub_lines = ["weird format", "other", ' '.join(lines[-2].split()[:6])]
            else:
                sub_lines = ["weird format", "other", ' '.join(lines[-1].split()[:6])]
            self.logger.info(f'Correctly Downloaded')
            self.downloaded_links += 1
            self.save_correct_download(dataset_id, link, linkname, sub_lines)

    def download_from_link(self, dirpath, linkname, dataset_id, link):
        """
        Downloads the file associated with the given link, using the Wget program.

        :param dirpath: (str) directory path where the download file will be saved.
        :param linkname: (str) incremental link name used for debugging and statistics purposes.
        :param dataset_id: (int) id of the dataset which comprises the considered URL.
        :param link: (str) download URL.

        :return: none.
        """
        logpath = self.output_path+'/logs/download/wget/dataset_'+str(dataset_id)
        os.makedirs(logpath, exist_ok=True)
        logfile = logpath+'/'+linkname+'.log'
        # Run the Wget program
        prc = subprocess.run(['wget', '-P', dirpath, '-o', logfile, link], capture_output=True, check=False, text=True)
        if prc.returncode == 0:
            self.logger.info(f'PRC Correctly Downloaded')
        else:
            self.logger.info(f"PRC exception occurred")
        # Analyze the output of the wget program
        self.analyze_output(prc.returncode, logfile, dataset_id, link, linkname)

    def test_download_links(self, datasets, start_id, end_id, signature):
        """
        Given a start_id and an end_id, tries to download each dataset's link using the Wget program.

        :param datasets: (json) Corpus containing all datasets and their metadata.
        :param start_id: (int) id of the first dataset to download.
        :param end_id: (int) id of the last dataset to download.
        :param signature: (str) signature string used for naming the statistics and log files.

        :return: (dict) dictionary of datasets containing a mapping between each link and the corresponding downloaded
                        file.
        """

        self.logger.info(f'--- TEST DOWNLOAD --- run {datetime.datetime.now()} ---\n')
        self.logger.info(f"Downloading dataset from id {start_id} to {end_id}")

        for idx in range(start_id, end_id+1):
            if idx in datasets.keys():
                d = datasets[idx]
                # Initialize download counters
                count_link = 0
                self.downloaded_links = 0
                self.exception_links = 0
                links = d['download']
                self.downloaded_dataset[idx] = {}
                # Create directory for dataset's files
                dirname = self.output_path + 'downloads/dataset_' + str(idx) + '/'
                os.makedirs(dirname, exist_ok=True)
                self.logger.info(f'--- Beginning download of dataset: {str(idx)} ---')
                self.logger.info(f'Number of links available --- {len(links)}')

                for link in d['download']:
                    count_link += 1
                    linkname = 'link_' + str(count_link)
                    self.logger.info(f'Trying to download link: {link}')
                    if self.check_file_compliance(link):
                        self.download_from_link(dirname, linkname, idx, link)
                    else:
                        self.logger.info(f'Unsuitable format --- {link}')
                        self.exception_links += 1
                        # rows follow the format:
                        # ['dataset_id', 'linkname', 'link', 'code', 'type', 'filename', 'filetype']
                        self.rows.append([idx, linkname, link, '--', 'format', '--', '--'])

                self.logger.info(f'Dataset {idx}: Number of downloaded links {self.downloaded_links}, '
                                 f'Number of broken links {self.exception_links}')

                self.stats[idx] = {'number_of_links': len(d['download']), 'downloaded_links': self.downloaded_links,
                                   'exception_links': self.exception_links}

                self.logger.info(f'--- End of download of dataset: {idx} ---')
                # Saving the download statistics and information in two separate files
                save_stats(self.stats, self.output_path+"stats/download/download_stats_"+signature+".json")
                save_output(self.rows, self.output_path+"results/download/download_infos_"+signature+".csv")

        self.logger.info(f'\n--- END OF TEST DOWNLOAD --- run {datetime.datetime.now()} ---')

        return self.downloaded_dataset
