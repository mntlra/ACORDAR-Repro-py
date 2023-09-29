from .utils import singleton
import logging
import sys
import configparser


class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """
    def __init__(self, logger, level):
       self.logger = logger
       self.level = level
       self.linebuf = ''

    def write(self, buf):
       for line in buf.rstrip().splitlines():
          self.logger.log(self.level, line.rstrip())

    def flush(self):
        pass


@singleton
class Logger:

    def __init__(self, filepath=None):

        config = configparser.SafeConfigParser()
        config.optionxform = str
        #config.read('../../properties/common.ini')

        formatter = logging.Formatter(
            '%(asctime)s - [%(module)s:%(lineno)d]- %(levelname)s - %(message)s')

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

        lhandler = logging.StreamHandler()
        lhandler.setFormatter(formatter)

        # Print in stdout
        consoleHandler = logging.StreamHandler(sys.stdout)
        self.logger.addHandler(consoleHandler)

        if filepath is None:
            self.logger.addHandler(lhandler)
        else:
            self.set_file(filepath)

    def set_file(self, filepath):
        lhandler = logging.FileHandler(filepath, 'w')
        self.logger.addHandler(lhandler)

        sys.stdout = StreamToLogger(self.logger, logging.DEBUG)
        sys.stderr = StreamToLogger(self.logger, logging.ERROR)

        return self
