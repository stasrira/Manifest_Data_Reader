import os
# import time
from pathlib import Path
import logging
from file_load.file_error import FileError
# from utils import setup_logger_common
# from utils import global_const as gc
from utils import common as cm
from file_load import StudyConfig
from csv import reader
from collections import OrderedDict


#  Text file class (used as a base)
class File:
    filepath = None
    wrkdir = None
    filename = None
    file_type = None  # 1:text, 2:excel
    file_delim = None  # ','
    lines_arr = None  # []
    __headers = None  # []
    error = None  # FileErrors class reference holding all errors associated with the current file
    sample_id_field_names = None  # []
    loaded = None
    logger = None

    def __init__(self, filepath, file_type=1, file_delim=',', replace_blanks_in_header=True):
        self.filepath = filepath
        self.wrkdir = os.path.dirname(os.path.abspath(filepath))
        self.filename = Path(os.path.abspath(filepath)).name
        self.file_type = file_type
        self.file_delim = file_delim
        self.error = FileError(self)
        self.lines_arr = []
        self.__headers = []
        self.log_handler = None
        self.header_row_num = 1  # default header row number
        self.sample_id_field_names = []
        self.replace_blanks_in_header = replace_blanks_in_header
        self.loaded = False

    @property
    def headers(self):
        if not self.__headers:
            self.get_headers()
        return self.__headers

    def setup_logger(self, wrkdir, filename):
        pass
        '''
        log_folder_name = gc.INQUIRY_LOG_DIR  # gc.LOG_FOLDER_NAME

        # if a relative path provided, convert it to the absolute address based on the application working dir
        if not os.path.isabs(log_folder_name):
            log_folder_path = Path(wrkdir) / log_folder_name
        else:
            log_folder_path = Path(log_folder_name)

        lg = setup_logger_common(StudyConfig.study_logger_name, StudyConfig.study_logging_level,
                                 log_folder_path,  # Path(wrkdir) / log_folder_name,
                                 filename + '_' + time.strftime("%Y%m%d_%H%M%S", time.localtime()) + '.log')

        self.log_handler = lg['handler']
        return lg['logger']
        '''

    def get_file_content_1(self):
        if not self.logger:
            loc_log = logging.getLogger(StudyConfig.study_logger_name)
        else:
            loc_log = self.logger

        if not self.lines_arr:
            if cm.file_exists(self.filepath):
                loc_log.debug('Loading file content of "{}"'.format(self.filepath))
                with open(self.filepath, "r") as fl:
                    self.lines_arr = [line.rstrip('\n') for line in fl]
                    fl.close()
                    self.loaded = True
            else:
                _str = 'Loading content of the file "{}" failed since the file does not appear to exist".'.format(
                    self.filepath)
                self.error.add_error(_str)
                loc_log.error(_str)
                self.lines_arr = None
                self.loaded = False
        return self.lines_arr

    def get_headers(self):
        if not self.__headers:
            hdrs = self.get_row_by_number_to_list(self.header_row_num)

            if self.replace_blanks_in_header:
                self.__headers = [hdr.strip().replace(' ', '_') for hdr in hdrs]
        return self.__headers

    def get_row_by_number(self, rownum):
        line_list = self.get_file_content()
        # check that requested row is withing available records of the file and >0
        if line_list is not None and len(line_list) >= rownum > 0:
            return line_list[rownum - 1]
        else:
            return ''

    def get_row_by_number_to_list(self, rownum):
        row = self.get_row_by_number(rownum)
        row_list = list(reader([row], delimiter=self.file_delim, skipinitialspace=True))[0]
        return row_list

    def get_row_by_number_with_headers(self, rownum):
        row = self.get_row_by_number_to_list(rownum)
        row_with_header = OrderedDict()  # output dictionary
        header = self.get_headers()
        for field, title in zip(row, header):
            row_with_header[title] = field
        return row_with_header

    def rows_count(self, exclude_header=False):
        num = len(self.get_file_content())
        if exclude_header:
            num = num - 1
        return num
