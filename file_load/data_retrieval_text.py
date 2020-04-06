from file_load import File
from utils import common as cm


class DataRetrievalText(File):

    def __init__(self, filepath, req_error, req_logger, file_type=1):

        # req_error parameter is a pointer to the current request error object
        delim = cm.identify_delimeter_by_file_extension(filepath)
        File.__init__(self, filepath, file_type, file_delim=delim)
        self.error = req_error
        self.logger = req_logger
        self.sheet_name = ''

    def get_column_values(self, col_number, header_row_number=0, exclude_header=True):
        col_values = []
        # read content of the file
        self.get_file_content()

        if self.loaded:
            self.logger.debug('Loading column #{} from file "{}"'.format(col_number, self.filepath))

            self.header_row_num = header_row_number + 1  # accommodate for 1-base numbering
            if header_row_number < self.rows_count():
                for i in range(self.rows_count() + 1):
                    if i < self.header_row_num:
                        pass
                    elif i == self.header_row_num and exclude_header:
                        pass
                    else:
                        cell_value = self.get_row_by_number_to_list(i)[col_number]
                        col_values.append(cell_value)
            else:
                col_values = None
                # self.loaded = False
                # return col_values
        else:
            # no file found
            _str = 'Loading content of the file "{}" failed since the file does not appear to exist".' \
                .format(self.filepath)
            self.error.add_error(_str)
            self.logger.error(_str)

            col_values = None

        return col_values