from file_load import File
import xlrd
from utils import common as cm


class DataRetrievalExcel(File):

    def __init__(self, filepath, req_error, req_logger, sheet_name='', file_type=2):

        # rec_error parameter is a pointer to the current request error object

        File.__init__(self, filepath, file_type)
        self.error = req_error
        self.logger = req_logger
        self.sheet_name = sheet_name
        # self.config =

        # self.get_file_content()

    def get_column_values(self, col_number, header_row_number=0, exclude_header=True):
        col_values = []
        # adjust passed numbers to the 0-based numbering
        # col_number = col_number - 1
        # header_row_number = header_row_number - 1
        if cm.file_exists(self.filepath):
            self.logger.debug('Loading column #{} from file "{}"'.format(col_number, self.filepath))

            with xlrd.open_workbook(self.filepath) as wb:
                sheet = self.get_wksheet_name(wb)
                if sheet:
                    sheet.cell_value(0, 0)
                    if sheet.ncols >= col_number >= 0:
                        for i in range(sheet.nrows):
                            if i < header_row_number:
                                # skip all rows before the header
                                pass
                            elif i == header_row_number and exclude_header:
                                pass
                            else:
                                cell = sheet.cell(i, col_number)
                                cell_value = self.validate_cell_value(cell, wb)
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

    def get_wksheet_name(self, wb):
        if not self.sheet_name or len(self.sheet_name) == 0:
            # by default retrieve the first sheet in the excel file
            sheet = wb.sheet_by_index(0)
        else:
            # if sheet name was provided
            sheets = wb.sheet_names()  # get list of all sheets
            if self.sheet_name in sheets:
                # if given sheet name in the list of available sheets, load the sheet
                sheet = wb.sheet_by_name(self.sheet_name)
            else:
                # report an error if given sheet name not in the list of available sheets
                _str = ('Given sheet name "{}" was not found in the file "{}". '
                        'Verify that the sheet name exists in the file.') \
                    .format(self.sheet_name, self.filepath)
                self.error.add_error(_str)
                self.logger.error(_str)
                sheet = None

        return sheet

    @staticmethod
    def validate_cell_value(cell, wb):
        cell_value = cell.value
        # take care of number and dates received from Excel and converted to float by default
        if cell.ctype == 2 and int(cell_value) == cell_value:
            # the key is integer
            cell_value = str(int(cell_value))
        elif cell.ctype == 2:
            # the key is float
            cell_value = str(cell_value)
        # convert date back to human readable date format
        # print ('cell_value = {}'.format(cell_value))
        if cell.ctype == 3:
            cell_value_date = xlrd.xldate_as_datetime(cell_value, wb.datemode)
            cell_value = cell_value_date.strftime("%Y-%m-%d")
        return cell_value

    def get_file_content(self):
        if not self.lineList:
            if cm.file_exists(self.filepath):
                self.logger.debug('Loading file content of "{}"'.format(self.filepath))

                with xlrd.open_workbook(self.filepath) as wb:
                    sheet = self.get_wksheet_name(wb)
                    if not sheet:
                        self.lineList = None
                        self.loaded = False
                        return self.lineList

                sheet.cell_value(0, 0)

                for i in range(sheet.nrows):
                    ln = []
                    for j in range(sheet.ncols):
                        # print(sheet.cell_value(i, j))
                        # ln.append('"' + sheet.cell_value(i,j) + '"')
                        cell = sheet.cell(i, j)
                        cell_value = self.validate_cell_value(cell, wb)
                        ln.append('"' + cell_value + '"')

                    self.lineList.append(','.join(ln))

                wb.unload_sheet(sheet.name)
                self.loaded = True
            else:
                _str = 'Loading content of the file "{}" failed since the file does not appear to exist".'.format(
                    self.filepath)
                self.error.add_error(_str)
                self.logger.error(_str)

                self.lineList = None
                self.loaded = False
        return self.lineList

