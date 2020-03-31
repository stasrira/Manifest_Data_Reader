from pathlib import Path
import os
import stat
import traceback
import time
import xlrd
from utils import global_const as gc
from utils import common as cm
from utils import common2 as cm2
from utils import setup_logger_common
from utils import ConfigData
from file_load import File  # , MetaFileExcel
from file_load.file_error import InquiryError
from data_retrieval import DataSource
import xlwt


class Inquiry(File):

    def __init__(self, filepath, cfg_path='', file_type=2, sheet_name=''):

        # load_configuration (main_cfg_obj) # load global and local configureations

        File.__init__(self, filepath, file_type)

        self.sheet_name = sheet_name  # .strip()

        if cfg_path=='':
            self.conf_main = ConfigData(gc.CONFIG_FILE_MAIN)
        else:
            self.conf_main = ConfigData(cfg_path)

        self.error = InquiryError(self)

        self.log_handler = None
        self.logger = self.setup_logger(self.wrkdir, self.filename)
        self.logger.info('Start working with Download Inquiry file {}'.format(filepath))
        self.inq_match_arr = []
        self.columns_arr = []

        self.processed_folder = gc.INQUIRY_PROCESSED_DIR
        # if a relative path provided, convert it to the absolute address based on the application working dir
        if not os.path.isabs(self.processed_folder):
            self.processed_folder = Path(self.wrkdir) / self.processed_folder
        else:
            self.processed_folder = Path(self.processed_folder)

        self.download_request_path = None

        self.disqualified_items = {}
        self.disqualified_inquiry_path = ''  # will store path to a inquiry file with disqualified sub-aliquots

        self.data_sources = None

        # self.sheet_name = ''
        # self.sheet_name = sheet_name  # .strip()
        if not self.sheet_name or len(self.sheet_name) == 0:
            # if sheet name was not passed as a parameter, try to get it from config file
            self.sheet_name = gc.INQUIRY_EXCEL_WK_SHEET_NAME  # 'wk_sheet_name'
        # print (self.sheet_name)
        self.logger.info('Data will be loaded from worksheet: "{}"'.format(self.sheet_name))

        self.conf_process_entity = None

        # print('GO in for 1st time')
        self.get_file_content()
        # print('Out For First Time')

    def get_file_content(self):
        # print('Inquiry get_file_content ---------')
        if not self.columns_arr or not self.lines_arr:
            self.columns_arr = []
            self.lines_arr = []
            if cm.file_exists(self.filepath):
                self.logger.debug('Loading file content of "{}"'.format(self.filepath))

                with xlrd.open_workbook(self.filepath) as wb:
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
                            _str = ('Given worksheet name "{}" was not found in the file "{}". '
                                    'Verify that the worksheet name exists in the file.').format(
                                self.sheet_name, self.filepath)
                            self.error.add_error(_str)
                            self.logger.error(_str)

                            self.lines_arr = None
                            self.loaded = False
                            return self.lines_arr

                sheet.cell_value(0, 0)

                lines = []  # will hold content of the inquiry file as an array of arrays (rows)
                columns = []
                for i in range(sheet.ncols):
                    column = []
                    for j in range(sheet.nrows):
                        if i == 0:
                            lines.append([])  # adds an array for each new row in the inquiry file

                        # print(sheet.cell_value(i, j))
                        cell = sheet.cell(j, i)
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
                            cell_value = cell_value_date.strftime("%Y-%m-%directory")
                        column.append(cell_value)  # adds value to the current column array
                        # lines[j].append('"' + cell_value + '"')  # adds value in "csv" format for a current row
                        lines[j].append(cell_value)

                    # self.columns_arr.append(','.join(column))
                    columns.append (column)  # adds a column to a list of columns

                # populate lines_arr and columns_arr properties
                self.lines_arr = lines
                self.columns_arr = columns

                # populate lineList value as required for the base class
                self.lineList = []
                for ln in lines:
                    self.lineList.append(','.join(str(ln)))

                wb.unload_sheet(sheet.name)

                # perform validation of the current inquiry file
                self.validate_inquiry_file()

                if self.error.exist():
                    # report that errors exist
                    self.loaded = False
                    # print(self.error.count)
                    # print(self.error.get_errors_to_str())
                    _str = 'Errors ({}) were identified during validating of the inquiry. \nError(s): {}'.format(
                        self.error.count, self.error.get_errors_to_str())
                else:
                    self.loaded = True

            else:
                _str = 'Loading content of the file "{}" failed since the file does not appear to exist".'.format(
                    self.filepath)
                self.error.add_error(_str)
                self.logger.error(_str)

                self.columns_arr = None
                self.lines_arr = None
                self.loaded = False
        return self.lineList

    def validate_inquiry_file(self):
        self.logger.info('Start validating the current inquiry file "{}".'.format(self.filepath))
        row_count = 1
        failed_cnt = 0
        for row in self.lines_arr:
            if row_count == self.header_row_num:  # 1
                # skip the first column as it is a header
                row_count +=1
                continue
            sub_al = row[4]  # get value that supposed to present a sub-aliquot value
            # go through 4 first fields and validate that provided values are expected
            for i in range(4):
                col_category = cm2.get_dict_value(str(i+1), 'inquiry_file_structure')
                if not cm2.key_exists_in_dict(str(row[i]).lower(), col_category):
                    _str = 'Unexpected value "{}" was provided for "{}" (line #{}, column #{})'\
                        .format(row[i],col_category, row_count, i+1)
                    self.logger.critical(_str)
                    # disqualify an inquiry file row, if unexpected value was provided for any of the first 4 fields
                    self.disqualify_inquiry_item(sub_al, _str, row)

                    failed_cnt +=1
                    break

            row_count +=1

        self.logger.info('Finish validating the inquiry file with{}.'
                         .format(' no errors.'
                                    if failed_cnt == 0
                                    else ' errors; {} records were disqualified - see earlier log entries for details'
                                 .format(failed_cnt)
                                 ))

    def setup_logger(self, wrkdir, filename):

        # m_cfg = ConfigData(gc.CONFIG_FILE_MAIN)

        log_folder_name = gc.INQUIRY_LOG_DIR  # gc.LOG_FOLDER_NAME

        # m_logger_name = gc.MAIN_LOG_NAME
        # m_logger = logging.getLogger(m_logger_name)

        logger_name = gc.INQUIRY_LOG_NAME
        logging_level = self.conf_main.get_value('Logging/request_log_level')

        # if a relative path provided, convert it to the absolute address based on the application working dir
        if not os.path.isabs(log_folder_name):
            log_folder_path = Path(wrkdir) / log_folder_name
        else:
            log_folder_path = Path(log_folder_name)

        lg = setup_logger_common(logger_name, logging_level,
                                 log_folder_path,  # Path(wrkdir) / log_folder_name,
                                 str(filename) + '_' + time.strftime("%Y%m%d_%H%M%S", time.localtime()) + '.log')

        self.log_handler = lg['handler']
        return lg['logger']

    def process_inquiry(self):
        self.conf_process_entity = self.load_source_config()

        #  self.data_source_locations = self.conf_process_entity.get_value('Datasource/locations')
        self.data_sources = DataSource(self)
        self.match_inquiry_items_to_sources()
        self.create_download_request_file()
        self.create_inquiry_file_for_disqualified_entries()

        # check for errors and put final log entry for the inquiry.
        if self.error.exist():
            _str = 'Processing of the current inquiry was finished with the following errors: {}\n'.format(
                self.error.get_errors_to_str())
            self.logger.error(_str)
        else:
            _str = 'Processing of the current inquiry was finished successfully.\n'
            self.logger.info(_str)

    def match_inquiry_items_to_sources(self):
        cur_row = 0
        for inq_line in self.lines_arr:
            if cur_row == self.header_row_num - 1:
                cur_row += 1
                continue
            # print(inq_line)
            # concatenate study_id for the current inquiry line using conversion of the field values
            # set in the dict_config.yaml
            inq_study_path = '/'.join([cm2.get_dict_value(str(inq_line[i]).lower(), cm2.get_dict_value(str(i+1), 'inquiry_file_structure'))
                                       for i in range(4)])
            # print (inq_study_path)
            assay = inq_line[3]  # identify assay for the current inquiry line
            sub_al = inq_line[4]  # identify sub-aliquot for the current inquiry line

            # check if current sub-aliquot is not part of disqualified items array
            if self.disqualified_items and sub_al in self.disqualified_items.keys():
                # if sub-aliquot was disqualifed already, skip this line
                continue

            # identify aliquot for the given sub-aliquot
            al = cm2.convert_sub_aliq_to_aliquot(sub_al, assay) # identify aliquot for the current inquiry line

            match = False
            for src_item in self.data_sources.source_content_arr:
                match_out = False
                # attempt match by the sub-aliquot
                match_out, match_details = \
                    self.is_item_found_soft_match(sub_al, src_item['name'], src_item['soft_comparisions'], sub_al)
                if match_out:
                    match = True
                # if sub-aliquot match was not success, attempt to match by the aliquot
                elif src_item['aliquot_match']:
                    match_out, match_details = \
                        self.is_item_found_soft_match(al, src_item['name'], src_item['soft_comparisions'], sub_al)
                    if match_out:
                        match = True
                # if a match was found using one of the above methods, record the item to inq_match_arr
                if match_out:
                    item_details = {
                        'sub-aliquot': sub_al,
                        'study': inq_study_path,
                        'source': src_item,
                        'match_details': match_details,
                        'obj_type': ('dir' if src_item['search_by'] == 'folder_name'
                                        else 'file' if src_item['search_by'] == 'file_name'
                                        else 'unknown')
                    }
                    self.inq_match_arr.append(item_details)

            if not match:
                self.disqualify_inquiry_item(sub_al, 'No match found in the data source.', inq_line)

    def is_item_found_soft_match(self, srch_item, srch_in_str, soft_match_arr, item_to_be_reported):
        out = False
        _str = ''
        # identify if the search is performed for sub_aliquot (full value) or aliquot (partial value)
        if srch_item == item_to_be_reported:
            entity = 'sub-aliquot'
        else:
            entity = 'aliquot'

        soft_match = False
        if srch_item in srch_in_str:
            out = True
        else:
            if soft_match_arr:
                for item in soft_match_arr:
                    srch_in_str = srch_in_str.replace(item['find'], item['replace'])
                    srch_item = srch_item.replace(item['find'], item['replace'])
                if srch_item in srch_in_str:
                    out = True
                    soft_match = True
        # prepare log entry
        if out:
            _str = str('Loose' if soft_match else 'Exact') + \
                   ' match was ' + \
                   'found for {} item "{}". Match values are as following: "{}" and "{}".'\
                       .format(entity, item_to_be_reported, srch_item, srch_in_str)

        # log outcome of the match process, the "soft" match will logged as warning
        if out:
            if entity == 'aliquot':
                # if match was found by aliquot (partial id value), always report it as "warning"
                self.logger.warning(_str)
            else:
                # proceed here if match was found by sub-aliquot (full id value)
                if soft_match:
                    self.logger.warning(_str)
                else:
                    self.logger.info(_str)

        # prepare match details to output from this function
        match_type = ''
        if soft_match:
            # this was a soft match
            if entity == 'aliquot':
                match_type = 'loose/aliquot'
            else:
                match_type = 'loose'
        else:
            # this was an exact match
            if entity == 'aliquot':
                match_type = 'exact/aliquot'
            else:
                match_type = 'exact'

        out_details = {
            'match_type': match_type,
            'details': _str
        }
        return out, out_details

    def load_source_config(self):
        cfg_source = ConfigData(Path(self.wrkdir) / gc.CONFIG_FILE_SOURCE_NAME)
        return cfg_source

    def create_download_request_file(self):
        self.logger.info("Start preparing download_request file.")
        # path for the script file being created
        rf_path = Path(gc.OUTPUT_REQUESTS_DIR + "/" + time.strftime("%Y%m%d_%H%M%S", time.localtime()) + '_' +
                       self.filename.replace(' ', '') + '.tsv')

        self.download_request_path = rf_path

        if not self.inq_match_arr:
            self.logger.warning('No inquiries with matched datasources exists for the current inquiry file. '
                                 'Skipping creating a download request file.')
            return

        with open(rf_path, "w") as rf:
            # write headers to the file
            headers = '\t'.join(['Source', 'Destination', 'Aliquot_id', 'Obj_Type'])
            rf.write(headers + '\n')

            for item in self.inq_match_arr:
                src_path = item['source']['path']

                #prepare values for the current inquiry row to put into the outcome file
                project_path = self.conf_process_entity.get_value('Destination/location/project_path')
                study_path = item['study']
                target_subfolder = item['source']['target_subfolder']
                sub_aliquot = item['sub-aliquot']
                obj_type = item['obj_type']

                # check if current sub-aliquot is not part of disqualified items array
                if self.disqualified_items and sub_aliquot in self.disqualified_items.keys():
                    # if sub-aliquot was disqualifed already, skip this line
                    continue

                # get template for the destination path and replace placeholders with values
                # "{project_path}/{study_path}/{target_subfolder}"
                dest_path = self.conf_process_entity.get_value('Destination/location/path_template')
                dest_path = dest_path.replace('{project_path}', project_path)
                dest_path = dest_path.replace('{study_path}', study_path)
                dest_path = dest_path.replace('{target_subfolder}', target_subfolder)


                line = '\t'.join([str(src_path), str(Path(dest_path)), str(sub_aliquot), str(obj_type)])
                rf.write(line +'\n')

        self.logger.info("Finish preparing download_request file '{}'.".format(rf_path))

    def disqualify_inquiry_item(self, sa, disqualify_status, inquiry_item):
        # adds a sub aliquots to the dictionary of disqualified items
        # key = sub-aliquot, values: dictionary with 2 values:
        #       'status' - reason for disqualification
        #       'inquiry_item: array of values for inquiry row from an inquiry file
        details = {'status': disqualify_status, 'inquiry_item':inquiry_item}
        self.disqualified_items[sa]= details
        self.logger.warning('Sub-aliquot "{}" was disqualified with the following status: "{}"'
                            .format(sa, disqualify_status))

    def create_inquiry_file_for_disqualified_entries(self):
        if self.disqualified_items:
            self.logger.info("Start preparing inquiry file for disqualified sub-aliquots.")
            # path for the script file being created

            wb = xlwt.Workbook()  # create empty workbook object
            sh = wb.add_sheet('Re-process_inquiry')  # sheet name can not be longer than 32 characters

            cur_row = 0  # first row for 0-based array
            cur_col = 0  # first col for 0-based array
            # write headers to the file
            headers = self.lines_arr[0]
            for val in headers:
                sh.write(cur_row, cur_col, val)
                cur_col += 1

            cur_row += 1

            for di in self.disqualified_items:
                fields = self.disqualified_items[di]['inquiry_item']
                cur_col = 0
                for val in fields:
                    sh.write(cur_row, cur_col, val)
                    cur_col += 1
                cur_row += 1

            if not os.path.isabs(gc.DISQUALIFIED_INQUIRIES):
                disq_dir = Path(self.wrkdir) / gc.DISQUALIFIED_INQUIRIES
            else:
                disq_dir = Path(gc.DISQUALIFIED_INQUIRIES)

            # if DISQUALIFIED_INQUIRIES folder does not exist, it will be created
            os.makedirs(disq_dir, exist_ok=True)

            # identify path for the disqualified inquiry file
            self.disqualified_inquiry_path = Path(str(disq_dir) + '/' +
                                                  time.strftime("%Y%m%d_%H%M%S", time.localtime()) +
                                                  '_reprocess_disqualified_' +
                                                # .stem method is used to get file name without an extension
                                                  Path(self.filename).stem.replace(' ', '') + '.xls')

            wb.save(str(self.disqualified_inquiry_path))

            self.logger.info("Successfully prepared the inquiry file for disqualified sub-aliquots and saved in '{}'."
                             .format(str(self.disqualified_inquiry_path)))
