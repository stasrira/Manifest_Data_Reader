from utils import common as cm
from file_load import DataRetrievalText
from file_load import DataRetrievalExcel
from app_error import ManifestFileError
import json
from data_retrieval import MetadataDB
import os


class ManifestFile:

    def __init__(self, manifest_path, manifest_location_obj):
        self.manifest_path = manifest_path  # file path
        self.file_name = os.path.basename(manifest_path)  #file name
        self.manifest_location_obj = manifest_location_obj
        self.processed = False

        self.conf_manifest = self.manifest_location_obj.conf_manifest  # manifest configuration file
        self.error = ManifestFileError(self) # self.manifest_location_obj.error
        self.logger = self.manifest_location_obj.logger
        self.manifest_location_error = self.manifest_location_obj.error

        self.alt_conf_manifest = None
        # check if current file is listed as an exception and has alternative configuration properties
        exc_files = self.conf_manifest.get_value('Exception_files')  # get Exception_files section, if available
        if exc_files and isinstance(exc_files, list):
            # exception section is present, check if the current file is listed as an exception
            for exc_item in exc_files:
                if exc_item['name'] == self.file_name:
                    self.alt_conf_manifest = exc_item  # save alternative configuration setting for the file, if present
                    break

        self.manifest_file_data = None
        self.manifest_columns = {}
        self.submitted_manifest_rows = {}

    def process_manifest(self):
        self.logger.info('Start processing manifest file: "{}"'.format(self.manifest_path))

        # set default values
        header_row_num_alt = None
        sheet_name_alt = None
        cfg_manifest_fields_alt = None

        if self.alt_conf_manifest:
            if 'File' in self.alt_conf_manifest.keys():
                if 'header_row_num' in self.alt_conf_manifest['File']:
                    header_row_num_alt = self.alt_conf_manifest['File']['header_row_num']
                if 'sheet_name' in self.alt_conf_manifest['File']:
                    sheet_name_alt = self.alt_conf_manifest['File']['sheet_name']
            if 'Fields' in self.alt_conf_manifest.keys():
                cfg_manifest_fields_alt = self.alt_conf_manifest['Fields']

        # use alternative header_row_num, if available, otherwise the one from main config section
        header_row_num = header_row_num_alt if header_row_num_alt \
            else self.conf_manifest.get_value('File/header_row_num')

        if cm.is_excel(self.manifest_path):
            # use alternative sheet_name, if available, otherwise the one from main config section
            sheet_name = sheet_name_alt if sheet_name_alt else self.conf_manifest.get_value('File/sheet_name')
            self.manifest_file_data = DataRetrievalExcel(self.manifest_path, self.error, self.logger, sheet_name)
        else:
            self.manifest_file_data = DataRetrievalText(self.manifest_path, self.error, self.logger)

        # set file's header related properties
        self.manifest_file_data.header_row_num = header_row_num
        self.manifest_file_data.replace_blanks_in_header = False
        # self.manifest_file_data.get_file_content()
        headers = self.manifest_file_data.headers

        cfg_manifest_fields = self.conf_manifest.get_value('Fields')

        if self.manifest_file_data.loaded:
            # If the manifest file was loaded proceed here
            # loop through manifest fields listed in the config file
            for mf in cfg_manifest_fields:
                match = False
                mf_alt = None
                # check if alternative config values for a field are present
                if cfg_manifest_fields_alt and isinstance(cfg_manifest_fields_alt, dict):
                    if mf in cfg_manifest_fields_alt.keys():
                        mf_alt = cfg_manifest_fields_alt[mf]
                # combine a "current" config set for a field based on availability of the properties in alt config set
                cur_cfg_manifest_fields = {
                    'name': mf_alt['name']
                    if mf_alt and 'name' in mf_alt.keys()
                    else cfg_manifest_fields[mf]['name'] if 'name' in cfg_manifest_fields[mf].keys() else None,
                    'required': mf_alt['required']
                    if mf_alt and 'required' in mf_alt.keys()
                    else cfg_manifest_fields[mf]['required'] if 'required' in cfg_manifest_fields[mf].keys() else None
                }
                # cur_cfg_manifest_fields = mf_alt if mf_alt else cfg_manifest_fields[mf]
                # loop through list of possible field names (in the file) listed for the current manifest field
                # for fn in cfg_manifest_fields[mf]['name']:
                if cur_cfg_manifest_fields['name']:
                    for fn in cur_cfg_manifest_fields['name']:
                        cnt = 0
                        match = False
                        # loop through the list of fields in the header and compare to the list
                        for hdr in headers:
                            if hdr.strip() == fn.strip():
                                # assign column of values from the file for the matching manifest field
                                self.manifest_columns[mf.strip()] = self.manifest_file_data.get_column_values(cnt)
                                match = True
                                break
                            cnt += 1
                        if match:
                            # if a match was found, exist current loop and go to next manifest field
                            break
                if not match:
                    if 'required' in cur_cfg_manifest_fields and cur_cfg_manifest_fields['required']:
                        _str = 'Cannot find a matching field in the manifest file for the required ' \
                               'manifest field "{}"; list of expected field names: {}.'\
                            .format(mf, cur_cfg_manifest_fields['name'])
                        self.logger.error(_str)
                        self.error.add_error(_str)
        else:
                _str = 'Cannot load manifest file {}. Check earlier log entries for errors.'.format(self.manifest_path)
                self.logger.error(_str)
                self.error.add_error(_str)

        manifest_rows = self.prepare_manifest_rows(False)

        if self.error.exist():
            _str = 'Aborting processing current manifest, since errors were reported; file: "{}".'\
                .format(self.manifest_path)
            self.logger.error(_str)
            # self.error.add_error(_str)
            return
        else:
            # if no errors reported
            self.logger.info('Start loading current manifest rows to Metadata DB.')
            mdb = MetadataDB(self)
            for row in manifest_rows:
                if not self.error.exist():
                    # proceed if no system errors were reported
                    outcome = mdb.submit_row(row, self.manifest_path)
                    if not mdb.error.exist():
                        # no critical errors produced during submitting the row to MDB
                        if outcome:
                            for outcome_row in outcome:
                                _str1 = 'Status {} was received for processing row ({}). '\
                                    .format(outcome_row['status'], row)
                                _str2 = 'Detailed messages from database => {}'.format(outcome_row['description'])
                                if outcome_row['status'] == 'OK':
                                    self.logger.info(_str1)
                                    self.logger.info(_str2)
                                else:
                                    self.logger.warning(_str1)
                                    self.logger.warning(_str2)
                                # save stats of processing a row to a special dictionary;it will be use to produce stats
                                if not outcome_row['status'] in self.submitted_manifest_rows.keys():
                                    self.submitted_manifest_rows[outcome_row['status']] = []
                                self.submitted_manifest_rows[outcome_row['status']].append((row[mdb.alid_name], outcome_row))
                    else:
                        # some errors produced during submitting a row to MDB, abort processing the file
                        self.logger.error(
                            'Aborting processing current manifest, since errors were reported in the database '
                            'communication module; file: "{}".'
                            .format(self.manifest_path))
                        return
                else:
                    self.logger.error('Aborting processing current manifest, since errors were reported; file: "{}".'
                                      .format(self.manifest_path))
                    return
            self.processed = True

    # function goes through all found columns and creates dictionary or json for each row of a manifest
    def prepare_manifest_rows(self, json_format = None):
        if not json_format:
            json_format = False
        else:
            json_format = True

        manifest_rows = []
        if self.manifest_columns:
            list_len_max = 0
            # loop through all manifest columns and find max size across all columns
            for item in self.manifest_columns:
                if len(self.manifest_columns[item]) > list_len_max:
                    list_len_max = len(self.manifest_columns[item])

            # loop through all columns based on the value in list_len_max
            for i in range(list_len_max):
                manifest_row_dics = {}
                # loop through all manifest columns list and pick up appropriate values
                for item in self.manifest_columns:
                    # print ('Item: {}, value = {}'.format(item, self.manifest_columns[item][i]))
                    if self.manifest_columns[item][i]:
                        manifest_row_dics[item] = self.manifest_columns[item][i]
                    else:
                        manifest_row_dics[item] = ''

                # validate that the prepared row is not empty (all values are blank)
                some_value_present = False
                for key in manifest_row_dics.keys():
                    if len(str(manifest_row_dics[key]).strip()) > 0:
                        some_value_present = True
                        break
                # if no values were found, the row is blank and should be rejected
                if some_value_present:
                    # if some values were found in the row, add it to the final list of rows
                    # append created dictionary of a row to a dictionary of rows
                    if json_format:
                        manifest_rows.append(json.dumps(manifest_row_dics))
                    else:
                        manifest_rows.append(manifest_row_dics)
                else:
                    # if no value were found for the current row it will be skipped
                    self.logger.warning(
                        'Blank row will be ignored - row #{} of the file "{}".'
                            .format(i + 1 + self.manifest_file_data.header_row_num, self.manifest_path))
            return manifest_rows



