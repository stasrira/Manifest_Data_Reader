from utils import common as cm
from file_load import DataRetrievalText
from file_load import DataRetrievalExcel
from app_error import EntityErrors
import json
from data_retrieval import MetadataDB


class ManifestFile:

    def __init__(self, manifest_path, manifest_location_obj):
        self.manifest_path = manifest_path
        self.manifest_location_obj = manifest_location_obj
        self.disqualified = False
        self.disqualified_reasons = []

        self.conf_manifest = self.manifest_location_obj.conf_manifest
        self.error = EntityErrors(self) # self.manifest_location_obj.error
        self.logger = self.manifest_location_obj.logger

        self.manifest_file_data = None

        self.manifest_columns = {
            'aliquot_id':[],
            'sample_id': [],
            'creation_date': [],
            'volume': [],
            'num_cells': []}

        self.process_manifest()

    def process_manifest(self):

        header_row_num = self.conf_manifest.get_value('File/header_row_num')

        if cm.is_excel(self.manifest_path):
            sheet_name = self.conf_manifest.get_value('File/sheet_name')
            self.manifest_file_data = DataRetrievalExcel(self.manifest_path, self.error, self.logger, sheet_name)
            self.manifest_file_data.header_row_num = header_row_num
            self.manifest_file_data.replace_blanks_in_header = False
        else:
            self.manifest_file_data = DataRetrievalText(self.manifest_path, self.error, self.logger)
            self.manifest_file_data.header_row_num = header_row_num
            self.manifest_file_data.replace_blanks_in_header = False
        # self.manifest_file_data.get_file_content()
        headers = self.manifest_file_data.headers

        cfg_manifest_fields = self.conf_manifest.get_value('Fields')

        if self.manifest_file_data.loaded:
            # If the manifest file was loaded proceed here
            # loop through manifest fields listed in the config file
            for mf in cfg_manifest_fields:
                # loop through list of possible field names (in the file) listed for the current manifest field
                for fn in cfg_manifest_fields[mf]['name']:
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
                        # if a match was found, exist loop current loop and go to next manifest field
                        break
                    else:
                        if 'required' in cfg_manifest_fields[mf] and cfg_manifest_fields[mf]['required']:
                            _str = 'Cannot find a matching field in the manifest file for the required ' \
                                   'manifest field "{}"; list of expected field names: {}.'\
                                .format(mf, cfg_manifest_fields[mf]['name'])
                            self.logger.error(_str)
                            self.error.add_error(_str)
        else:
            _str = 'Cannot load manifest file {}. Check earlier log entry for errors.'.format(self.manifest_path)
            self.logger.error(_str)
            self.error.add_error(_str)

        manifest_rows = self.prepare_manifest_rows(False)

        print ('')

        if self.error.exist():
            _str = 'Aborting processing current manifiest, since errors were reported "{}".'.format(self.manifest_path)
            self.logger.error(_str)
            self.error.add_error(_str)
            return
        else:
            # if no errors reported
            # TODO: load manifest to DB
            mdb = MetadataDB(self)
            for row in manifest_rows:
                outcome = mdb.submit_row(row, self.manifest_path)
                print (outcome)
            pass

    # function goes through all found columns and creates dictionary or json for each row of a manifest
    def prepare_manifest_rows(self, jsonFormat = None):
        if not jsonFormat:
            jsonFormat = False
        else:
            jsonFormat = True

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
                # append created dictionary of a row to a dictionary of rows
                if jsonFormat:
                    manifest_rows.append(json.dumps(manifest_row_dics))
                else:
                    manifest_rows.append(manifest_row_dics)
            return manifest_rows



