import pyodbc
import traceback
import json
from utils import global_const as gc


class MetadataDB:
    def __init__(self, manifest_file):
        self.manifest_file = manifest_file
        self.error = manifest_file.error
        self.logger = manifest_file.logger
        self.conf_manifest = manifest_file.conf_manifest

        # load configuration values
        self.conf_database = gc.DB_CONFIG
        # info from main config file
        self.s_conn = self.conf_database['connection']['mdb_conn_str']
        self.procedure = self.conf_database['procedure']['load_manifest_record']
        self.alid_name = self.conf_database['manifest']['expected_aliquot_id_name']  # aliquot_id
        self.sid_name = self.conf_database['manifest']['expected_sample_id_name']  # sample_id
        # info from manifest location coinfig file
        self.study_id = self.conf_manifest.get_value('Database/study_id')

        self.conn = None

    def open_connection(self):
        self.logger.info('Attempting to open connection to Metadata DB.')
        try:
            self.conn = pyodbc.connect(self.s_conn, autocommit=True)
            self.logger.info('Successfully established the database connection.')
        except Exception as ex:
            # report unexpected error during openning DB connection
            _str = 'Unexpected Error "{}" occurred during an attempt to open connecton to database ({})\n{} ' \
                .format(ex, self.s_conn, traceback.format_exc())
            self.logger.error(_str)
            self.error.add_error(_str)

    def submit_row(self, row_dict, metadata_file_path):  # sample_id, row_json, dict_json, filepath):

        if not self.conn:
            # open connection if needed
            self.open_connection()

        if not self.conn:
            # connection failed to open
            self.logger.error('Database connection cannot be established (see earlier log info for details).')
            return None

        aliquot_id = row_dict[self.alid_name]
        sample_id = row_dict[self.sid_name]
        str_proc = self.procedure

        # {aliquot_id}',  @sample_id ='{sample_id}', @study_id = {study_id}, @manifest_data ='{@manifest_data}', @source_name='{source_name}'
        # json.dumps(manifest_row_dics)
        # prepare stored proc string to be executed
        str_proc = str_proc.replace('{aliquot_id}', str(aliquot_id))  # '{aliquot_id}'
        str_proc = str_proc.replace('{sample_id}', str(sample_id))  # '{sample_id}'
        str_proc = str_proc.replace('{study_id}', str(self.study_id))  # '{study_id}'
        str_proc = str_proc.replace('{@manifest_data}', str(json.dumps(row_dict)))  # '{manifest data}'
        str_proc = str_proc.replace('{source_name}', str(self.manifest_file.manifest_path))  # '{dict_json}'

        # '{samlpe_update}'

        self.logger.info('Attempting to execute the following SQL call: {}'.format(str_proc))
        # print ('procedure (str_proc) = {}'.format(str_proc))

        # TODO: if procedure execution does not fail but return back status saying "ERROR:", record an error for the row
        try:
            cursor = self.conn.cursor()
            cursor.execute(str_proc)
            # returned recordsets
            rs_out = []
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            results = []
            for row in rows:
                results.append(dict(zip(columns, row)))
            rs_out.extend(results)
            return rs_out

        except Exception as ex:
            # report an error if DB call has failed.
            _str = 'Error "{}" occurred during submitting a row (sample_id = "{}") to database ' \
                   'using this SQL script "{}". Here is the traceback: \n{} '.format(
                    ex, sample_id, str_proc, traceback.format_exc())
            self.error.add_error(_str)
            self.logger.error(_str)
