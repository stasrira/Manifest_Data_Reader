from app_error import ManifestLocationError
from utils import common as cm
from pathlib import Path
from utils import ConfigData
import os
from file_load import ManifestFile
from file_load import DataRetrievalText
from file_load import DataRetrievalExcel

class ManifestLocation:

    def __init__(self, manif_location_details, logger):
        self.disqualified = False
        self.disqualified_reasons = []
        self.location_path = manif_location_details['path']
        self.manifest_local_config_file = manif_location_details['config']
        self.manifest_local_config_path = Path(self.location_path + '/' + self.manifest_local_config_file)
        # self.conf_main = ConfigData(gc.CONFIG_FILE_MAIN)

        self.error = ManifestLocationError(self)
        self.logger = logger

        # check if manifest config file is present
        if not cm.file_exists(self.manifest_local_config_path):
            _str = 'Expected to exist config file "{}" was not present.'.format(self.manifest_local_config_path)
            self.logger.warning(_str)
            self.disqualified = True
            self.disqualified_reasons.append(_str)
            return

        self.conf_manifest = ConfigData(self.manifest_local_config_path)

        print('')

    def process_manifests(self):
        (root, _, manifest_files) = next(os.walk(self.location_path))

        for manifest_file in manifest_files:
            if manifest_file == self.manifest_local_config_file:
                # ignore local config file
                continue
            if manifest_file.startswith('~$'):
                # ignore temporary excel files (present when a file is open for reading)
                continue

            manifest_file_obj = ManifestFile(Path(root + '/' + manifest_file), self)

            '''
            header_row_num = self.conf_manifest.get_value('File/header_row_num')
            sheet_name = self.conf_manifest.get_value('File/sheet_name')

            if cm.is_excel(manifest_file):
                self.manifest_file_data = DataRetrievalExcel(Path(root + '/' + manifest_file), self.error, self.logger, sheet_name)
            else:
                self.manifest_file_data = DataRetrievalText(Path(root + '/' + manifest_file), self.error, self.logger)
            # self.manifest_file_data.get_file_content()
            aliquots = self.manifest_file_data.get_column_values(4)
            '''
            print('')