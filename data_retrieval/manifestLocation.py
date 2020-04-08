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
        self.manifest_files = []
        # self.conf_main = ConfigData(gc.CONFIG_FILE_MAIN)

        self.error = ManifestLocationError(self)
        self.logger = logger

        # check if manifest config file is present
        if not cm.file_exists(self.manifest_local_config_path):
            _str = 'Expected to exist config file "{}" was not present.'.format(self.manifest_local_config_path)
            self.logger.warning(_str)
            self.error.add_error(_str)
            self.disqualified = True
            self.disqualified_reasons.append(_str)
            return

        self.conf_manifest = ConfigData(self.manifest_local_config_path)
        # validate loaded config file; it will set "self.disqualified = True", in case of errors
        self.validate_manifest_config()

        pass

    def process_manifests(self):
        (root, _, manifest_files) = next(os.walk(self.location_path))

        for manifest_file in manifest_files:
            if manifest_file == self.manifest_local_config_file:
                # ignore local config file
                continue
            if manifest_file.startswith('~$'):
                # ignore temporary excel files (present when a file is open for reading)
                continue
            self.logger.info('Manifest file selected for processing: "{}"'.format(manifest_file))
            manifest_file_obj = ManifestFile(Path(root + '/' + manifest_file), self)
            self.manifest_files.append(manifest_file_obj)
            manifest_file_obj.process_manifest()
            if manifest_file_obj.processed:
                self.logger.info('Manifest file was processed successfully; file: "{}"'.format(manifest_file))
            else:
                self.logger.warning(
                    'Manifest file was not processed (file: "{}"). See errors reported earlier in the log'
                        .format(manifest_file, manifest_file_obj.error.count))
        pass

    def validate_manifest_config(self):
        self.logger.info('Start validation of the config file "{}"'.format(self.manifest_local_config_path))
        str_errors = []
        if self.conf_manifest.cfg:
            if not 'Database' in self.conf_manifest.cfg.keys():
                _str = 'Expected section "Database" was no found in the config file.'
                str_errors.append(_str)
            else:
                if not 'study_id' in self.conf_manifest.cfg['Database'].keys():
                    _str = 'Expected parameter "Database\\study_id" was no found in the config file.'
                    str_errors.append(_str)
            if not 'Fields' in self.conf_manifest.cfg.keys():
                _str = 'Expected section "Fields" was no found in the config file.'
                str_errors.append(_str)
            else:
                if not 'aliquot_id' in self.conf_manifest.cfg['Fields'].keys():
                    _str = 'Expected parameter "Fields\\aliquot_id" was no found in the config file.'
                    str_errors.append(_str)
                if not 'sample_id' in self.conf_manifest.cfg['Fields'].keys():
                    _str = 'Expected parameter "Fields\\sample_id" was no found in the config file.'
                    str_errors.append(_str)
        else:
            _str = 'Manifest config file is blank or cannot be read.'
            str_errors.append(_str)

        if str_errors:
            self.disqualified = True
            for err in str_errors:
                self.logger.error(err)
                self.error.add_error(err)
                self.disqualified_reasons.append(err)
            self.logger.info('Errors reported during validation of the config file "{}"'
                             .format(self.manifest_local_config_path))
        else:
            self.logger.info('Successful validation of the config file "{}"'.format(self.manifest_local_config_path))

