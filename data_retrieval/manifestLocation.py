from app_error import ManifestLocationError
from utils import common as cm
from pathlib import Path
from utils import ConfigData
import os
from file_load import ManifestFile


class ManifestLocation:

    def __init__(self, manif_location_details, logger, main_config):
        self.disqualified = False
        self.disqualified_reasons = []
        self.location_path = manif_location_details['path']
        self.manifest_local_config_file = manif_location_details['config']
        self.manifest_local_config_path = Path(self.location_path + '/' + self.manifest_local_config_file)
        self.manifest_files = []
        self.conf_manifest = None

        self.error = ManifestLocationError(self)
        self.logger = logger
        self.main_cfg = main_config

        # check if manifest config file is present
        if not cm.file_exists(self.manifest_local_config_path):
            _str = 'Expected to exist config file "{}" was not present.'.format(self.manifest_local_config_path)
            self.logger.warning(_str)
            self.error.add_error(_str)
            self.disqualified = True
            self.disqualified_reasons.append(_str)
            return

        self.conf_manifest = ConfigData(self.manifest_local_config_path)

        pass

    def process_manifests(self):
        (root, _, manifest_files) = next(os.walk(self.location_path))

        ignore_files = self.main_cfg.get_value('Location/ignore_files')  # global list of files to be always ignored

        for manifest_file in manifest_files:
            if manifest_file == self.manifest_local_config_file:
                # ignore local config file
                continue
            if manifest_file.startswith('~$'):
                # ignore temporary excel files (present when a file is open for reading)
                continue
            if manifest_file in ignore_files:
                # ignore any files in the global ignore list
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
        warnings_produced = False
        # check if manifest config file is not blank
        if not self.conf_manifest.cfg:
            str_errors.append('The manifest file is blank, cannot proceed.')
        # check if manifest config file has some keys inside
        if not isinstance(self.conf_manifest.cfg,dict):
            str_errors.append('The manifest file is not properly formatted, cannot find any key/value pairs.')
        # get validation rules from the main config file
        mnf_cfg_validation = self.main_cfg.get_value ('Validate/manifest_config_fields')
        if not str_errors:
            if mnf_cfg_validation:
                for validate_section in mnf_cfg_validation:  # loop through sections outlined in the config file
                    sec_name = list(validate_section.keys())[0]
                    # verify that expected sections are present in the config file
                    if not sec_name in self.conf_manifest.cfg.keys():
                        _str = 'Expected section "{}" was no found in the config file.'.format(sec_name)
                        str_errors.append(_str)
                    else:
                        section_items = validate_section[sec_name]
                        if section_items:
                            # validate that required fields are present inside of the section of the config file
                            for item in section_items:
                                if not item['name'] in self.conf_manifest.cfg[sec_name].keys():
                                    if item['required']:
                                        _str = 'Required parameter "{}\\{}" was not found in the config file.'\
                                            .format(sec_name, item['name'])
                                        str_errors.append(_str)
                                    else:
                                        _str = 'Expected optional parameter "{}\\{}" was not found in the config file.' \
                                            .format(sec_name, item['name'])
                                        self.logger.warning(_str)
                                        warnings_produced = True
                            # check for unexpected fields in the manifest config file
                            for manif_item in self.conf_manifest.cfg[sec_name].keys():
                                match_found = False
                                for item in section_items:
                                    if manif_item == item['name']:
                                        match_found = True
                                        break
                                if not match_found:
                                    _str = 'Unexpected parameter "{}\\{}" was found in the config file.' \
                                        .format(sec_name, manif_item)
                                    self.logger.warning(_str)
                                    warnings_produced = True
        if str_errors:
            # if errors are present, disqualify the manifest location
            self.disqualified = True
            for err in str_errors:
                self.logger.error(err)
                self.error.add_error(err)
                self.disqualified_reasons.append(err)
            self.logger.info('Errors reported during validation of the config file "{}"'
                             .format(self.manifest_local_config_path))
        else:
            if warnings_produced:
                self.logger.info(
                    'Validation (with warnings) passed for the config file "{}"'.format(self.manifest_local_config_path))
            else:
                self.logger.info(
                    'Successful validation of the config file "{}"'.format(self.manifest_local_config_path))

