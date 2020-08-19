from pathlib import Path
import sys
import os
import time
import traceback
from utils import setup_logger_common, deactivate_logger_common, common as cm
from utils import ConfigData
from utils import global_const as gc
from utils import send_email as email
from collections import OrderedDict
from data_retrieval import ManifestLocation

# if executed by itself, do the following
if __name__ == '__main__':

    # load main config file and get required values
    m_cfg = ConfigData(gc.CONFIG_FILE_MAIN)

    # print ('m_cfg = {}'.format(m_cfg.cfg))
    # assign values
    common_logger_name = gc.MAIN_LOG_NAME  # m_cfg.get_value('Logging/main_log_name')

    # get path configuration values
    logging_level = m_cfg.get_value('Logging/main_log_level')
    # get path configuration values and save them to global_const module
    # path to the folder where all application level log files will be stored (one file per run)
    gc.APP_LOG_DIR = m_cfg.get_value('Location/app_logs')

    log_folder_name = gc.APP_LOG_DIR  # gc.LOG_FOLDER_NAME

    prj_wrkdir = os.path.dirname(os.path.abspath(__file__))

    email_msgs = []
    # email_attchms = []

    # get current location of the script and create Log folder
    # if a relative path provided, convert it to the absolute address based on the application working dir
    if not os.path.isabs(log_folder_name):
        logdir = Path(prj_wrkdir) / log_folder_name
    else:
        logdir = Path(log_folder_name)
    # logdir = Path(prj_wrkdir) / log_folder_name  # 'logs'
    lg_filename = time.strftime("%Y%m%d_%H%M%S", time.localtime()) + '.log'

    lg = setup_logger_common(common_logger_name, logging_level, logdir, lg_filename)  # logging_level
    mlog = lg['logger']

    mlog.info('Start processing Sample Manifests')

    gc.DB_CONFIG = m_cfg.get_value('Database')

    try:

        locations_list = m_cfg.get_value('Location/sources')
        manif_dir_name_default = m_cfg.get_value('Manifest/folder_name')
        manif_cfg_file_name_default = m_cfg.get_value('Manifest/config_file_name')

        manifest_locations = OrderedDict()
        manifest_loc_objs = []

        if locations_list:
            for location in locations_list:
                # print (location)
                manif_dir_name_loc = None
                manif_cfg_file_name_loc = None
                # check if Manifest section was provided for a current location
                if isinstance(location, dict) and 'Manifest' in location.keys():
                    manifest_loc = location['Manifest']
                    # get folder name assigned to this location, if provided
                    if isinstance(manifest_loc, dict) and 'folder_name' in manifest_loc.keys():
                        manif_dir_name_loc = manifest_loc['folder_name']
                    # get config file name assigned to this location, if provided
                    if isinstance(manifest_loc, dict) and 'config_file_name' in manifest_loc.keys():
                        manif_cfg_file_name_loc = manifest_loc['config_file_name']

                qualif_dirs = cm.get_file_system_items_global(
                    location['path'],
                    'dir',
                    manif_dir_name_loc if manif_dir_name_loc else manif_dir_name_default)

                for qualif_dir in qualif_dirs:
                    cfg_file_name = manif_cfg_file_name_loc if manif_cfg_file_name_loc else manif_cfg_file_name_default
                    manifest_locations[qualif_dir] = {
                        'path': qualif_dir,
                        'config': cfg_file_name
                    }
                # print (qualif_dirs)
            # print (manifest_locations)

            # manifest_loc_objs = []
            manifest_loc_obj = None
            for manifest_loc in manifest_locations:
                try:
                    mlog.info('-->>Selected for processing manifest directory: "{}"'.format(manifest_loc))
                    manifest_loc_obj = ManifestLocation(manifest_locations[manifest_loc], mlog, m_cfg)
                    manifest_loc_objs.append(manifest_loc_obj)
                    # if not disqualified yet, validate the manifest config file
                    if not manifest_loc_obj.disqualified:
                        # validate loaded config file; it will set "self.disqualified = True", in case of errors
                        manifest_loc_obj.validate_manifest_config()
                    # if not disqualified yet, process manifest
                    if not manifest_loc_obj.disqualified:
                        # process manifest files in the current manifest location
                        manifest_loc_obj.process_manifests()
                        # log status of processing files in the manifest location
                        mlog.info('Finish processing of {} manifest(s) in: "{}".'
                                  .format(len(manifest_loc_obj.manifest_files), manifest_loc))
                        mlog.info('=> Beginning of the status summary of the processed manifests in "{}"'
                                  .format(manifest_loc))
                        for manifest_file in manifest_loc_obj.manifest_files:
                            if not manifest_file.error.exist():
                                fl_status = 'OK'
                                _str = 'Processing status: "{}". Manifest file: {}' \
                                    .format(fl_status, manifest_file.manifest_path)
                            else:
                                fl_status = 'ERROR'
                                _str = 'Processing status: "{}". Check processing log file for this manifest: {}' \
                                    .format(fl_status, manifest_file.logger.handlers[0])
                                errors_present = 'ERROR'
                            if fl_status == "OK":
                                mlog.info(_str)
                            else:
                                mlog.warning(_str)
                        mlog.info('=> End of the status summary of the processed manifests in "{}"'
                                  .format(manifest_loc))
                    else:
                        mlog.info('=> Current manifest directory was disqualified with the following reason: "{}"'
                                  .format(manifest_loc_obj.disqualified_reasons))
                except Exception as ex:
                    # report unexpected error during processing manifest location
                    _str = 'Unexpected Error "{}" occurred during processing manifest location "{}" \n{} ' \
                        .format(ex, manifest_loc, traceback.format_exc())
                    mlog.critical(_str)
                    if manifest_loc_obj:
                        manifest_loc_obj.disqualified = True
                        manifest_loc_obj.disqualified_reasons.append(_str)
        else:
            mlog.error("No 'Location/sources' were provided in the main config file.")

        # prepare email messages for each proceed location
        email_msgs = cm.prepare_status_email(manifest_loc_objs)

        # collect stats for errors and disqualifications across all manfiest locations and files
        mnf_errors_cnt = 0
        mnf_disqual_cnt = 0
        fls_errors_cnt = 0
        fls_processed_cnt = 0
        if len(manifest_loc_objs) > 0:
            for m_obj in manifest_loc_objs:
                if m_obj.error:
                    # count errors towared total count of manifest errors
                    mnf_errors_cnt += m_obj.error.count
                if m_obj.disqualified and m_obj.disqualified_reasons:
                    # count disqualifications towared total count of manifest disqualifications
                    mnf_disqual_cnt += len(m_obj.disqualified_reasons)

                # add processed manifest files toward total count of files
                fls_processed_cnt += len(m_obj.manifest_files)
                # check for errors during processing manifest files
                for file in m_obj.manifest_files:
                    if file.error:
                        # count errors towared total count of manifest errors
                        fls_errors_cnt += file.error.count

        # collect final details and send notification email
        mlog.info('Preparing to send notificatoin email.')

        email_to = m_cfg.get_value('Email/send_to_emails')
        email_subject = 'processing of manifest files. '

        if mnf_errors_cnt + mnf_disqual_cnt + fls_errors_cnt == 0:
            if len(manifest_loc_objs) > 0:
                # no errors or disqualifications were reported
                email_subject = 'SUCCESSFUL processing of manifest files.'
            else:
                # no manifests were processed
                email_subject = 'No manifest files were processed.'
        else:
            if mnf_disqual_cnt > 0:
                if mnf_errors_cnt + fls_errors_cnt > 0:
                    email_subject = 'ERROR(s) and DISQUALIFICATION(s) are present during processing of manifest files.'
                else:
                    email_subject = 'DISQUALIFICATION(s) are present during processing of manifest files.'
            else:
                email_subject = 'ERROR(s) are present during processing of manifest files.'

        # prepare stats of processed vs not processed locations for the status email
        identified_loc = len(manifest_locations)  # number of identified locations
        processed_loc = len(manifest_loc_objs)  # number of identified locations
        not_processed_loc = []  # array to keep not processed locations
        if manifest_locations and manifest_loc_objs:
            # if not all locations were processed, collect them into not_processed_loc list
            if identified_loc > processed_loc:
                for lc in manifest_locations:
                    l_match = False
                    for lo in manifest_loc_objs:
                        if lc == lo.location_path:
                            l_match = True
                            break
                    if not l_match:
                        not_processed_loc.append(lc)

        email_body = ('Total of identified manifest locations: <b>{}</b>'.format(identified_loc)
                      + '<br/>Total of processed manifest locations: <font color = "green"><b>{}</b></font>'
                      .format(processed_loc)
                      + '<br/>Log file for the run: {}'.format(str(mlog.handlers[0].baseFilename))
                      + (
                          '<br/><br/><font color="red">Total of not processed locations: <b>{}</b>'
                              .format(identified_loc - processed_loc)
                          + '<br/>Location(s) details:<br/>'
                          + '<br/>'.join(not_processed_loc)
                          + '</font>'
                          if identified_loc > processed_loc else '')
                      + '<br/><br/>Number of processed manifest files (across all locations): <b>{}</b>'
                      .format(fls_processed_cnt)
                      + '<br/><br/>Processed Manifest\'s details:'
                      + '<br/><br/>'
                      + '<br/><br/>'.join(email_msgs)
                      )

        # print ('email_subject = {}'.format(email_subject))
        # print('email_body = {}'.format(email_body))

        mlog.info('Sending a status email with subject "{}" to "{}".'.format(email_subject, email_to))

        try:
            if m_cfg.get_value('Email/send_emails'):
                email.send_yagmail(
                    emails_to=email_to,
                    subject=email_subject,
                    message=email_body
                    # commented adding attachements, since some log files go over 25GB limit and fail email sending
                    # ,attachment_path=email_attchms
                )
        except Exception as ex:
            # report unexpected error during sending emails to a log file and continue
            _str = 'Unexpected Error "{}" occurred during an attempt to send final email upon ' \
                   'finishing processing of manifests:\n{} ' \
                .format(ex, traceback.format_exc())
            mlog.critical(_str)

        mlog.info('End of processing of manifests.')

    except Exception as ex:
        # report unexpected error to log file
        _str = 'Unexpected Error "{}" occurred during processing file: {}\n{} ' \
            .format(ex, os.path.abspath(__file__), traceback.format_exc())
        mlog.critical(_str)
        raise

    deactivate_logger_common(mlog, mlog.handlers[0])
    sys.exit()
