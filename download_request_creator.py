from pathlib import Path
import sys
import os
from os import walk
import time
import traceback
from utils import setup_logger_common, deactivate_logger_common, common as cm
from utils import ConfigData
from utils import global_const as gc
from utils import send_email as email
from file_load import Inquiry

# if executed by itself, do the following
if __name__ == '__main__':

    # load main config file and get required values
    m_cfg = ConfigData(gc.CONFIG_FILE_MAIN)

    # print ('m_cfg = {}'.format(m_cfg.cfg))
    # assign values
    common_logger_name = gc.MAIN_LOG_NAME  # m_cfg.get_value('Logging/main_log_name')

    # get path configuration values
    logging_level = m_cfg.get_value('Logging/main_log_level')
    # path to the folder where all new inquiry files will be posted
    inquiries_loc = m_cfg.get_value('Location/inquiries')

    gc.DISQUALIFIED_INQUIRIES = m_cfg.get_value('Location/inquiries_disqualified')
    # get path configuration values and save them to global_const module
    # path to the folder where all application level log files will be stored (one file per run)
    gc.APP_LOG_DIR = m_cfg.get_value('Location/app_logs')
    # path to the folder where all log files for processing inquiry files will be stored
    # (one file per inquiry)
    gc.INQUIRY_LOG_DIR = m_cfg.get_value('Location/inquiry_logs_relative_path')
    # path to the folder where all processed (and renamed) inquiries will be stored
    gc.INQUIRY_PROCESSED_DIR = m_cfg.get_value('Location/inquiries_processed_relative_path')
    # path to the folder where created submission packages will be located. One package sub_folder per inquiry.
    gc.OUTPUT_REQUESTS_DIR = m_cfg.get_value('Location/output_requests')
    # path to dir with dynamically created inquiry files for disqualified aliquots
    gc.DISQUALIFIED_INQUIRIES =  m_cfg.get_value('Location/inquiries_disqualified_path')

    log_folder_name = gc.APP_LOG_DIR  # gc.LOG_FOLDER_NAME

    # this variable define if Data Downloader app will be executed at the end of processing inquiries
    run_data_download = m_cfg.get_value('Execute/run_data_downloader')
    # path to the Data Downloader tool
    gc.DATA_DOWNLOADER_PATH = m_cfg.get_value('Location/data_downloader_path')

    prj_wrkdir = os.path.dirname(os.path.abspath(__file__))

    email_msgs = []
    # email_attchms = []

    # inquiries_loc = 'E:/MounSinai/MoTrPac_API/ProgrammaticConnectivity/MountSinai_metadata_file_loader/DataFiles'
    inquiries_path = Path(inquiries_loc)

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

    mlog.info('Start processing download inquiries in "{}"'.format(inquiries_path))

    try:

        (root, source_inq_dirs, _) = next(walk(inquiries_path))

        # mlog.info('Download inquiries to be processed (count = {}): {}'.format(len(inquiries), inquiries))

        inq_proc_cnt = 0
        errors_present = 'OK'

        for inq_dir in source_inq_dirs:
            source_inquiry_path = Path(root) / inq_dir
            mlog.info('Selected for processing inquiry source: "{}", full path: {}'.format(inq_dir, source_inquiry_path))

            (_, _, inq_files) = next(walk(source_inquiry_path))
            inquiries = [fl for fl in inq_files if fl.endswith(('xlsx', 'xls'))]
            mlog.info('Inquiry files presented (count = {}): "{}"'.format(len(inquiries), inquiries))

            for inq_file in inquiries:
                inq_path = Path(source_inquiry_path) / inq_file

                # email_msgs = []
                # email_attchms = []

                try:
                    # print('--------->Process file {}'.format(inq_path))
                    mlog.info('The following Inquiry file was selected: "{}".'.format(inq_path))

                    # save timestamp of beginning of the file processing
                    ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())

                    inq_obj = Inquiry(inq_path)

                    if inq_obj and inq_obj.loaded:
                        # proceed processing inquiry
                        mlog.info('Inquiry file was successfully loaded.')
                        mlog.info('Starting processing Download Inquiry file: "{}".'.format(inq_path))

                        inq_obj.process_inquiry()

                        mlog.info('Processing of Download Inquiry was finished for {}'.format(inq_path))

                        inq_proc_cnt += 1

                    # identify if any errors were identified and set status variable accordingly
                    if not inq_obj.error.exist():
                        if not inq_obj.disqualified_items:
                            # no disqualified sub-aliquots present
                            fl_status = 'OK'
                            _str = 'Processing status: "{}". Download Inquiry: {}'.format(fl_status, inq_path)
                            # errors_present = 'OK'  # this variable is set to OK by default, no update needed
                        else:
                            # some disqualified sub-aliquots are presetn
                            fl_status = 'OK_with_Disqualifications'
                            _str = 'Processing status: "{}". Download Inquiry: {}'.format(fl_status, inq_path)
                            if not errors_present == 'ERROR':
                                errors_present = 'DISQUALIFY'
                    else:
                        fl_status = 'ERROR'
                        _str = 'Processing status: "{}". Check processing log file for this inquiry: {}' \
                            .format(fl_status, inq_obj.logger.handlers[0])
                        errors_present = 'ERROR'

                    if fl_status == "OK":
                        mlog.info(_str)
                    else:
                        mlog.warning(_str)

                    # deactivate the current Inquiry logger
                    deactivate_logger_common(inq_obj.logger, inq_obj.log_handler)

                    processed_dir = inq_obj.processed_folder  # 'Processed'
                    # if Processed folder does not exist in the Inquiry source sub-folder, it will be created
                    os.makedirs(processed_dir, exist_ok=True)

                    inq_processed_name = ts + '_' + fl_status + '_' + str(inq_file).replace(' ', '_').replace('__','_')
                    # print('New file name: {}'.format(ts + '_' + fl_status + '_' + fl))
                    # move processed files to Processed folder
                    os.rename(inq_path, processed_dir / inq_processed_name)
                    mlog.info('Processed Download Inquiry "{}" was moved and renamed as: "{}"'
                              .format(inq_path, processed_dir / inq_processed_name))

                    # preps for email notification

                    # TODO: move this step to a separate function
                    nbsp = 3
                    email_msgs.append(
                        ('Inquiry file (#{}): <br/>{} <br/> was processed and moved/renamed to: <br/> {}.'
                         '<br/> <b>Errors summary:</b><br/>{}'
                         '<br/> <i>Log file location: <br/>{}</i>'
                         '<br/> Created Download Request file locatoin:<br/>{}'
                         '<br/> <b>Data sources used for this inquiry:</b><br/>{}'
                         '<br/> <font color="green"><b>Processed Aliquots:</b></font><br/>{}'
                         '<br/> <b>Disqualified Aliquots</b> (if present, see the log file for more details):<br/>{}'
                         '<br/> A inquiry file for re-processing Disqualified Aliquots was saved in:<br/>{}'
                         ''.format(inq_proc_cnt,
                                    '&nbsp;'*nbsp + str(inq_path),
                                   '&nbsp;'*nbsp + str(processed_dir / inq_processed_name),
                                   '<font color="red">Check details for {} Error(s) in the log file </font>'
                                   .format(inq_obj.error.count)
                                                            if inq_obj.error.exist()
                                                            else '<font color="green">No Errors</font> ',
                                   '&nbsp;'*nbsp + inq_obj.log_handler.baseFilename,
                                   '&nbsp;'*nbsp + str(inq_obj.download_request_path),
                                   '<br>'.join(['&nbsp;'*nbsp + 'Status: {}'
                                               .format(('<font color="red">Disqualified</font> - {}<br>>{}'
                                                    .format(ds['path'],
                                                        '<br>'.join(inq_obj.data_sources.disqualified_data_sources[ds['path']]))
                                                            if ds['path'] in
                                                               inq_obj.data_sources.disqualified_data_sources.keys()
                                                            else '<font color="green">OK</font> - {}'
                                                                .format(ds['path'])))
                                                for ds in inq_obj.data_sources.source_locations
                                                ])
                                                if inq_obj.data_sources.source_locations else 'None',
                                   '<br>'.join(['{}{} ({})'.format('&nbsp;'*nbsp, item['sub-aliquot'], item['study']) +
                                        (' - {}{} match ->{}'
                                            .format(
                                                '<b> warning - ' if item['match_details']['match_type'] != 'exact'
                                                    else '<font color="green">',
                                                item['match_details']['match_type'],
                                                '</b> ' if item['match_details']['match_type'] != 'exact'
                                                    else '</font> '
                                                )
                                        )
                                                + '{} ({})'.format(item['source']['name'], item['source']['path'])
                                        for item in inq_obj.inq_match_arr
                                                ])
                                                            if inq_obj.inq_match_arr else 'None',
                                   '<br>'.join(['&nbsp;'*nbsp + val +
                                        ' - <font color="red">status: {}</font> Here is the inquiry entry for it: {}'
                                        .format(inq_obj.disqualified_items[val]['status'],
                                                inq_obj.disqualified_items[val]['inquiry_item'])
                                        for val in inq_obj.disqualified_items.keys()])
                                                        if inq_obj.disqualified_items else 'None',
                                   '&nbsp;'*nbsp + str(inq_obj.disqualified_inquiry_path)
                                                            if inq_obj.disqualified_items else 'N/A'
                                   )
                         )
                         
                    )

                    # email_attchms.append(inq_obj.log_handler.baseFilename)

                    # print ('email_msgs = {}'.format(email_msgs))

                    inq_obj = None

                except Exception as ex:
                    # report an error to log file and proceed to next file.
                    mlog.error('Error "{}" occurred during processing file: {}\n{} '
                               .format(ex, inq_path, traceback.format_exc()))
                    raise

        mlog.info('Number of successfully processed Inquiries = {}'.format(inq_proc_cnt))

        # start Data Download request if proper config setting was provided
        dd_status = {'status': '', 'message': ''}
        if run_data_download:
            # start process
            mlog.info('Starting asyncroniously Data Downloader app: "{}".'.format(gc.DATA_DOWNLOADER_PATH))
            try:
                dd_process = cm.start_external_process_async(gc.DATA_DOWNLOADER_PATH)
                # check if it is running
                dd_status = cm.check_external_process(dd_process)
                mlog.info('Status of running Data Downloader app: "{}".'.format(dd_status))
            except Exception as ex:
                # report unexpected error during starting Data Downloader
                _str = 'Unexpected Error "{}" occurred during an attempt to start Data Downloader app ({})\n{} ' \
                    .format(ex, gc.DATA_DOWNLOADER_PATH, traceback.format_exc())
                mlog.critical(_str)
                dd_status = {'status': 'Error', 'message': _str}

        mlog.info('Preparing to send notificatoin email.')

        email_to = m_cfg.get_value('Email/send_to_emails')
        email_subject = 'processing of download inquiry. '

        if inq_proc_cnt > 0:
            # collect final details and send email about this study results
            # email_subject = 'processing of Download Iquiries for "{}"'.format(gc.PROJECT_NAME)
            if errors_present == 'OK':
                email_subject = 'SUCCESSFUL ' + email_subject
            elif errors_present == 'DISQUALIFY':
                email_subject = 'SUCCESSFUL (with disqualifications) ' + email_subject
            else:
                email_subject = 'ERROR(s) present during ' + email_subject

            if dd_status and 'status' in dd_status.keys() and dd_status['status'].lower() == 'error':
                email_subject = email_subject + 'Errors starting Data Downloader.'

            email_body = ('Number of inquiries processed: {}.'.format(inq_proc_cnt)
                          + '<br/>Run Data Downloader setting was set to "{}"'.format(run_data_download)
                          + ('<br/>Data Downloader location: {}'.format(gc.DATA_DOWNLOADER_PATH) if run_data_download else '')
                            # TODO: change color of the status in the next line depending on the value
                          + ('<br/>Status of starting Data Downloader: {}'.format(dd_status['status']) if run_data_download else '')
                          + '<br/><br/>Processed Inquiry\'s details:'
                          + '<br/><br/>'
                          + '<br/><br/>'.join(email_msgs)
                          )

            # print ('email_subject = {}'.format(email_subject))
            # print('email_body = {}'.format(email_body))

            mlog.info('Sending a status email with subject "{}" to "{}".'.format(email_subject, email_to))

            try:
                if m_cfg.get_value('Email/send_emails'):
                    email.send_yagmail(
                        emails_to= email_to,
                        subject=email_subject,
                        message=email_body
                        # commented adding attachements, since some log files go over 25GB limit and fail email sending
                        # ,attachment_path=email_attchms
                    )
            except Exception as ex:
                # report unexpected error during sending emails to a log file and continue
                _str = 'Unexpected Error "{}" occurred during an attempt to send email upon ' \
                       'finishing processing "{}" study: {}\n{} ' \
                    .format(ex, inq_path, os.path.abspath(__file__), traceback.format_exc())
                mlog.critical(_str)

            mlog.info('End of processing of download inquiries in "{}".'.format(inquiries_path))

    except Exception as ex:
        # report unexpected error to log file
        _str = 'Unexpected Error "{}" occurred during processing file: {}\n{} ' \
            .format(ex, os.path.abspath(__file__), traceback.format_exc())
        mlog.critical(_str)
        raise

    sys.exit()
