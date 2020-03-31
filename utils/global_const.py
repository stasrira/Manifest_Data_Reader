# ========== config file names
# main config file name
CONFIG_FILE_MAIN = 'configs/main_config.yaml'
CONFIG_FILE_DICTIONARY = 'configs/dict_config.yaml'
CONFIG_FILE_SOURCE_NAME = 'configs/source_config.yaml'

# PROJECT_NAME = 'ECHO'  # this key is stored in here instead of being passed from a inquiry.

# source level default name for the config file
DEFAULT_STUDY_CONFIG_FILE = 'source_config.yaml'

# name for the each type of log
MAIN_LOG_NAME = 'main_log'
INQUIRY_LOG_NAME = 'inquiry_processing_log'

# default folder names for logs and processed files

# following variables will be defined at the start of execution based on the config values from main_config.yaml
APP_LOG_DIR = ''  # path to the folder where all application level log files will be stored (one file per run)
INQUIRY_LOG_DIR = ''  # path to the folder where all log files for processing inquiry files will be stored
                          # (one file per inquiry)
INQUIRY_PROCESSED_DIR = ''  # path to the folder where all processed (and renamed) inquiries will be stored
DISQUALIFIED_INQUIRIES = '' # path to dir with dynamically created inquiry files for disqualified aliquots
OUTPUT_REQUESTS_DIR = ''  # path to the folder where all processed (and renamed) inquiries will be stored

DATA_DOWNLOADER_PATH = '' # path to the location of Data Downloader app, set based on the main config value

# the following 3 lines are to be removed
# SUBMISSION_PACKAGES_DIR = "submission_packages"
# LOG_FOLDER_NAME = 'logs'
# PROCESSED_FOLDER_NAME = 'processed'

# name of the sheet name in the inquiry file (excel file) where from data should be retrieved.
# If omitted, the first sheet in array of sheets will be used
INQUIRY_EXCEL_WK_SHEET_NAME = ''  # 'Submission_Request'

# default values for Study config file properties
# DEFAULT_CONFIG_VALUE_LIST_SEPARATOR = ','
# DEFAULT_REQUEST_SAMPLE_TYPE_SEPARATOR = '/'

ASSAY_CHARS_TO_REPLACE = [' ', '/', '\\']


# default study config file extension
# DEFAULT_STUDY_CONFIG_FILE_EXT = '.cfg.yaml'

# Excel processing related
# STUDY_EXCEL_WK_SHEET_NAME = 'wk_sheet_name'  # name of the worksheet name to be used for loading data from
