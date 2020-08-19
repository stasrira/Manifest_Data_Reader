# ========== config file names
# main config file name
CONFIG_FILE_MAIN = 'configs/main_config.yaml'

# name for the each type of log
MAIN_LOG_NAME = 'main_log'

# following variables will be defined at the start of execution based on the config values from main_config.yaml
APP_LOG_DIR = ''  # path to the folder where all application level log files will be stored (one file per run)
PROCESSED_FOLDER_NAME = 'processed'
PROCESSED_FOLDER_MAX_FILE_COPIES = 20  # reflects number of copies allowed in addition to the file itself, i.e. 'abc.xlsx' and its copies 'abc(1).xlsx', etc.

# stores main config database configuration settings
DB_CONFIG = None
