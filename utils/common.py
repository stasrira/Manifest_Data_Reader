from pathlib import Path


def get_project_root():
    # Returns project root folder.
    return Path(__file__).parent.parent


def file_exists(fn):
    try:
        with open(fn, "r"):
            return 1
    except IOError:
        return 0

def is_excel(file_path):
    ext = Path(file_path).suffix
    if 'xls' in ext:
        return True
    else:
        return False

def identify_delimeter_by_file_extension(file_path):
    ext = Path(file_path).suffix
    out_value = None

    if ext == 'csv':
        out_value = ','
    elif ext == 'tab':
        out_value = '   '
    elif 'xls' in ext:
        out_value = None
    else:
        out_value = ','

    return  out_value

def start_external_process_async (exec_path):
    from subprocess import Popen
    process = Popen(exec_path, shell=True)
    return process

def check_external_process(process):
    pr_out = process.poll()
    if pr_out is None:
        status = 'running'
        message = ''
    else:
        status = 'stopped'
        message = pr_out
    out = {'status': status, 'message': message}
    return out

def get_file_system_items_old(dir_cur, search_deep_level, exclude_dirs=None, item_type='dir', ext_match=None):
    import glob
    import os

    # base_loc = self.data_loc / dir_cur
    if ext_match is None:
        ext_match = []
    if exclude_dirs is None:
        exclude_dirs = []
    deep_cnt = 0
    cur_lev = ''
    items = []
    cur_lev = 'SampleManifests'
    while deep_cnt <= search_deep_level:
        cur_lev = '/*' + cur_lev # '/*'
        items_cur = glob.glob(str(Path(str(dir_cur) + cur_lev)))

        if item_type == 'dir':
            items_clean = [fn for fn in items_cur if
                           Path.is_dir(Path(fn)) and not os.path.basename(fn) in exclude_dirs]
        elif item_type == 'file':
            items_clean = []
            for ext in ext_match:
                items_found = [fn for fn in items_cur if not Path.is_dir(Path(fn))
                               and (len(ext_match) == 0 or fn.endswith(ext))]
                                # and (len(ext_match) == 0 or os.path.splitext(fn)[1] in ext_match)]
                if items_found:
                    items_clean.extend(items_found)
        else:
            items_clean = None
        items.extend(items_clean)
        deep_cnt += 1
    return items


def get_file_system_items_global(dir_cur, item_type='dir', match_pattern=None):
    import glob
    import os

    # base_loc = self.data_loc / dir_cur
    if match_pattern is None:
        match_pattern = '*'
    items = []

    search_pattern = str(Path(str(dir_cur) + '/**/' + match_pattern)) # '/**/SampleManifests'
    items_cur = glob.glob(search_pattern, recursive = True)

    if item_type == 'dir':
        items_clean = [fn for fn in items_cur if Path.is_dir(Path(fn)) ]
    elif item_type == 'file':
        items_clean = [fn for fn in items_cur if not Path.is_dir(Path(fn))]

    else:
        items_clean = None
    items.extend(items_clean)

    return items