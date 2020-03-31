from pathlib import Path
import os
import glob
# from utils import common as cm
from lxml import html
import requests
import traceback


class DataRetrieval:

    def __init__(self, inquiry):
        # self.path_sub_aliqs = {}
        self.inq_obj = inquiry  # reference to the current inquiry object
        self.error = self.inq_obj.error
        self.logger = self.inq_obj.logger
        self.conf_process_entity = inquiry.conf_process_entity
        # self.data_loc = None
        self.init_specific_settings()

    def init_specific_settings(self):
        # should be overwritten in classed that inherit this one
        pass

    def get_data_by_folder_name(self, data_loc, search_deep_level, exclude_dirs):
        # retrieves data for each sub-aliquot listed in the inquiry file based on presence
        # of aliquot id key in the name of the folder
        disqualify =None
        dirs, disqualify = self.find_locations_by_folder(data_loc, search_deep_level, exclude_dirs)
        return dirs, disqualify

    def get_data_by_file_name(self, data_loc, search_deep_level, exclude_dirs, ext_match):
        # it retrieves all files potentially qualifying to be a source
        disqualify =None
        files = []
        if Path(data_loc).exists():
            files = self.get_file_system_items(data_loc, search_deep_level, exclude_dirs, 'file', ext_match)
        else:
            _str = 'Expected to exist directory "{}" is not present'.format(data_loc)
            _str2 = '- reported from "DataRetrieval" class, "get_data_by_file_name" function.'
            self.logger.warning(_str + _str2)
            disqualify = (data_loc, _str)
            # self.disqualify_source(data_loc, _str)
        return files, disqualify

    def find_locations_by_folder(self, loc_path, search_deep_level, exclude_dirs):
        disqualify =None
        # get directories of the top level and filter out any directories to be excluded
        dirs_top, disqualify = self.get_top_level_dirs(loc_path, exclude_dirs)
        dirs = []  # holds final list of directories
        dirs.extend(dirs_top)

        # if deeper than top level search is required, proceed here
        if search_deep_level > 0:
            for d in dirs_top:
                items = self.get_file_system_items(d, search_deep_level-1, exclude_dirs, 'dir')
                dirs.extend(items)

        return dirs, disqualify

    def get_top_level_dirs(self, path, exclude_dirs=None):
        disqualify =None
        if exclude_dirs is None:
            exclude_dirs = []
        if Path(path).exists():
            itr = os.walk(Path(path))
            _, dirs, _ = next(itr)
            if not dirs:
                dirs = []
            dirs = list(set(dirs) - set(exclude_dirs))  # remove folders to be excluded from the list of directories
            dirs_path = [str(Path(path + '/' + dr)) for dr in dirs]
        else:
            _str = 'Expected to exist directory "{}" is not present'.format (path)
            _str2 = '- reported from "DataRetrieval" class, "get_top_level_dirs" function.'
            # self.logger.warning('Expected to exist directory "{}" is not present - reported from "DataRetrieval" '
            #                    'class, "get_top_level_dirs" function'.format (path))
            self.logger.warning(_str + _str2)
            dusqualify = (path, _str)
            # self.disqualify_source(path, _str)
            dirs_path = []
        return dirs_path, dusqualify

    @staticmethod
    def get_file_system_items(dir_cur, search_deep_level, exclude_dirs=None, item_type='dir', ext_match=None):
        # base_loc = self.data_loc / dir_cur
        if ext_match is None:
            ext_match = []
        if exclude_dirs is None:
            exclude_dirs = []
        deep_cnt = 0
        cur_lev = ''
        items = []
        while deep_cnt <= search_deep_level:
            cur_lev = cur_lev + '/*'
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

    def get_web_data(self, url, str_xpath, exclude_entries=None, ext_match=None):
        # TODO: implement logic to check each link on the page to loop throug all possible trees until a max deep level
        items = []
        disqualify = None
        if not exclude_entries:
            exclude_entries = []
        try:
            page = requests.get(url)
        except Exception as ex:
            # report error during opening a web location
            _str = 'Unexpected Error "{}" occurred during opening web location'.format(ex)
            _str2 = ' at: "{}"\n{} '.format(url, traceback.format_exc())
            self.logger.critical(_str + _str2)
            # self.error.add_error(_str + _str2)
            # self.disqualify_source(url, _str)
            disqualify = (url, _str)
            return items, disqualify

        content = html.fromstring(page.content)

        try:
            entries = content.xpath(str_xpath)
        except Exception as ex:
            # report error at attempt of applying xpath
            _str = 'Unexpected Error "{}" occurred during applying xpath value "{}" against content'\
                .format(ex, str_xpath)
            _str2 = ' of URL "{}"\n{} '.format(url, traceback.format_exc())
            self.logger.critical(_str + _str2)
            # self.disqualify_source(url, _str)
            disqualify = (url, _str)
            # self.error.add_error(_str + _str2)
            return items, disqualify

        for entry in entries:
            if not entry in exclude_entries:
                # str_entry = str(entry)
                if (ext_match and str(entry).endswith(tuple(ext_match))) or (not ext_match):
                    items.append(entry)
                    # print (entry)
        return items, disqualify

    # def disqualify_source(self, source_path, reason):
    #    pass