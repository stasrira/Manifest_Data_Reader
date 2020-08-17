import yaml
from utils import common as cm


class ConfigData:

    def __init__(self, cfg_path):
        self.loaded = False

        if cm.file_exists(cfg_path):
            with open(cfg_path, 'r') as ymlfile:
                self.cfg = yaml.load(ymlfile)
            # self.prj_wrkdir = os.path.dirname(os.path.abspath(cfg_path))
            self.loaded = True
        else:
            self.cfg = None
            # self.prj_wrkdir = None

    def get_value(self, yaml_path, delim='/'):
        path_elems = yaml_path.split(delim)

        # loop through the path to get the required key
        val = self.cfg
        for el in path_elems:
            # make sure "val" is not None and continue checking if "el" is part of "val"
            if val and el in val:
                try:
                    val = val[el]
                except Exception:
                    val = None
                    break
            else:
                val = None

        return val

    def get_item_by_key(self, key_name):
        v = self.get_value(key_name)
        if v is not None:
            return str(self.get_value(key_name))
        else:
            return v

