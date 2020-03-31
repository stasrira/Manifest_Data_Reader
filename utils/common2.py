from utils import global_const as gc
from utils import ConfigData


def convert_sub_aliq_to_aliquot(sa, assay):
    aliquot = sa
    fl_cfg_dict = ConfigData(gc.CONFIG_FILE_DICTIONARY)
    assay = fl_cfg_dict.get_item_by_key('assay/' + assay.lower())
    if assay is not None:
        assay_postfix = fl_cfg_dict.get_item_by_key('assay_sub_aliquot_postfix/' + assay)
        if assay_postfix is not None:
            apf_len = len(assay_postfix)
            if sa[-apf_len:] == assay_postfix:
                aliquot = sa[:len(sa) - apf_len]
    return aliquot


# get value for the given key from dict_config.yaml file
def get_dict_value(key, section):
    fl_cfg_dict = ConfigData(gc.CONFIG_FILE_DICTIONARY)
    # replace spaces and slashes with "_"
    key = replace_unacceptable_chars(key, gc.ASSAY_CHARS_TO_REPLACE)
    try:
        v = fl_cfg_dict.get_item_by_key(section + "/" + key)
        if v is not None:
            return v
        else:
            return key
    except Exception:
        return key


# checks if provided key exists in dict_config.yaml file
def key_exists_in_dict(key, section):
    fl_cfg_dict = ConfigData(gc.CONFIG_FILE_DICTIONARY)
    key = replace_unacceptable_chars(key, gc.ASSAY_CHARS_TO_REPLACE)
    try:
        v = fl_cfg_dict.get_item_by_key(section + "/" + key)
        if v is not None:
            return True
        else:
            return False
    except Exception:
        return False

def replace_unacceptable_chars (val, bad_chars = None):
    val_loc = str(val)
    if not bad_chars:
        bad_chars = ['/', ' ']
    # replace not allowed characters with "_"
    for ch in bad_chars:
        val_loc = val_loc.replace(ch, '_')
    # remove repeating "_" symbols
    while '__' in val_loc:
        val_loc = val_loc.replace('__', '_')
    return val_loc