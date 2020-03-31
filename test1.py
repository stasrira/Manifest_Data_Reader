from utils import common as cm

# if executed by itself, do the following
if __name__ == '__main__':
    # print('Test1')
    start_dir = 'D:\MounSinai\Darpa\Programming\Manifest_Data_Reader\datasource_examples' # 'J:\Projects\ECHO' # 
    # items = cm.get_file_system_items(start_dir, 12)
    items = cm.get_file_system_items_global(start_dir, 'dir', 'SampleManifests')
    # items = cm.get_file_system_items_global(start_dir, 'file', 'SampleManifests/manifest_config.yaml')
    print(items)

