import os
import shutil
import pdb

input_path = '/rdsi/market/access_opendap/201709'
output_path = '/rdsi/market/access_test/201709'

alias_dict = {'Arcturus': 'Emerald', 'Jaxa': 'Yanco'}

f_to_write_list = os.listdir(input_path)
f_written_list = os.listdir(output_path)
for f in f_to_write_list:
    site_parts = f.split('_')
    site_name = site_parts[0]
    try:
        site_parts[0] = alias_dict[site_name]
    except:
        pass
    new_f_name = '_'.join(site_parts)
    input_f = os.path.join(input_path, f)
    new_output_f = os.path.join(output_path, new_f_name)
    if not os.path.isfile(new_output_f):
        shutil.copy(input_f, new_output_f)    

