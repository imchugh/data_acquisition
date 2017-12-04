import os
import datetime as dt
import pdb

#------------------------------------------------------------------------------
# Get the date from a standard line of the BOM data
def get_date_from_line(line):
    
    line_list = line.split(',')
    return dt.datetime(int(line_list[2]), int(line_list[3]), 
                       int(line_list[4]), int(line_list[5]), 
                       int(line_list[6]))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def generate_date_list(start_date, end_date):
    
    if not start_date < end_date:
        raise Exception
    delta = end_date - start_date
    count = delta.days * 48 + delta.seconds / 1800 + 1
    return [start_date + dt.timedelta(minutes = i * 30) for
            i in range(count)]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def generate_dummy_line(valid_line, date):
    
    line_list = valid_line.split(',')
    start_list = ['dd', line_list[1]]
    date_list = [str(date.year), str(date.month).zfill(2), 
                 str(date.day).zfill(2), str(date.hour).zfill(2), 
                 str(date.minute).zfill(2)]
    data_list = [' ' * len(i) for i in line_list[7: -1]]
    dummy_list = start_list + date_list + data_list + [line_list[-1]]
    return ','.join(dummy_list)
#------------------------------------------------------------------------------

old_path = '/mnt/OzFlux/AWS/Current'
new_path = '/mnt/OzFlux/AWS/New/Converted'
out_path = '/mnt/OzFlux/AWS/New/Combined'

new_f_list = os.listdir(new_path)

for fname in new_f_list:

    if fname[0] == '.':
        continue

    old_fpname = os.path.join(old_path, fname)
    new_fpname = os.path.join(new_path, fname)
    out_fpname = os.path.join(out_path, fname)

    old_dict = {}
    new_dict = {}

    with open(old_fpname) as old_f:
        old_header = old_f.readline()
        for line in old_f:
            old_dict[get_date_from_line(line)] = line

    with open(new_fpname) as new_f:
        new_header = new_f.readline()
        for line in new_f:
            new_dict[get_date_from_line(line)] = line

    date_list = generate_date_list(sorted(old_dict.keys())[0],
                                   sorted(new_dict.keys())[-1])

    if not old_header == new_header:
        raise Exception('Headers dont match!')

    with open(out_fpname, 'w') as out_f:
        out_f.write(old_header)
        for date in date_list:
            try:
                line = old_dict[date]
            except KeyError:
                try:
                    line = new_dict[date]
                except KeyError:
                    line = generate_dummy_line(line, date)
            out_f.write(line)

