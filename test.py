import pdb
import xlrd

master_file_pathname = '/home/ian/Temp/site_master.xls'

wb = xlrd.open_workbook(master_file_pathname)
sheet = wb.sheet_by_name("Active")
header_row = 9
start_row = 10
header_list = sheet.row_values(header_row)
alias_dict = {'Name': 'site_name',
              'Altitude': 'elevation'}
bom_sites_info = {}
for row_num in range(start_row, sheet.nrows):
    this_row = sheet.row_values(row_num)
    ozflux_site_name = this_row[header_list.index('Site')]
    bom_sites_info[ozflux_site_name] = {}
    for var in ['Latitude', 'Longitude', 'Altitude']:
        var_col_idx = header_list.index(var)
        try:
            var = alias_dict[var]
        except:
            pass
        bom_sites_info[ozflux_site_name][var.lower()] = this_row[var_col_idx]
    for index in range(1, 5):
        stn_col_idx = header_list.index('BoM_ID_{}'.format(str(index)))
        try:
            bom_id = str(int(this_row[stn_col_idx]))
            bom_sites_info[ozflux_site_name][bom_id] = {}
            for var in ['Latitude', 'Longitude', 'Altitude', 
                        'Distance', 'Name']:
                col_idx = header_list.index('{}_{}'.format(var, str(index)))
                try:
                    var = alias_dict[var]
                except KeyError:
                    pass
                bom_sites_info[ozflux_site_name][bom_id][var.lower()] = this_row[col_idx]
        except:
            continue
    
#bom_sites_info_ = {}
#for n in range(start_row,sheet.nrows):
#    xlrow = sheet.row_values(n)
#    ozflux_site_name = str(xlrow[0])
#    bom_sites_info_[ozflux_site_name] = {}
#    bom_sites_info_[ozflux_site_name]["latitude"] = xlrow[4]
#    bom_sites_info_[ozflux_site_name]["longitude"] = xlrow[5]
#    bom_sites_info_[ozflux_site_name]["elevation"] = xlrow[6]
#    for i in [9,16,23,30]:
#        if xlrow[i]!="":
#            bom_id = str(int(xlrow[i+1]))
#            bom_sites_info_[ozflux_site_name][bom_id] = {}
#            bom_sites_info_[ozflux_site_name][bom_id]["site_name"] = xlrow[i]
#            bom_sites_info_[ozflux_site_name][bom_id]["latitude"] = xlrow[i+3]
#            bom_sites_info_[ozflux_site_name][bom_id]["longitude"] = xlrow[i+4]
#            bom_sites_info_[ozflux_site_name][bom_id]["elevation"] = xlrow[i+5]
#            bom_sites_info_[ozflux_site_name][bom_id]["distance"] = xlrow[i+6]