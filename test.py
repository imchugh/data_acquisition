import pdb
import xlrd

master_file_pathname = '/mnt/OzFlux/Sites/site_master.xls'

wb = xlrd.open_workbook(master_file_pathname)
sheet = wb.sheet_by_name("Active")
xl_row = 10
bom_sites_info = {}
pdb.set_trace()
for n in range(xl_row,sheet.nrows):
    xlrow = sheet.row_values(n)
    ozflux_site_name = str(xlrow[0])
    bom_sites_info[ozflux_site_name] = {}
    bom_sites_info[ozflux_site_name]["latitude"] = xlrow[4]
    bom_sites_info[ozflux_site_name]["longitude"] = xlrow[5]
    bom_sites_info[ozflux_site_name]["elevation"] = xlrow[6]
    for i in [9,16,23,30]:
        if xlrow[i]!="":
            bom_id = str(int(xlrow[i+1]))
            bom_sites_info[ozflux_site_name][bom_id] = {}
            bom_sites_info[ozflux_site_name][bom_id]["site_name"] = xlrow[i]
            bom_sites_info[ozflux_site_name][bom_id]["latitude"] = xlrow[i+3]
            bom_sites_info[ozflux_site_name][bom_id]["longitude"] = xlrow[i+4]
            bom_sites_info[ozflux_site_name][bom_id]["elevation"] = xlrow[i+5]
            bom_sites_info[ozflux_site_name][bom_id]["distance"] = xlrow[i+6]

pdb.set_trace()
