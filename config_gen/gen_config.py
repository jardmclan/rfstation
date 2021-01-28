

import json
import os

config = None
with open("config_base.json", "r") as f:
    config = json.load(f)


metadata_field_translations = {
    "SKN": "skn",
    "Station.Name": "name",
    "OBSERVER": "observer",
    "Network": "network",
    "Island": "island",
    "ELEV.m.": "elevation_m",
    "LAT": "lat",
    "LON": "lng",
    "NCEI.id": "ncei_id",
    "NWS.id": "nws_id",
    "NESDIS.id": "nesdis_id",
    "SCAN.id": "scan_id",
    "SMART_NODE_RF.id": "smart_node_rf_id"
}

raster_file_data = config["raster_file_data"]
station_file_data = config["station_file_data"]


raster_file_data_item = {}

raster_file_data_item["header_id"] = "hawaii_statewide_250m"
raster_file_data_item["include_header"] = True
raster_file_info = []
raster_file_data_item["raster_file_info"] = raster_file_info

#populate raster_file_info
src_base_dir = "c://users/jard/downloads/all_rf_data/allMonYrData"
dest_base_dir = "/home/mcleanj/rfing/data/all_rf_data/allMonYrData"
contents = os.listdir(src_base_dir)
for dir_item in contents:
    path = os.path.join(src_base_dir, dir_item)
    if os.path.isdir(path):
        #dir names should be dates
        date_parts = dir_item.split("_")
        date = "%s-%s" % (date_parts[0], date_parts[1])
        fname = "%s_%s_statewide_rf_mm.tif" % (date_parts[0], date_parts[1])
        #construct path manually because of windows horrendous backslash custom
        fpath = "%s/%s/%s" % (dest_base_dir, dir_item, fname)

        raster_file_info_item = {}

        raster_file_info_item["classification"] = "rainfall"
        raster_file_info_item["subclassification"] = "new"
        raster_file_info_item["period"] = "month"
        raster_file_info_item["units"] = "mm"
        raster_file_info_item["raster_file"] = fpath
        raster_file_info_item["raster_date"] = date
        raster_file_info_item["ext_data"] = {}
        raster_file_info.append(raster_file_info_item)

raster_file_data.append(raster_file_data_item)



station_file_data_item = {}
station_file_data_item["classification"] = "rainfall"
station_file_data_item["subclassification"] = "new"
station_file_data_item["units"] = "mm"
metadata_info = {}
station_file_data_item["metadata_info"] = metadata_info
station_file_info = []
station_file_data_item["station_file_info"] = station_file_info


# #populate metadata_info
# metadata_file = "/home/mcleanj/rfing/data/all_rf_data/monthly_rf_new_data_1990_2020_FINAL_19dec2020.csv"
# #dest_base_dir = "/home/mcleanj/rfing/data/all_rf_data"
# metadata_info["file"] = metadata_file
# metadata_info["metadata_cols"] = [0, 14]
# metadata_info["field_name_translations"] = metadata_field_translations
# #all info should be in the columns themselves, but allow extension data just in case
# metadata_info["ext_data"] = {}

# #populate station_file_info

# station_file_info_item = {}
# station_file_info_item["skn_col"]
# station_file_info_item["data_col_start"]
# station_file_info_item["fill"]
# station_file_info_item["period"]
# station_file_info_item["ext_data"] = {}
# station_file_info_item["nodata"] = "NA"
# station_file_info.append(station_file_info_item)


# station_file_data.append(station_file_data_item)

with open("config.json", "w") as f:
    config = json.dump(config, f, indent = 4)
