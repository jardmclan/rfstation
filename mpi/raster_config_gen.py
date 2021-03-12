import json
from sys import argv

#load config
if len(argv) < 2:
    raise RuntimeError("Invalid command line args. Must provide config file")
config_file = argv[1]
config = None
with open(config_file, "r") as f:
    config = json.load(f)


fbase = "/home/mcleanj/rfing/data/all_rf_data/allMonYrData/"

yr_range = [1990, 2020]
months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

data = config["raster_file_data"][0]["file_info"]

for yr in range(*yr_range):
    for month in months:
        date_s = "%d-%s" % (yr, month)
        folder = "%d_%s/" % (yr, month)
        fname = "%d_%s_statewide_rf_mm.tif" % (yr, month)
        fpath = "%s%s%s" % (fbase, folder, fname)
        file_info = {
            "date": date_s,
            "file": fpath
        }
        data.append(file_info)


with open("raster_config.json", "w") as f:
    json.dump(config, f, indent= 4)











