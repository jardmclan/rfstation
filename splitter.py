import csv

fname = "daily_rf_data_2019_11_27.csv"
split = 13
skn_i = 0

meta_file = open("site_meta.csv", "w", newline="")
meta_writer = csv.writer(meta_file)
vals_file = open("site_vals.csv", "w", newline="")
vals_writer = csv.writer(vals_file)
with open(fname, "r") as f:
    reader = csv.reader(f)
    for row in reader:
        meta = row[:split]
        vals = [row[skn_i]] + row[split:]
        meta_writer.writerow(meta)
        vals_writer.writerow(vals)

meta_file.close()
vals_file.close()

