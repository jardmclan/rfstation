import csv
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import Semaphore, Manager
import re
import json
from copy import deepcopy
from ingestion_handler import ingestion_handler
from sys import stderr, argv
from os.path import join


config_file = "./config.json"
config = None
if len(argv) > 1:
    config_file = argv[1]
with open(config_file, "r") as f:
    config = json.load(f)

basedata_f = config["basedata"]
data_file = config["data_file"]
bash_file = config["bash_file"]
failure_file = config["failure_file"]
processes = config["processes"]
threads = config["threads"]
cleanup = config["cleanup"]
retry = config["retry"]
outdir = config["output_dir"]
date_format = config["date_format"]
date_format = re.compile(date_format)

basedata = None
with open(basedata_f, "r") as f:
    basedata = json.load(f)





def handle_row(row, header, failure_lock):
    skn = row[0]
    with ThreadPoolExecutor(threads) as t_exec:
        for i in range(1, len(row)):
            date = header[i]
            value = row[i]
            data = deepcopy(basedata)
            data["value"]["skn"] = skn
            data["value"]["date"] = date
            data["value"]["value"] = value
            meta_file = "%s_doc_%d.json" % (skn, i)
            meta_file = join(outdir, meta_file)
            f = t_exec.submit(ingestion_handler, data, bash_file, meta_file, cleanup, retry)
            def cb(date):
                def _cb(f):
                    #print(f.result())
                    e = f.exception()
                    if e is not None:
                        print(e, file = stderr)
                        #log failure
                        #cbs may be called from other thread, so lock file to be safe
                        with failure_lock:
                            with open(failure_file, "a") as failure_log:
                                failure_log.write("%s,%s\n" % (skn, date))
                return _cb
            f.add_done_callback(cb(date))
    print("Completed skn %s" % skn)




# XYYYY.MM.DD
def parse_date(date):
    match = re.match(date_format, date)
    if match is None:
        raise ValueError("Invalid date.")
    year = match.group(1)
    month = match.group(2)
    day = match.group(3)
    #note don't need time
    parsed_date = "%s-%s-%s" % (year, month, day) 
    return parsed_date


def main():
    #init failure log
    with open(failure_file, "w") as failure_log:
        failure_log.write("skn,date\n")
    manager = Manager()
    #need managed lock since interprocess, note this generates an additional process
    failure_lock = manager.Lock()
    throttle_limit = processes + 10
    throttle = Semaphore(throttle_limit)
    with ProcessPoolExecutor(processes) as p_exec:
        with open(data_file, "r") as fd:
            reader = csv.reader(fd)
            header = None
            for row in reader:
                if header is None:
                    header = row
                    #translate date header fields to proper date format
                    #first item should be skn
                    for i in range(1, len(row)):
                        date = row[i]
                        parsed_date = parse_date(date)
                        header[i] = parsed_date
                else:
                    throttle.acquire()
                    f = p_exec.submit(handle_row, row, header, failure_lock)
                    def cb(f):
                        e = f.exception()
                        if e is not None:
                            print(e, file = stderr)
                        throttle.release()
                    f.add_done_callback(cb)
    print("Complete!")

                    





if __name__ == "__main__":
    main()