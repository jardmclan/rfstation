import csv
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import Semaphore, Manager
import re
import json
from copy import deepcopy
from ingestion_handler import ingestion_handler
from sys import stderr

date_format = re.compile("X([0-9]{4}).([0-9]{2}).([0-9]{2})")

basedata_f = "basedata_meta.json"
basedata = None
with open(basedata_f, "r") as f:
    basedata = json.load(f)

data_file = "site_vals.csv"
bash_file = "../bin/add_meta.sh"
failure_file = "./failures.log"
processes = 2
threads = 2
cleanup = True
retry = 5



def handle_row(row, header, failure_lock):
    skn = row[0]
    with ThreadPoolExecutor(threads) as t_exec:
        complete = 0
        for i in range(1, len(row)):
            date = header[i]
            value = row[i]
            data = deepcopy(basedata)
            data["value"]["skn"] = skn
            data["value"]["date"] = date
            data["value"]["value"] = value
            meta_file = "doc_%d.json" % i
            f = t_exec.submit(ingestion_handler, data, bash_file, meta_file, cleanup, retry)
            def cb(date):
                def _cb(f):
                    nonlocal complete
                    e = f.exception()
                    if e is not None:
                        #log failure
                        #cbs may be called from other thread, so lock file to be safe
                        with failure_lock:
                            with open(failure_file, "a") as failure_log:
                                failure_log.write("%s,%s\n" % (skn, date))
                    complete += 1
                    if complete % 100 == 0:
                        print("Completed %d docs" % complete)
                    throttle.release()
                return _cb
            f.add_done_callback(cb(skn))




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
                    def cb(skn):
                        def _cb(f):
                            e = f.exception()
                            if e is None:
                                print(e, file = stderr)
                            throttle.release()
                            print("Completed skn %s" % skn)
                        return _cb
                    f.add_done_callback(cb(row[0]))

                    





if __name__ == "__main__":
    main()