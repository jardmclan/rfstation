import csv
import json
from concurrent.futures import ThreadPoolExecutor
import os
import subprocess
import re
import random
from time import sleep
from multiprocessing import Lock, Semaphore



def ingestion_handler(data, bash_file, meta_file, cleanup, retry, delay = 0):
    if retry < 0:
        raise Exception("Retry limit exceeded")

    def retry_failure_with_backoff():
        backoff = 0
        #if first failure backoff of 0.25-0.5 seconds
        if delay == 0:
            backoff = 0.25 + random.uniform(0, 0.25)
        #otherwise 2-3x current backoff
        else:
            backoff = delay * 2 + random.uniform(0, delay)
        #retry with one less retry remaining and current backoff
        return ingestion_handler(data, bash_file, meta_file, cleanup, retry - 1, backoff)

    sleep(delay)

    uuid = None
    with open(meta_file, "w") as f:
        json.dump(data, f)
    try:
        out = subprocess.check_output(["sh", bash_file])
    except CalledProcessError:
        uuid = retry_failure_with_backoff()
    #if success output should be "Successfully submitted metadata object <uuid>"
    match = re.fullmatch(r"Successfully submitted metadata object (.+)", out)
    if match is not None:
        uuid = match.group(1)
    else:
        uuid = retry_failure_with_backoff()
    if cleanup:
        os.remove(meta_file)
    return uuid
    

def main():
    
    basedata_f = "basedata.json"
    basedata = None
    with open(basedata_f, "r") as f:
        basedata = json.load(f)

    basedata["value"]["type"] = "station_metadata"

    trans = {
        "SKN": "skn",
        "Station.Name": "name",
        "OBSERVER,Network": "network",
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

    failure_lock = Lock()
    throttle = Semaphore(10)

    #reset failures log
    with open("failures.log", "w") as failure_log:
        failure_log.write("")

    skn_index = 0
    meta_file = "site_meta.csv"
    with open(meta_file, "r") as fd:
        reader = csv.reader(fd)
        header = None
        #threads>
        with ThreadPoolExecutor(3) as t_exec:
            row_num = 0
            complete = 0
            for row in reader:
                throttle.acquire()
                if header is None:
                    header = row
                else:
                    skn = row[skn_index]
                    data = basedata
                    for i in len(header):
                        header_field = header[i]
                        field_name = trans[header_field]
                        value = row[i]
                        data["value"][field_name] = value
                    f = t_exec.submit(ingestion_handler, data)
                    #wrapper since skn will change due to python's wonderful scoping
                    def cb(skn):
                        def _cb(f):
                            nonlocal complete
                            e = f.exception()
                            if e is not None:
                                #log failure
                                #cbs may be called from other thread, so lock file to be safe
                                with failure_lock:
                                    with open("failures.log", "a") as failure_log:
                                        failure_log.write("%s\n" % skn)
                            complete += 1
                            if complete % 100 == 0:
                                print("Completed %d docs" % complete)
                            throttle.release()
                        return _cb
                    f.add_done_callback(cb(skn))
        print("Complete!")



if __name__ == "__main__":
    main()