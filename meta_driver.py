import csv
import json
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Lock, Semaphore, cpu_count
from ingestion_handler import ingestion_handler
    

def main():
    
    basedata_f = "basedata_meta.json"
    basedata = None
    with open(basedata_f, "r") as f:
        basedata = json.load(f)

    trans = {
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

    failure_lock = Lock()
    throttle = Semaphore(100)

    #create/reset failures log
    with open("failures.log", "w") as failure_log:
        failure_log.write("")

    skn_index = 0
    meta_file = "site_meta.csv"
    with open(meta_file, "r") as fd:
        reader = csv.reader(fd)
        header = None
        threads = cpu_count()
        with ThreadPoolExecutor(threads) as t_exec:
            row_num = 0
            complete = 0
            for row in reader:
                if header is None:
                    header = row
                else:
                    throttle.acquire()
                    skn = row[skn_index]
                    data = dict(basedata)
                    for i in range(len(header)):
                        header_field = header[i]
                        field_name = trans[header_field]
                        value = row[i]
                        data["value"][field_name] = value
                    doc_file_name = "./output/meta_%d.json" % row_num
                    f = t_exec.submit(ingestion_handler, data, "./bin/add_meta.sh", doc_file_name, True, 5)
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
                row_num += 1
        print("Complete!")



if __name__ == "__main__":
    main()