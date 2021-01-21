import csv
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import Semaphore, Manager
import re
import json
from copy import deepcopy
from ingestion_handler import ingestion_handler
from sys import stderr, argv
from os.path import join





##########################################

import mpi4py
mpi4py.rc.recv_mprobe = False

from mpi4py import MPI

comm = MPI.COMM_WORLD


##########################################

distributor_rank = 0

##########################################

#process rank
rank = comm.Get_rank()
processor_name = MPI.Get_processor_name()

##########################################

#remember $date IS necessary


# XYYYY.MM.DD
def parse_date(date, period):
    parsed_date = None
    pattern = None
    if period == "day":
        pattern = "^X([0-9]{4})\.([0-9]{2})\.([0-9]{2})$"
    elif period == "month":
        pattern = "^X([0-9]{4})\.([0-9]{2})$"
    else:
        raise ValueError("Unknown period.")
    match = re.match(pattern, date)
    if match is None:
        raise ValueError("Invalid date.")
    if period == "day":
        year = match.group(1)
        month = match.group(2)
        day = match.group(3)
        #note don't need time
        parsed_date = "%s-%s-%s" % (year, month, day)
    elif period == "month":
        year = match.group(1)
        month = match.group(2)
        #note don't need time
        parsed_date = "%s-%s" % (year, month)
    
    return parsed_date




#csv data should pass out metadata objects directly
#geotiff pass file, let processors read and package (larger data with more processing overhead)

#should separate station metadata and 

def distribute():

    ###################################################

    #unique version number for the data set, should change with each run
    version = config["version"]

    raster_file_data = config["raster_file_data"]
    station_file_data = config["station_file_data"]

    #document names should not change, broad identifier of document type
    #5 potential douments
    doc_names = {
        "raster_header": "hcdp_raster_header",
        "raster": "hcdp_raster",
        "value": "hcdp_station_value",
        "active_range": "hcdp_station_active_range",
        "metadata": "hcdp_station_metadata"
    }
    

    for raster_file_data_item in raster_file_data:
        raster_file_classification = raster_file_data_item["classification"]
        raster_file_subclassification = raster_file_data_item["subclassification"]
        raster_file_units = raster_file_data_item["units"]
        #note this should go in the header doc, serves as a string tag for the spatial extent (should always be statewide, but leave just in case, if this changes a new header is required)
        raster_spatial_extent = raster_file_data_item["extent"]
        include_header = raster_file_data_item["include_header"]





    ###################################################




    ranks = comm.Get_size() - 1

    def send_doc(doc):
        recv_rank = -1
        #get next request for data (continue until receive request or all ranks error out and send -1)
        while recv_rank == -1 and ranks > 0:
            #receive data requests from ranks
            recv_rank = comm.recv()
            #if recv -1 one of the ranks errored out, subtract from processor ranks (won't be requesting any more data)
            if recv_rank == -1:
                ranks -= 1
            #otherwise send data chunk to the rank that requested data
            else:
                comm.send(doc, dest = recv_rank)

    #migrate to config
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


    #temp, fill in actual info please (config?)
    metadata_columns = 10
    meta_file = ""
    values_file = ""
    id_col = "SKN"
    nodata = "NA"
    classification = "rainfall"

    #also need fill types

    #note the range is inclusive at both ends
    def get_active_range(data, dates):
        active_range = None
        start = None
        end = None
        for i in len(row):
            item = row[i]
            if item == nodata:
                end = i
                if start is None:
                    start = i
        if start is not None:
            active_range = [dates[start], dates[end]]
        return active_range

    def wrap_date(date):
        return {
            "$date": date
        }

    if meta_file is not None:
        with open(meta_file, "r") as fd:
            reader = csv.reader(fd)
            header = None
            for row in reader:
                if header is None:
                    header = row
                else:
                    value = {}
                    for i in range(len(row)):
                        col = header[i]
                        col_trans = trans[col]
                        item = row[i]
                        value[col_trans] = item
                    #set subclass to null
                    meta_doc = {
                        "classification": classification,
                        "value": value
                    }
                    send_doc(meta_doc)


    if values_file is not None:
        #this is for the values document, not metadata file should be handled separately
        #single initial column should be skn
        with open(values_file, "r") as fd:
            reader = csv.reader(fd)
            header = None
            dates = None
            for row in reader:
                if header is None:
                    header = row
                    #
                    dates = row[1:]
                else:
                    skn = row[0]
                    data = row[1:]
                    active_range = get_active_range(data, dates)

                    active_range_doc = {
                        "skn": skn,
                        "active_range": {
                            "start": wrap_date(active_range[0]),
                            "end": wrap_date(active_range[1])
                        }
                    }

                    #send off active range doc
                    send_doc(active_range_doc)


                    for value in data:
                        value_doc = {
                            "classification": classification,
                            "subclassification": None,
                            "skn": skn,
                            "unit": "mm",
                            "granularity": "monthly",
                            "value": value
                        }
                        send_doc(value_doc)

    while ranks > 0:
        recv_rank = comm.recv()
        #send terminator
        comm.send(None, dest = recv_rank)
        #reduce number of ranks that haven't received terminator
        ranks -= 1
    print("Complete!")







    

#three separate objects, one with active date range
#put time series granularities into metadata so know what types of value docs are available

def data_handler():
    i = 0
    try:
        #send rank to request data
        data = comm.sendrecv(rank, dest = distributor_rank)
        #process data and request more until terminator received from distributor
        while data is not None:
            out_name = "data_%d_%d" % (rank, i)
            i += 1
            try:
                ingestion_handler(data, bash_file, out_name, cleanup, retry)
            except Exception as e:
                pass
                    
            data = comm.sendrecv(rank, dest = distributor_rank)
            print("Rank %d received terminator. Exiting data handler..." % rank)
    except Exception as e:
        print("An error has occured in rank %d while handling data: %s" % (rank, e), file = stderr)
        print("Rank %d encountered an error. Exiting data handler..." % rank)
        #notify the distributor that one of the ranks failed and will not be requesting more data by sending -1
        comm.send(-1, dest = distributor_rank)


##########################################


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