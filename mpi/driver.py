import csv
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import Semaphore, Manager
import re
import json
from copy import deepcopy
from ingestion_handler import ingestion_handler
from sys import stderr, argv
from os.path import join
from geotiff_data import GeotiffData




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

#everything needs version, so make global
#unique version number for the data set, should change with each run
version = config["version"]

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


def handle_geotiff(file, include_header):
    data = GeotiffData(file)
    if include_header:
        pass




#csv data should pass out metadata objects directly
#geotiff pass file, let processors read and package (larger data with more processing overhead)

#should separate station metadata and 

#note fill type SHOULD be a standard field, if just raw data it would just be unfilled

def distribute():

    ranks = comm.Get_size() - 1

    def send_info(info):
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
                comm.send(info, dest = recv_rank)


    ###################################################

    

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
    

    ###########################################################################

    for raster_file_data_item in raster_file_data:
        header_complete = False
        #for document format the main body should only have properties that are garenteed between data sets, everything else should be in the data portion
        #not everything might have fill type
        raster_classification = raster_file_data_item["classification"]
        raster_subclassification = raster_file_data_item["subclassification"]
        raster_period = raster_file_data_item["period"]
        raster_units = raster_file_data_item["units"]
        #include header id in case want extend to multiple headers later (change in resolution, spatial extent, etc), can use something like "hawaii_statewide_default" or something like that
        raster_header_id = raster_file_data_item["header_id"]
        include_header = raster_file_data_item["include_header"]

        raster_file_info = raster_file_data_item["raster_file_info"]
        for raster_file_info_item in raster_file_info:
            raster_file = raster_file_info_item["raster_file"]
            #this is the only thing that should change on a per file basis
            raster_date = raster_file_info_item["raster_date"]
            #any additional information unique to this set of rasters (non-standard fields), just set to null if there are none
            raster_ext = raster_file_info_item["ext_data"]


                

            header_complete = True

            #distribute info with file, type, and field data
            info = {
                "type": "raster",
                "version": version,
                "data": {
                    "header_id": raster_header_id,
                    "classification": raster_classification,
                    "subclassification": raster_subclassification,
                    "units": raster_units,
                    "period": raster_period,
                    "date": raster_date,
                    "ext" : raster_ext,
                    "include_header": include_header and not header_complete
                },
                "file": raster_file
            }
            
            send_info(info)

            #######
            
            raster_header_doc = {
                "name": doc_names["raster_header"],
                "version": version,
                "value": {
                    "id": raster_header_id
                    "data": None
                }
            }

            raster_doc = {
                "name": doc_names["raster"],
                "version": version,
                "value": {
                    "header_id": raster_header_id,
                    "classification": raster_classification,
                    "subclassification": raster_subclassification,
                    "units": raster_units,
                    "period": raster_period,
                    "date": {
                        "$date": raster_date
                    },
                    "ext" : raster_ext,
                    "data": None
                }
            }






    ###################################################


    for station_file_data_item in station_file_data:
        station_classification = station_file_data_item["classification"]
        station_subclassification = station_file_data_item["subclassification"]
        
        station_units = station_file_data_item["units"]

        #process metadata as a separate file, can just recycle one of the files if multiple
        #this way if the group has variable sets of stations it handles they can be stripped out and put into a separate file to avoid issues (or can just feed it one of the files if theyre all the same)
        #if no metadata just set this to null
        metadata_info = station_file_data_item["metadata_info"]
        #handle metadata item first
        #make metadata classification agnostic, there might be overlap between stations between classifications, all the metadata is pulled in beforehand so shouldn't matter (can change this if need, but should be fine, value docs have classification and subclass if available)
        #might want to add something to check if station skn already exists? worry about this later
        if metadata_info is not None:
            metadata_file = metadata_info["file"]
            metadata_cols = metadata_info["metadata_cols"]
            metadata_field_name_translation = metadata_info["field_name_translations"]
            #all info should be in the columns themselves, but allow extension data just in case
            metadata_ext = metadata_info["ext_data"]

            #have each classification, subclass have their own set of metadata, when this changes in the application it should pull the new set
            #potentially some duplication, but more extensible and less difficult to track, also should limit items stored by application to some extent
            #NOTE should actually make the subclass for this stuff 'new', then if decide to pull in station data for the legacy stuff later add in an additional set with the legacy stations
            #this will likely have a lot of duplication but don't worry about that (the metadata duplication should have minimal storage impact anyway)
            #can also use this to indicate no associated station data implicitly since returns no data rather than having it indicated by dataset in application
            #so application can assume always have both, and if there's no rainfall stations then it just won't have anything to display, same result with lower complexity

            info = {
                "type": "station_metadata",
                "data": {
                    "metadata_cols": metadata_cols,
                    "field_name_translations": metadata_field_name_translation,
                    "ext": metadata_ext
                },
                "file": metadata_file
            }

        station_file_info = station_file_data_item["station_file_info"]
        for station_file_info_item in station_file_info:

            station_skn_col = station_file_info_item["skn_col"]
            data_cols = station_file_info_item["data_cols"]

            station_fill = station_file_info_item["fill"]
            station_period = station_file_info_item["period"]

            info = {
                "type": "station_metadata",
                "data": {
                    "metadata_cols": metadata_cols,
                    "field_name_translations": metadata_field_name_translation,
                    "ext": metadata_ext
                },
                "file": metadata_file
            }





    ##################################################################


    

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