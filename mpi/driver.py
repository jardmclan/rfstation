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
from ingestion_handler import ingestion_handler



##########################################

import mpi4py
mpi4py.rc.recv_mprobe = False

from mpi4py import MPI

comm = MPI.COMM_WORLD


##########################################

distributor_rank = 0

#load config
if len(argv) < 2:
    raise RuntimeError("Invalid command line args. Must provide config file")
config_file = argv[1]
config = None
with open(config_file) as f:
    config = json.load(f)

#everything needs version, so make global
#unique version number for the data set, should change with each run
version = config["version"]

##########################################

#process rank
rank = comm.Get_rank()
processor_name = MPI.Get_processor_name()
doc_num = 0

##########################################


#remember $date IS necessary


def get_doc_name(doc_type):
    doc_names = config["doc_names"]
    return doc_names[doc_type]

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



#data, bash_file, meta_file, cleanup, retry, delay = 0
def send_doc(doc):
    bash_file = config["bash_file"]
    outdir = config["outdir"]
    cleanup = config["cleanup"]
    retry = config["retry"]

    meta_file = "doc_%d_%d" % (doc_num, rank)
    doc_num += 1
    meta_file = join(outdir, meta_file)
    
    ingestion_handler(doc, bash_file, meta_file, cleanup, retry)


def handle_station_metadata(file, data):
    classification = data["classification"]
    subclassification = data["subclassification"]
    metadata_cols = data["metadata_cols"]
    field_name_translations = data["field_name_translations"]
    units = data["units"]
    ext = data["ext"]

    
    with open(file, "r") as fd:
        reader = csv.reader(fd)
        header = None
        for row in reader:
            if header is None:
                header = row[metadata_cols[0], metadata_cols[1]]
            else:
                value = {}
                for i in range(metadata_cols[0], metadata_cols[1]):
                    col = header[i]
                    col_trans = field_name_translations[col]
                    item = row[i]
                    value[col_trans] = item
                doc_name = get_doc_name("station_metadata")
                #set subclass to null
                meta_doc = {
                    "name": doc_name,
                    "version": version,
                    "value": {
                        "classification": classification,
                        "subclassification": subclassification,
                        "units": units,
                        "ext": ext,
                        "data": value
                    }
                }
                send_doc(meta_doc)





#note the range is inclusive at both ends
def get_active_range(data, nodata, dates):
    active_range = None
    start = None
    end = None
    for i in len(data):
        item = data[i]
        if item != nodata:
            end = i
            if start is None:
                start = i
    if start is not None:
        active_range = [dates[start], dates[end]]
    return active_range

def handle_station_values(file, data):
    classification = data["classification"]
    subclassification = data["subclassification"]
    data_col_start = data["data_col_start"]
    fill = data["fill"]
    period = data["period"]
    skn_col = data["skn_col"]
    ext = data["ext"]
    nodata = data["nodata"]

    #this is for the values document, not metadata file should be handled separately
    #single initial column should be skn
    with open(file, "r") as fd:
        reader = csv.reader(fd)
        dates = None
        for row in reader:
            if dates is None:
                dates = row[data_col_start[0]]
                #transform dates
                for i in range(len(dates)):
                    dates[i] = parse_date(dates[i], period)
            else:
                skn = row[skn_col]
                values = row[data_col_start[0]]
                active_range = get_active_range(values, nodata, dates)
                doc_name = get_doc_name("active_range")
                active_range_doc = {
                    "name": doc_name,
                    "version": version,
                    "value": {
                        "skn": skn,
                        "active_range": {
                            "start": {
                                "$date:": active_range[0]
                            },
                            "end": { 
                                "$date": active_range[1]
                            }
                        }
                    }
                }

                #send off active range doc
                send_doc(active_range_doc)

                doc_name = get_doc_name("station_value")
                for i in range(len(values)):
                    value = values[i]
                    if value != nodata:
                        value_doc = {
                            "name": doc_name,
                            "version": version,
                            "value": {
                                "classification": classification,
                                "subclassification": subclassification,
                                "skn": skn,
                                "period": period,
                                "date": {
                                    "$date": dates[i]
                                },
                                "ext": ext,
                                "value": value
                            }
                        }
                        send_doc(value_doc)



def handle_geotiff(file, data):
    header_id = data["header_id"]
    classification = data["classification"]
    subclassification = data["subclassification"]
    units = data["units"]
    period = data["period"]
    date = data["date"]
    ext = data["ext"]
    include_header =  data["include_header"]

    geotiff_data = GeotiffData(file)

    if include_header:
        doc_name = get_doc_name("raster_header")
        raster_header_doc = {
            "name": doc_name,
            "version": version,
            "value": {
                "id": header_id,
                "data": geotiff_data.header
            }
        }

    doc_name = get_doc_name("raster")
    raster_doc = {
        "name": doc_name,
        "version": version,
        "value": {
            "header_id": header_id,
            "classification": classification,
            "subclassification": subclassification,
            "units": units,
            "period": period,
            "date": {
                "$date": date
            },
            "ext" : ext,
            "data": geotiff_data.data
        }
    }



def handle_info():
    
    try:
        #send rank to request data
        info = comm.sendrecv(rank, dest = distributor_rank)
        #process data and request more until terminator received from distributor
        while info is not None:
            try:
                file = info["file"]
                data = info["data"]
                #three types
                if info.type == "raster":
                    handle_geotiff(file, data)
                elif info.type == "station_vals":
                    handle_station_values(file, data)
                elif info.type == "station_metadata":
                    handle_station_metadata(file, data)
                else:
                    raise RuntimeError("Unknown document type.")
            except Exception as e:
                pass
                    
            data = comm.sendrecv(rank, dest = distributor_rank)
            print("Rank %d received terminator. Exiting data handler..." % rank)
    except Exception as e:
        print("An error has occured in rank %d while handling data: %s" % (rank, e), file = stderr)
        print("Rank %d encountered an error. Exiting data handler..." % rank)
        #notify the distributor that one of the ranks failed and will not be requesting more data by sending -1
        comm.send(-1, dest = distributor_rank)




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
        #note that items should not include header if already added
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

            #put units in metadata, should be uniform for class/subclass (station metadata set)
            info = {
                "type": "station_metadata",
                "data": {
                    "classification": station_classification,
                    "subclassification": station_subclassification,
                    "metadata_cols": metadata_cols,
                    "field_name_translations": metadata_field_name_translation,
                    "units": station_units,
                    "ext": metadata_ext
                },
                "file": metadata_file
            }

        station_file_info = station_file_data_item["station_file_info"]
        for station_file_info_item in station_file_info:

            station_skn_col = station_file_info_item["skn_col"]
            data_col_start = station_file_info_item["data_col_start"]

            station_fill = station_file_info_item["fill"]
            station_period = station_file_info_item["period"]
            station_val_ext = station_file_info_item["ext_data"]
            station_file_nodata = station_file_info_item["nodata"]

            info = {
                "type": "station_vals",
                "data": {
                    "classification": station_classification,
                    "subclassification": station_subclassification,
                    "data_col_start": data_col_start,
                    "fill": station_fill,
                    "period": station_period,
                    "skn_col": station_skn_col,
                    "nodata": station_file_nodata,
                    "ext": station_val_ext
                },
                "file": metadata_file
            }

    while ranks > 0:
        recv_rank = comm.recv()
        #send terminator
        comm.send(None, dest = recv_rank)
        #reduce number of ranks that haven't received terminator
        ranks -= 1
    print("Complete!")



    


##########################################


if rank == distributor_rank:
    print("Starting distributor, rank: %d, node: %s" % (rank, processor_name))
    distribute()
else:
    print("Starting data handler, rank: %d, node: %s" % (rank, processor_name))
    handle_info()