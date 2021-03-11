import csv
import re
import json
from ingestion_handler import ingestion_handler
from sys import stderr, argv
from os.path import join
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


#document names should not change, broad identifier of document type
#5 potential douments
doc_names = {
    "raster_header": "hcdp_raster_header",
    "raster": "hcdp_raster",
    "station_value": "hcdp_station_value",
    "active_range": "hcdp_station_active_range",
    "metadata": "hcdp_station_metadata"
}

##########################################


#remember $date IS necessary


def get_doc_name(doc_type):
    doc_name = doc_names[doc_type]
    return doc_name

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
    global doc_num
    bash_file = config["bash_file"]
    outdir = config["outdir"]
    cleanup = config["cleanup"]
    retry = config["retry"]

    meta_file = "doc_%d_%d.json" % (doc_num, rank)
    doc_num += 1
    meta_file = join(outdir, meta_file)
    uuid = ingestion_handler(doc, bash_file, meta_file, cleanup, retry)
    print("Complete UUID: %s" % uuid)




def handle_station_values(info):

    dates = info["dates"]
    data = info["data"]
    doc_key_base = info["doc_key_base"]
    period = info["period"]
    station_id = info["station_id"]
    nodata = info["nodata"] 
    descriptor = info["descriptor"]
                
    #note active range should be retreivable using min and max queries on date field

    doc_name = get_doc_name("station_value")

    for i in range(len(data)):
        value = data[i]
        if value != nodata:
            value_doc = {
                "name": doc_name,
                "value": {
                    "version": version,
                    "key": {
                        "period": period
                    },
                    "descriptor": {
                        "station_id": station_id
                    },
                    "date": dates[i],
                    "data": value
                }
            }
            for key in doc_key_base:
                value_doc["value"]["key"][key] = doc_key_base[key]
            for key in descriptor:
                value_doc["value"]["descriptor"][key] = descriptor[key]
            send_doc(value_doc)



#SKN, METADATA DOC PART OF DATA


def handle_info():
    
    try:
        #send rank to request data
        info = comm.sendrecv(rank, dest = distributor_rank)
        #process data and request more until terminator received from distributor
        while info is not None:

            handle_station_values(info)
                    
            info = comm.sendrecv(rank, dest = distributor_rank)
        print("Rank %d received terminator. Exiting data handler..." % rank)
    except Exception as e:
        print("An error has occured in rank %d while handling data: %s" % (rank, e), file = stderr)
        print("Rank %d encountered an error. Exiting data handler..." % rank)
        #notify the distributor that one of the ranks failed and will not be requesting more data by sending -1
        comm.send(-1, dest = distributor_rank)





def distribute():

    ranks = comm.Get_size() - 1

    def send_info(info):
        nonlocal ranks
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

    
    station_file_data = config["station_file_data"]

    
    for station_file_data_item in station_file_data:

        doc_key_base = station_file_data_item["key"]
        data_file = station_file_data_item["file"]
        id_col = station_file_data_item["id_col"]
        data_col_start = station_file_data_item["data_col_start"]
        nodata = station_file_data_item["nodata"]
        period = station_file_data_item["period"]
        descriptor = station_file_data_item["descriptor"]

        #have each rank handle one row at a time
        with open(data_file, "r") as fd:
            reader = csv.reader(fd)
            dates = None
            for row in reader:
                if dates is None:
                    dates = row[data_col_start:]
                    #transform dates
                    for i in range(len(dates)):
                        dates[i] = parse_date(dates[i], period)
                else:
                    station_id = row[id_col]
                    values = row[data_col_start:]

                    info = {
                        "dates": dates,
                        "data": values,
                        "doc_key_base": doc_key_base,
                        "descriptor": descriptor,
                        "period": period,
                        "station_id": station_id,
                        "nodata": nodata
                    }
                    send_info(info)

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