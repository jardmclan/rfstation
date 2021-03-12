import csv
import re
import json
from ingestion_handler import ingestion_handler
from sys import stderr, argv
from os.path import join
from geotiff_data import RasterData
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

#document names should not change, broad identifier of document type
#5 potential douments
doc_names = {
    "raster_header": "hcdp_raster_header",
    "raster": "hcdp_raster",
    "value": "hcdp_station_value",
    "active_range": "hcdp_station_active_range",
    "metadata": "hcdp_station_metadata"
}

##########################################

#process rank
rank = comm.Get_rank()
processor_name = MPI.Get_processor_name()
doc_num = 0

##########################################


#remember $date IS necessary


def get_doc_name(doc_type):
    doc_name = doc_names[doc_type]
    return doc_name



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





def handle_geotiff(info):

    doc_key_base = info["key"]
    descriptor = info["descriptor"]
    date = info["date"]
    raster_file = info["file"]

    geotiff_data = RasterData(raster_file)


    doc_name = get_doc_name("raster")
    raster_doc = {
        "name": doc_name,
        "value": {
            "version": version,
            "key": {
            },
            "descriptor": {
            },
            "date": date,
            "data": geotiff_data.data
        }
    }
    for key in doc_key_base:
        raster_doc["value"]["key"][key] = doc_key_base[key]
    for key in descriptor:
        raster_doc["value"]["descriptor"][key] = descriptor[key]
    send_doc(raster_doc)



def handle_info():
    
    try:
        #send rank to request data
        info = comm.sendrecv(rank, dest = distributor_rank)
        #process data and request more until terminator received from distributor
        while info is not None:
            handle_geotiff(info)
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

    

    raster_data = config["raster_data"]

    

    ###########################################################################

    for raster_data_item in raster_data:
 
        key = raster_data_item["key"]
        descriptor = raster_data_item["descriptor"]
        raster_file_info = raster_data_item["file_info"]

        for raster_file_info_item in raster_file_info:
            date = raster_file_info_item["date"]
            raster_file = raster_file_info_item["file"]

            #distribute info with file, type, and field data
            info = {
                "key": key,
                "descriptor": descriptor,
                "date": date,
                "file": raster_file
            }

            send_info(info)

    ###################################################


   

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