import math

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
size = comm.Get_size()
processor_name = MPI.Get_processor_name()

##########################################




def partition_tables(tables):
    tables_range = len(tables)
    tables_pivot = math.ceil(tables_range / 2.0)
    tables_lower = tables[:tables_pivot]
    tables_upper = tables[tables_pivot:]
    return (tables_lower, tables_upper)

def partition(ranks, tables):
    ranks_range = ranks[1] - ranks[0]
    ranks_pivot = math.ceil(ranks_range / 2.0)
    ranks_lower = [ranks[0], ranks_pivot]
    ranks_upper = [ranks_pivot, ranks[1]]

    table_parts = partition_tables(tables)
    tables_lower = table_parts[0]
    tables_upper = table_parts[1]

    return [(ranks_lower, tables_lower), (ranks_upper, tables_upper)]


def handle_tables_local(tables):
    if len(tables < 1):
        raise ValueError("Tables list has no items. This should never happen unless the initial list is empty.")
    #only one table, reached bottom of recursion, return single table
    if len(tables) == 1:
        return tables[0]
    
    parts = partition_tables(tables)

    t1 = handle_tables_local(parts[0][1])
    t2 = handle_tables_local(parts[1][1])


    combined = combine_tables(t1, t2)
    return combined



def handle_tables(data):
    ranks = data[0]
    tables = data[1]

    if len(tables < 1):
        raise ValueError("Tables list has no items. This should never happen unless the initial list is empty.")
    #only one table, reached bottom of recursion, return single table
    if len(tables) == 1:
        return tables[0]
    
    parts = partition(ranks, tables)

    t1 = None
    t2 = None
    
    #distribute first group
    dist_ranks = parts[0][0]
    dist_tables = parts[0][1]
    #handle second group locally
    local_data = parts[1]

    dist_rank_range = dist_ranks[1] - dist_ranks[0]
    #if no ranks to distribute to then handle the rest locally
    if dist_rank_range == 0:
        #note using ceil so first group (dist group) always will have equal or greater the number of ranks, so if first group 0, second group wont have any either (can use local for both)
        t1 = handle_tables_local(parts[0][1])
        t2 = handle_tables_local(parts[1][1])
    else:
        #first rank in set of distributor ranks next rank to send to
        dist_rank = dist_ranks[0]
        #remaining ranks
        dist_rank_sub = dist_ranks[1:]
        #data to distribute (parent (current rank), ranks, tables)
        dist_data = (rank, dist_rank_sub, dist_tables)
        #send off first group to be processed by the next rank
        comm.send(dist_data, dest = dist_rank)
        #recursively handle second group
        
        t1 = handle_tables(dist_data)
        #get the table sent off to the next rank
        t2 = comm.recv()


    combined = combine_tables(t1, t2)
    return combined



def combine_tables():
    pass


def get_tables():
    data = comm.recv()
    parent = data[0]
    ranks = data[1]
    tables = data[2]

    #get combined table
    combined = handle_tables((ranks, tables))

    #send combined table to parent
    comm.send(combined, dest = parent)


        




if rank == distributor_rank:
    print("Starting distributor, rank: %d, node: %s" % (rank, processor_name))
    ranks = [0, size]
    tables = []
    #go directly to handle tables
    root_table = handle_tables((ranks, tables))
    
else:
    print("Starting data handler, rank: %d, node: %s" % (rank, processor_name))
    get_tables()