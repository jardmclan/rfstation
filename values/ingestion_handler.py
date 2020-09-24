import os
import json
from time import sleep
import random
import subprocess
import re

def ingestion_handler(data, bash_file, meta_file, cleanup, retry):
    out = None
    with open(meta_file, "w") as f:
        json.dump(data, f)
    uuid = submit_meta(bash_file, meta_file, cleanup, retry)
    if cleanup:
        os.remove(meta_file)
    return uuid



def submit_meta(bash_file, meta_file, cleanup, retry, delay = 0):
    if retry < 0:
        raise Exception("Retry limit exceeded")

    def get_backoff():
        backoff = 0
        #if first failure backoff of 0.25-0.5 seconds
        if delay == 0:
            backoff = 0.25 + random.uniform(0, 0.25)
        #otherwise 2-3x current backoff
        else:
            backoff = delay * 2 + random.uniform(0, delay)
        return backoff

    sleep(delay)
    uuid = None
    try:
        out = subprocess.check_output([bash_file, meta_file]).decode("utf-8")
        #if success output should be "Successfully submitted metadata object <uuid>"
        match = re.match(r"Successfully submitted metadata object (.+)", out)
        if match is not None:
            uuid = match.group(1)
        else:
            backoff = get_backoff()
            uuid = submit_meta(bash_file, meta_file, cleanup, retry - 1, backoff)
    except subprocess.CalledProcessError:
        backoff = get_backoff()
        uuid = submit_meta(bash_file, meta_file, cleanup, retry - 1, backoff)
    
    return uuid