import os
import json
from time import sleep
import random
import subprocess
import re

class IngestionRetryExceeded(Exception):
    pass

def ingestion_handler(data, bash_file, meta_file, cleanup, retry, delay = 0):

    if retry < 0:
        raise IngestionFailedError("Retry limit exceeded")

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
    out = None
    with open(meta_file, "w") as f:
        json.dump(data, f)
    try:
        out = subprocess.check_output([bash_file, meta_file]).decode("utf-8")
    except subprocess.CalledProcessError:
        uuid = retry_failure_with_backoff()
    #if success output should be "Successfully submitted metadata object <uuid>"
    match = re.match(r"Successfully submitted metadata object (.+)", out)
    if match is not None:
        uuid = match.group(1)
    else:
        uuid = retry_failure_with_backoff()
    if cleanup:
        try:
            os.remove(meta_file)
        except:
            pass
    return uuid