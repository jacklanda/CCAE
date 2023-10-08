# -*- coding: utf-8 -*-
# @author: YangLiu
# @email: yangliu.real@gmail.com

# entry script for web links crawling

import os
import sys
from random import shuffle
from time import time, sleep
from concurrent.futures import TimeoutError
from typing import Any, List

import pebble
from pebble import ProcessPool
try:
    from pyvirtualdisplay import Display  # only awailable for linux
except:
    raise ImportError("Please check pkg `pyvirtualdisplay`'s installation.'")

from auto_google_search import main, read_query_file

# country code mapping for 6 ChineseEnglish varieties
CC_MAP = {"cn": "CN", "hk": "HK", "mo": "MO", "tw": "TW", "my": "MY", "sg": "SG"}
AREA = CC_MAP[sys.argv[1]]


def get_chunks(lst: List, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def postlude(*args: Any, **kwargs: Any):
    try:
        os.system("killall chrome; killall chromedriver")
    except:
        pass
    try:
        os.system("rm -rf /tmp/.com.google.Chrome*")
    except:
        pass


if __name__ == "__main__":
    begin = time()
    # display = Display(visible=0, size=(800, 800))  # only awailable for linux
    # display.start()  # only awailable for linux
    query_list = read_query_file()

    if len(sys.argv) < 2:
        exit()

    search_area = CC_MAP[sys.argv[1]]

    def task_done(future: pebble.ProcessFuture):
        try:
            result_obj = future.result()
            print("task done")
        except TimeoutError as error:
            print("Function took longer than %d seconds" % error.args[1])
        except Exception as error:
            print("Function raised %s" % error)

    for batch in get_chunks(query_list, 200):  # batch_size = 200
        with ProcessPool(max_workers=5, max_tasks=5) as pool:
            for mini_batch in get_chunks(batch, 10):  # batch size for each worker
                shuffle(mini_batch)
                future = pool.schedule(main, (mini_batch,), timeout=600)
                future.add_done_callback(task_done)
        postlude()
        sleep(60)  # have a rest
        end = time()
        print(f"batch crawl done, time: {round(end-begin)}s")

    # display.stop()  # only awailable for linux
