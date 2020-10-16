import collections
import os
import time
import pickle

import logging
logger = logging.getLogger(__name__)

def timed_pickle_load(file_name, pickle_description):
    logger.info(f"Unpickling {pickle_description}...")
    timer = Timer().start()
    unpickled = pickle.load(open(file_name, "rb"))
    logger.info(timer.stop_string())
    return unpickled

def timed_pickle_dump(the_object, file_name, pickle_description):
    logger.info(f"Pickling {pickle_description}...")
    timer = Timer().start()
    pickle.dump(the_object, open(file_name, "wb"))
    logger.info(timer.stop_string())

class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""

class Timer:
    def __init__(self):
        self._start_time = None

    def start(self):
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError(f"Timer is running. Use .stop() to stop it")

        self._start_time = time.perf_counter()

        return self

    def stop(self):
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None
        return elapsed_time

    def stop_string(self):
        elapsed = self.stop()
        return f"Took {elapsed:0.2f}s"

def linecount(filename):
    f = open(filename, 'rb')
    lines = 0
    buf_size = 1024 * 1024
    read_f = f.raw.read

    buf = read_f(buf_size)
    while buf:
        lines += buf.count(b'\n')
        buf = read_f(buf_size)

    return lines

def chunker(l, n, s=0):
    """Yield successive n-sized chunks from l, skipping the first s chunks."""
    if isinstance(l, collections.Iterable):
        chnk = []
        for i, elem in enumerate(l):
            if i < s:
                continue

            chnk.append(elem)
            if len(chnk) == n:
                yield chnk
                chnk = []
        if len(chnk) != 0:
            yield chnk

    else:
        for i in range(s, len(l), n):
            yield l[i : i + n]

def mkdir(fp):
    try:
        os.makedirs(fp)
    except FileExistsError:
        pass
    return fp