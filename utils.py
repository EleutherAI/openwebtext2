import collections
import os

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