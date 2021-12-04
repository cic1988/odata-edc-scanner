from configparser import ConfigParser
from hotqueue import HotQueue


def get_taskqueue(name):
    import os
    import sys

    cp = ConfigParser()
    cp.read(os.path.dirname(os.path.abspath(__file__)) + '/config.ini')
    queuename = cp.get('local', 'queuename')
    dbfilename = cp.get('local', 'dbfilename')

    file = sys.argv[0]
    pathname = os.path.dirname(os.path.abspath(file))
    pathname = pathname + "/" + dbfilename
    return HotQueue(queuename, dbfilename=pathname)


def worker(name, *args, **kwargs):
    """ get taskqueue decorater """
    queue = get_taskqueue(name)
    return queue.worker(*args, **kwargs)
