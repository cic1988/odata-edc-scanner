import logging

from configparser import ConfigParser
from hotqueue import HotQueue

logger = logging.getLogger(__name__)

def get_taskqueue(name):
    import os
    import sys

    file = sys.argv[0]
    pathname = os.path.dirname(os.path.abspath(file))

    cp = ConfigParser()
    cp.read(pathname + '/config.ini')
    queuename = cp.get('local', 'queuename')
    dbfilename = cp.get('local', 'dbfilename')
    pathname = dbfilename

    if not os.path.exists(os.path.dirname(pathname)):
        logger.info(f'[... CREATE : {dbfilename} ...]')
        os.makedirs(os.path.dirname(pathname))

    return HotQueue(queuename, dbfilename=pathname)


def get_consumerqueue(name):
    import os
    import sys

    file = sys.argv[0]
    pathname = os.path.dirname(os.path.abspath(file))

    cp = ConfigParser()
    cp.read(pathname + '/config.ini')
    queuename = cp.get('local', 'consumerqueuename')
    dbfilename = cp.get('local', 'dbfilename')
    pathname = dbfilename

    if not os.path.exists(os.path.dirname(pathname)):
        os.makedirs(os.path.dirname(pathname))

    return HotQueue(queuename, dbfilename=pathname)


def worker(name, *args, **kwargs):
    """ get taskqueue decorater """
    queue = get_taskqueue(name)
    return queue.worker(*args, **kwargs)


def consumer(name, *args, **kwargs):
    """ get consumerqueue decorater """
    queue = get_consumerqueue(name)
    return queue.worker(*args, **kwargs)
