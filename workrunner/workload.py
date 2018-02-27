import collections
import os
import pprint
import random
import time
import sys
import subprocess

from commons import *
import config
import multiwriters
from utilities import utils

from pyreuse.helpers import *
from pyreuse.apputils.parseleveldboutput import parse_file

class Workload(object):
    def __init__(self, confobj, workload_conf_key = None):
        """
        workload_conf is part of confobj. But we may need to run
        multiple workloads with different configurations in our
        experiements. So we need workload_conf to specify which
        configuration we will use in a Workload instance.

        Since workload_conf has a default value, it should be
        compatible with previous code. However, new classes based
        one Workload should use this new __init__() with two parameters.
        """
        if not isinstance(confobj, config.Config):
            raise TypeError("confobj is not of type class config.Config".
                format(type(confobj).__name__))

        self.conf = confobj
        if workload_conf_key != None and workload_conf_key != 'None':
            self.workload_conf = confobj[workload_conf_key]

    def run(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

class NoOp(Workload):
    """
    This is a workload class that does nothing. It may be used to skip
    the file system aging stage. To skip aging workload, set
    conf['age_workload_class'] = "NoOp"
    """
    def run(self):
        pass

    def stop(self):
        pass


class SimpleRandReadWrite(Workload):
    def __init__(self, confobj, workload_conf_key = None):
        super(SimpleRandReadWrite, self).__init__(confobj, workload_conf_key)

    def run(self):
        mnt = self.conf["fs_mount_point"]
        datafile = os.path.join(mnt, "datafile")
        region = 2 * MB
        chunksize = 64 * KB
        n_chunks = region / chunksize
        chunkids = range(n_chunks)

        buf = "a" * chunksize
        f = open(datafile, "w+")
        random.shuffle(chunkids)
        for chunkid in chunkids:
            offset = chunkid * chunksize
            f.seek(offset)
            f.write(buf)
            os.fsync(f)

        random.shuffle(chunkids)
        for chunkid in chunkids:
            offset = chunkid * chunksize
            f.seek(offset)
            buf = f.read()
            os.fsync(f)

        f.close()

    def stop(self):
        pass


# class LinuxDD(Workload):
    # def __init__(self, confobj, workload_conf_key = None):
        # super(LinuxDD, self).__init__(confobj, workload_conf_key)

    # def run(self):
        # mnt = self.conf["fs_mount_point"]
        # cmd = "dd if=/dev/zero of={}/datafile bs=64k count=128".format(mnt)
        # print cmd
        # subprocess.call(cmd, shell=True)
        # subprocess.call("sync")

    # def stop(self):
        # pass

############################################
## Test Request Scale
############################################

# We want to write a 2MB file in different chunk sizes, issuing different number
# of multiple concurrent requests. We want to examine the completion time for
# each of the workloads. We also make note of the NCQ depth achieved by the
# different workloads.
#
# NOTE: All experiments for request scale run on /dev/sdc2
# We want the requests to be to non-contiguous regions, to avoid merging of
# write requests

class MeasureRequestScaleBaseClass(Workload):
    def __init__(
        self,
        confobj,
        chunksize,
        n_outstanding_requests,
        file_size=2*MB,
        workload_conf_key=None
    ):
        self.chunksize = chunksize
        self.n_outstanding_requests = n_outstanding_requests
        self.file_size = file_size
        super(MeasureRequestScaleBaseClass, self).__init__(
            confobj, workload_conf_key
        )

    def run(self):
        mnt = self.conf["fs_mount_point"]
        chunksize = self.chunksize
        n_outstanding_requests = self.n_outstanding_requests
        datafile = os.path.join(
            mnt, "workload_{}_{}".format(chunksize/KB, n_outstanding_requests)
        )

        n_chunks = self.file_size/chunksize
        n_request_groups = \
            n_chunks / n_outstanding_requests + n_chunks % n_outstanding_requests

        buf = "a" * chunksize
        f = open(datafile, "w+")

        # We are intentionally not writing sequentially, as we do not want our
        # write requests to be merged.
        for i in range(n_request_groups):
            for j in range(n_outstanding_requests):
                chunk_id = j*n_request_groups + i
                if chunk_id < n_chunks:
                    offset = chunk_id * chunksize
                    f.seek(offset)
                    f.write(buf)
            # To issue at most n_outstanding_requests
            os.fsync(f)

# NOTE: Class name MeasureRequestScale_{n}KB_{m}, means chunksize is 'n' KB, and
# n_outstanding_requests are 'm'
# The maximum number of n_outstanding_requests supported by the SSD is 32
class MeasureRequestScale_2KB_8(MeasureRequestScaleBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureRequestScale_2KB_8, self).__init__(
            confobj=confobj,
            chunksize=2*KB,
            n_outstanding_requests=8,
            workload_conf_key=None
        )

class MeasureRequestScale_2KB_16(MeasureRequestScaleBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureRequestScale_2KB_16, self).__init__(
            confobj=confobj,
            chunksize=2*KB,
            n_outstanding_requests=16,
            workload_conf_key=None
        )

class MeasureRequestScale_2KB_32(MeasureRequestScaleBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureRequestScale_2KB_32, self).__init__(
            confobj=confobj,
            chunksize=2*KB,
            n_outstanding_requests=32,
            workload_conf_key=None
        )

class MeasureRequestScale_4KB_4(MeasureRequestScaleBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureRequestScale_4KB_4, self).__init__(
            confobj=confobj,
            chunksize=4*KB,
            n_outstanding_requests=4,
            workload_conf_key=None
        )

class MeasureRequestScale_4KB_8(MeasureRequestScaleBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureRequestScale_4KB_8, self).__init__(
            confobj=confobj,
            chunksize=4*KB,
            n_outstanding_requests=8,
            workload_conf_key=None
        )

class MeasureRequestScale_4KB_16(MeasureRequestScaleBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureRequestScale_4KB_16, self).__init__(
            confobj=confobj,
            chunksize=4*KB,
            n_outstanding_requests=16,
            workload_conf_key=None
        )

class MeasureRequestScale_4KB_32(MeasureRequestScaleBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureRequestScale_4KB_32, self).__init__(
            confobj=confobj,
            chunksize=4*KB,
            n_outstanding_requests=32,
            workload_conf_key=None
        )

class MeasureRequestScale_8KB_4(MeasureRequestScaleBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureRequestScale_8KB_4, self).__init__(
            confobj=confobj,
            chunksize=8*KB,
            n_outstanding_requests=4,
            workload_conf_key=None
        )

class MeasureRequestScale_8KB_8(MeasureRequestScaleBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureRequestScale_8KB_8, self).__init__(
            confobj=confobj,
            chunksize=8*KB,
            n_outstanding_requests=8,
            workload_conf_key=None
        )

class MeasureRequestScale_8KB_16(MeasureRequestScaleBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureRequestScale_8KB_16, self).__init__(
            confobj=confobj,
            chunksize=8*KB,
            n_outstanding_requests=16,
            workload_conf_key=None
        )

class MeasureRequestScale_8KB_32(MeasureRequestScaleBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureRequestScale_8KB_32, self).__init__(
            confobj=confobj,
            chunksize=8*KB,
            n_outstanding_requests=32,
            workload_conf_key=None
        )

class MeasureRequestScale_16KB_4(MeasureRequestScaleBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureRequestScale_16KB_4, self).__init__(
            confobj=confobj,
            chunksize=16*KB,
            n_outstanding_requests=4,
            workload_conf_key=None
        )

class MeasureRequestScale_16KB_8(MeasureRequestScaleBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureRequestScale_16KB_8, self).__init__(
            confobj=confobj,
            chunksize=16*KB,
            n_outstanding_requests=8,
            workload_conf_key=None
        )

class MeasureRequestScale_16KB_16(MeasureRequestScaleBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureRequestScale_16KB_16, self).__init__(
            confobj=confobj,
            chunksize=16*KB,
            n_outstanding_requests=16,
            workload_conf_key=None
        )

class MeasureRequestScale_16KB_32(MeasureRequestScaleBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureRequestScale_16KB_32, self).__init__(
            confobj=confobj,
            chunksize=16*KB,
            n_outstanding_requests=32,
            workload_conf_key=None
        )
