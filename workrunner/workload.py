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

        f.close()

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

############################################
## Test Aligned Sequentiality
############################################

# The SSD being simulated has pages of size 2KB. We simulate the SSD for
# two different block sizes: 128 KB (64 pages), and 1 MB (512 pages).
#
# We want to write a file of size 2 MB. We vary the extent of unalignment while
# writing to SSD blocks, and make a note of how the number of
# pages moved for merging SSD blocks change with it. With increasing unaligned
# block writes, we expect the number of pages moved to go up.

class RandomWrite(Workload):
    def __init__(self, confobj, workload_conf_key=None):
        super(RandomWrite, self).__init__(confobj, workload_conf_key)

    def run(self):
        mnt = self.conf['fs_mount_point']
        datafile = os.path.join(mnt, "workload_alignment_temp_{}")
        page_size = 2*KB
        file_size = 4*MB
        n_pages = file_size/page_size

        buf = "a"*page_size
        write_order = range(n_pages)

        # Write two files of size 2*MB each randomly
        for file_id in range(2):
            datafile = datafile.format(file_id)
            f = open(datafile, "w+")
            random.shuffle(write_order)
            for i in write_order:
                offset = i*page_size
                f.seek(offset)
                f.write(buf)
                os.fsync(f)

            f.close()

class MeasureAlignmentBaseClass(Workload):
    def __init__(
        self,
        confobj,
        aligned_ratio,
        workload_conf_key=None,
    ):
        """
            aligned_ratio : The ratio of blocks with aligned writes
        """
        self.aligned_ratio = aligned_ratio
        super(MeasureAlignmentBaseClass, self).__init__(
            confobj, workload_conf_key
        )

    def run(self):
        n_pages_per_block = self.conf['flash_config']['n_pages_per_block']
        page_size = self.conf['flash_config']['page_size'] # Is 2KB always
        block_size = n_pages_per_block * page_size

        file_size = 2 * MB
        n_blocks = file_size/block_size

        # Randomly select the blocks with aligned writes
        n_aligned_blocks = int(self.aligned_ratio * n_blocks)
        aligned_block_ids = range(n_blocks)
        random.shuffle(aligned_block_ids)
        aligned_block_ids = aligned_block_ids[0:n_aligned_blocks]

        # Create a random order of page writes for writing to unaligned blocks
        random_page_order = range(n_pages_per_block)
        random.shuffle(random_page_order)

        mnt = self.conf["fs_mount_point"]
        datafile = os.path.join(mnt, "workload_alignment")
        buf = "a" * page_size
        f = open(datafile, "w+")

        for i in range(n_blocks):
            if i in aligned_block_ids:
                # Write pages sequentially
                page_order = range(n_pages_per_block)
            else:
                # Write pages randomly
                page_order = random_page_order

            for j in page_order:
                offset = i*block_size + j*page_size
                f.seek(offset)
                f.write(buf)
                os.fsync(f)

        f.close()

# Note: Class name MeasureAlignment_{n} means n% of the block writes are aligned
class MeasureAlignment_0(MeasureAlignmentBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureAlignment_0, self).__init__(
            confobj=confobj,
            aligned_ratio=0,
            workload_conf_key=workload_conf_key,
        )

class MeasureAlignment_20(MeasureAlignmentBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureAlignment_20, self).__init__(
            confobj=confobj,
            aligned_ratio=0.2,
            workload_conf_key=workload_conf_key,
        )

class MeasureAlignment_40(MeasureAlignmentBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureAlignment_40, self).__init__(
            confobj=confobj,
            aligned_ratio=0.4,
            workload_conf_key=workload_conf_key,
        )

class MeasureAlignment_60(MeasureAlignmentBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureAlignment_60, self).__init__(
            confobj=confobj,
            aligned_ratio=0.6,
            workload_conf_key=workload_conf_key,
        )

class MeasureAlignment_80(MeasureAlignmentBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureAlignment_80, self).__init__(
            confobj=confobj,
            aligned_ratio=0.8,
            workload_conf_key=workload_conf_key,
        )

class MeasureAlignment_100(MeasureAlignmentBaseClass):
    def __init__(self, confobj, workload_conf_key=None):
        super(MeasureAlignment_100, self).__init__(
            confobj=confobj,
            aligned_ratio=1,
            workload_conf_key=workload_conf_key,
        )

############################################
## Test Locality
############################################

# We compare the behaviour of three workloads:
# 1) Write sequentially, followed by reading data in the forward order
# 2) Write sequentially, followed by reading in the reverse order
# 3) Write randomly, and read randomly
#
# We expect workload 2 to exhibit the minimum miss ratio. However, we expect that
# workload 1 will be quite close to workload 2. Workload 3, because its random
# accesses, should exhibit the maximum miss ratio.

class SequentialWriteForwardRead(Workload):
    def __init__(self, confobj, workload_conf_key=None):
        super(SequentialWriteForwardRead, self).__init__(
            confobj, workload_conf_key
        )

    def run(self):
        mnt = self.conf['fs_mount_point']
        datafile = os.path.join(mnt, "datafile_seq_write_fwd_read")
        f = open(datafile, "w+")

        file_size = 2*MB
        chunk_size = 8*KB
        n_chunks = file_size/chunk_size
        chunk_ids = range(n_chunks)

        buf = "a"*chunk_size
        # Write sequentially
        for i in chunk_ids:
            offset = i*chunk_size
            f.seek(offset)
            f.write(buf)
            os.fsync(f)

        # Read sequentially forward
        for i in chunk_ids:
            offset = i*chunk_size
            f.seek(offset)
            buf = f.read()
            os.fsync(f)

        f.close()

class SequentialWriteBackwardRead(Workload):
    def __init__(self, confobj, workload_conf_key=None):
        super(SequentialWriteBackwardRead, self).__init__(
            confobj, workload_conf_key
        )

    def run(self):
        mnt = self.conf['fs_mount_point']
        datafile = os.path.join(mnt, "datafile_seq_write_bck_read")
        f = open(datafile, "w+")

        file_size = 2*MB
        chunk_size = 8*KB
        n_chunks = file_size/chunk_size
        chunk_ids = range(n_chunks)

        buf = "a"*chunk_size
        # Write sequentially
        for i in chunk_ids:
            offset = i*chunk_size
            f.seek(offset)
            f.write(buf)
            os.fsync(f)

        # Read sequentially backward
        chunk_ids.reverse()
        for i in chunk_ids:
            offset = i*chunk_size
            f.seek(offset)
            buf = f.read()
            os.fsync(f)

        f.close()

class RandomWriteRandomRead(Workload):
    def __init__(self, confobj, workload_conf_key=None):
        super(RandomWriteRandomRead, self).__init__(confobj, workload_conf_key)

    def run(self):
        mnt = self.conf['fs_mount_point']
        datafile = os.path.join(mnt, "datafile_rand_write_rand_read")
        f = open(datafile, "w+")

        file_size = 2*MB
        chunk_size = 8*KB
        n_chunks = file_size/chunk_size
        chunk_ids = range(n_chunks)

        buf = "a"*chunk_size
        # Write randomly
        random.shuffle(chunk_ids)
        for i in chunk_ids:
            offset = i*chunk_size
            f.seek(offset)
            f.write(buf)
            os.fsync(f)

        # Read randomly
        random.shuffle(chunk_ids)
        for i in chunk_ids:
            offset = i*chunk_size
            f.seek(offset)
            buf = f.read()
            os.fsync(f)

        f.close()
