import unittest
import collections
import shutil
import os

import config
from workflow import *
import wiscsim
from utilities import utils
from wiscsim.hostevent import Event, ControlEvent
from config_helper import rule_parameter
from pyreuse.helpers import shcmd
from config_helper import experiment


class Test_TraceOnly(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "SimpleRandReadWrite"

        para = experiment.get_shared_nolist_para_dict("test_exp_TraceOnly2", 16*MB)
        para['device_path'] = "/dev/loop0"
        para['enable_simulation'] = False
        para['enable_blktrace'] = True

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()


class Test_TraceAndSimulateDFTLDES(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "SimpleRandReadWrite"

        para = experiment.get_shared_nolist_para_dict("test_exp_TraceAndSimulateDFTLDES_xjjj", 16*MB)
        para['device_path'] = "/dev/loop0"
        para['ftl'] = "dftldes"
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()


class Test_TraceAndSimulateNKFTL(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "SimpleRandReadWrite"

        para = experiment.get_shared_nolist_para_dict("test_exp_TraceAndSimulateNKFTL_xjjj", 16*MB)
        para['device_path'] = "/dev/loop0"
        para['ftl'] = "nkftl2"
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()


class Test_SimulateForSyntheticWorkload(unittest.TestCase):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_src'] = config.LBAGENERATOR
                self.conf['lba_workload_class'] = "AccessesWithDist"
                self.conf['AccessesWithDist'] = {
                        'lba_access_dist': 'uniform',
                        'traffic_size': 8*MB,
                        'chunk_size': 64*KB,
                        'space_size': 8*MB,
                        'skew_factor': None,
                        'zipf_alpha': None,
                        }

        para = experiment.get_shared_nolist_para_dict("test_exp_SimulateForSyntheticWorkload", 16*MB)
        para['ftl'] = "nkftl2"
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()


class TestUsingExistingTraceToSimulate(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_src"] = config.LBAGENERATOR
                self.conf["lba_workload_class"] = "BlktraceEvents"
                self.conf['lba_workload_configs']['mkfs_event_path'] = \
                        self.para.mkfs_path
                self.conf['lba_workload_configs']['ftlsim_event_path'] = \
                        self.para.ftlsim_path

        para = experiment.get_shared_nolist_para_dict("test_exp_TestUsingExistingTraceToSimulate_jj23hx", 1*GB)
        para.update({
            'ftl': "dftldes",
            "mkfs_path": "./tests/testdata/sqlitewal-update/subexp-7928737328932659543-ext4-10-07-23-50-10--2726320246496492803/blkparse-events-for-ftlsim-mkfs.txt",
            "ftlsim_path": "./tests/testdata/sqlitewal-update/subexp-7928737328932659543-ext4-10-07-23-50-10--2726320246496492803/blkparse-events-for-ftlsim.txt",
            })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()




class TestUsingExistingTraceToStudyRequestScale(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_src"] = config.LBAGENERATOR
                self.conf["lba_workload_class"] = "BlktraceEvents"
                self.conf['lba_workload_configs']['mkfs_event_path'] = \
                        self.para.mkfs_path
                self.conf['lba_workload_configs']['ftlsim_event_path'] = \
                        self.para.ftlsim_path

        para = experiment.get_shared_nolist_para_dict("test_exp_TestUsingExistingTraceToStudyRequestScale_jj23hx", 1*GB)
        para.update({
            'ftl': "ftlcounter",
            "mkfs_path": "./tests/testdata/sqlitewal-update/subexp-7928737328932659543-ext4-10-07-23-50-10--2726320246496492803/blkparse-events-for-ftlsim-mkfs.txt",
            "ftlsim_path": "./tests/testdata/sqlitewal-update/subexp-7928737328932659543-ext4-10-07-23-50-10--2726320246496492803/blkparse-events-for-ftlsim.txt",
            'ftl' : 'ftlcounter',
            'enable_simulation': True,
            'dump_ext4_after_workload': True,
            'only_get_traffic': False,
            'trace_issue_and_complete': True,
            'do_dump_lpn_sem': False,
            })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()


###################################################################
# Experiments setting similar to SSD Contract paper
###################################################################

class TestRunningWorkloadAndOutputRequestScale(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "SimpleRandReadWrite"

        para = experiment.get_shared_nolist_para_dict("test_exp_TestRequestScale_jjj3nx", 16*MB)
        para['device_path'] = "/dev/loop0"
        para.update(
            {
                'device_path': "/dev/loop0",
                'ftl' : 'ftlcounter',
                'enable_simulation': True,
                'dump_ext4_after_workload': True,
                'only_get_traffic': False,
                'trace_issue_and_complete': True,
            })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()


class TestLocality(unittest.TestCase):
    def test(self):
        old_dir = "/tmp/results/sqlitewal-update"
        if os.path.exists(old_dir):
            shutil.rmtree(old_dir)

        # copy the data to
        shcmd("cp -r ./tests/testdata/sqlitewal-update /tmp/results/")

        for para in rule_parameter.ParaDict("testexpname", ['sqlitewal-update'], "locality"):
            experiment.execute_simulation(para)

class TestAlignment(unittest.TestCase):
    def test(self):
        old_dir = "/tmp/results/sqlitewal-update"
        if os.path.exists(old_dir):
            shutil.rmtree(old_dir)

        # copy the data to
        shcmd("cp -r ./tests/testdata/sqlitewal-update /tmp/results/")

        for para in rule_parameter.ParaDict("testexpname", ['sqlitewal-update'], "alignment"):
            experiment.execute_simulation(para)


class TestGrouping(unittest.TestCase):
    def test(self):
        old_dir = "/tmp/results/sqlitewal-update"
        if os.path.exists(old_dir):
            shutil.rmtree(old_dir)

        # copy the data to
        shcmd("cp -r ./tests/testdata/sqlitewal-update /tmp/results/")

        for para in rule_parameter.ParaDict("testexpname", ['sqlitewal-update'], "grouping"):
            experiment.execute_simulation(para)


class TestUniformDataLifetime(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "SimpleRandReadWrite"

        para = experiment.get_shared_nolist_para_dict("test_exp_TestUniformDataLifetime", 16*MB)
        para.update(
            {
                'ftl' : 'ftlcounter',
                'device_path'    : '/dev/loop0',
                'enable_simulation': True,
                'dump_ext4_after_workload': True,
                'only_get_traffic': False,
                'trace_issue_and_complete': False,
                'gen_ncq_depth_table': False,
                'do_dump_lpn_sem': False,
                'rm_blkparse_events': True,
                'sort_block_trace': False,
            })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment( Parameters(**para) )
        obj.main()


# class Test_TraceAndSimulateLinuxDD(unittest.TestCase):
    # def test_run(self):
        # class LocalExperiment(experiment.Experiment):
            # def setup_workload(self):
                # self.conf['workload_class'] = "LinuxDD"

        # para = experiment.get_shared_nolist_para_dict("test_exp_LinuxDD", 16*MB)
        # para['device_path'] = "/dev/loop0"
        # para['filesystem'] = "ext4"
        # para['ftl'] = "dftldes"
        # Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        # obj = LocalExperiment( Parameters(**para) )
        # obj.main()

############################################
## Test Request Scale
############################################

# We want to write a 2MB file in different chunk sizes, issuing different number
# of multiple concurrent requests. We want to examine the completion time for
# each of the workloads. We also make note of the NCQ depth achieved by the
# different workloads.
#
# NOTE: All experiments for request scale run on /dev/sdc2

# NOTE: Class name TestRequestScale_{n}KB_{m}, means chunksize
# is 'n' KB, and n_outstanding_requests are 'm'
# The maximum number of n_outstanding_requests supported by the SSD is 32

class TestRequestScale_2KB_8(unittest.TestCase):
    def test_run(self):
        # Define local experiment class
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_class"] = "MeasureRequestScale_2KB_8"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_request_scale_seqwrite_2KB_8",
            lbabytes=1024*MB
        )
        para.update({
            "device_path": "/dev/sdc2",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestRequestScale_2KB_16(unittest.TestCase):
    def test_run(self):
        # Define local experiment class
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_class"] = "MeasureRequestScale_2KB_16"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_request_scale_seqwrite_2KB_16",
            lbabytes=1024*MB,
        )
        para.update({
            "device_path": "/dev/sdc2",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestRequestScale_2KB_32(unittest.TestCase):
    def test_run(self):
        # Define local experiment class
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_class"] = "MeasureRequestScale_2KB_32"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_request_scale_seqwrite_2KB_32",
            lbabytes=1024*MB,
        )
        para.update({
            "device_path": "/dev/sdc2",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestRequestScale_4KB_4(unittest.TestCase):
    def test_run(self):
        # Define local experiment class
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_class"] = "MeasureRequestScale_4KB_4"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_request_scale_seqwrite_4KB_4",
            lbabytes=1024*MB
        )
        para.update({
            "device_path": "/dev/sdc2",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestRequestScale_4KB_8(unittest.TestCase):
    def test_run(self):
        # Define local experiment class
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_class"] = "MeasureRequestScale_4KB_8"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_request_scale_seqwrite_4KB_8",
            lbabytes=1024*MB
        )
        para.update({
            "device_path": "/dev/sdc2",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestRequestScale_4KB_16(unittest.TestCase):
    def test_run(self):
        # Define local experiment class
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_class"] = "MeasureRequestScale_4KB_16"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_request_scale_seqwrite_4KB_16",
            lbabytes=1024*MB
        )
        para.update({
            "device_path": "/dev/sdc2",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestRequestScale_4KB_32(unittest.TestCase):
    def test_run(self):
        # Define local experiment class
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_class"] = "MeasureRequestScale_4KB_32"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_request_scale_seqwrite_4KB_32",
            lbabytes=1024*MB
        )
        para.update({
            "device_path": "/dev/sdc2",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestRequestScale_8KB_4(unittest.TestCase):
    def test_run(self):
        # Define local experiment class
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_class"] = "MeasureRequestScale_8KB_4"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_request_scale_seqwrite_8KB_4",
            lbabytes=1024*MB
        )
        para.update({
            "device_path": "/dev/sdc2",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestRequestScale_8KB_8(unittest.TestCase):
    def test_run(self):
        # Define local experiment class
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_class"] = "MeasureRequestScale_8KB_8"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_request_scale_seqwrite_8KB_8",
            lbabytes=1024*MB
        )
        para.update({
            "device_path": "/dev/sdc2",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestRequestScale_8KB_16(unittest.TestCase):
    def test_run(self):
        # Define local experiment class
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_class"] = "MeasureRequestScale_8KB_16"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_request_scale_seqwrite_8KB_16",
            lbabytes=1024*MB
        )
        para.update({
            "device_path": "/dev/sdc2",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestRequestScale_8KB_32(unittest.TestCase):
    def test_run(self):
        # Define local experiment class
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_class"] = "MeasureRequestScale_8KB_32"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_request_scale_seqwrite_8KB_32",
            lbabytes=1024*MB
        )
        para.update({
            "device_path": "/dev/sdc2",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestRequestScale_16KB_4(unittest.TestCase):
    def test_run(self):
        # Define local experiment class
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_class"] = "MeasureRequestScale_16KB_4"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_request_scale_seqwrite_16KB_4",
            lbabytes=1024*MB
        )
        para.update({
            "device_path": "/dev/sdc2",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestRequestScale_16KB_8(unittest.TestCase):
    def test_run(self):
        # Define local experiment class
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_class"] = "MeasureRequestScale_16KB_8"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_request_scale_seqwrite_16KB_8",
            lbabytes=1024*MB
        )
        para.update({
            "device_path": "/dev/sdc2",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestRequestScale_16KB_16(unittest.TestCase):
    def test_run(self):
        # Define local experiment class
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_class"] = "MeasureRequestScale_16KB_16"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_request_scale_seqwrite_16KB_16",
            lbabytes=1024*MB
        )
        para.update({
            "device_path": "/dev/sdc2",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestRequestScale_16KB_32(unittest.TestCase):
    def test_run(self):
        # Define local experiment class
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf["workload_class"] = "MeasureRequestScale_16KB_32"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_request_scale_seqwrite_16KB_32",
            lbabytes=1024*MB
        )
        para.update({
            "device_path": "/dev/sdc2",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

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

# Note: Class name TestAlignment_{n} means n% of the block writes are aligned

class TestAlignmentGenerateTraces_0(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "MeasureAlignment_0"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_alignment_0_traces",
            lbabytes=16*MB
        )
        para.update({
            "device_path": "/dev/sdc1",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestAlignment_0(unittest.TestCase):
    def test(self):
        for para in rule_parameter.ParaDict(
                expname='test_alignment_0',
                trace_expnames=['test_alignment_0_traces'],
                rule='alignment',
            ):
            experiment.execute_simulation(para)

class TestAlignmentGenerateTraces_20(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "MeasureAlignment_20"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_alignment_20_traces",
            lbabytes=16*MB
        )
        para.update({
            "device_path": "/dev/sdc1",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestAlignment_20(unittest.TestCase):
    def test(self):
        for para in rule_parameter.ParaDict(
                expname='test_alignment_20',
                trace_expnames=['test_alignment_20_traces'],
                rule='alignment',
            ):
            experiment.execute_simulation(para)

class TestAlignmentGenerateTraces_40(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "MeasureAlignment_40"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_alignment_40_traces",
            lbabytes=16*MB
        )
        para.update({
            "device_path": "/dev/sdc1",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestAlignment_40(unittest.TestCase):
    def test(self):
        for para in rule_parameter.ParaDict(
                expname='test_alignment_40',
                trace_expnames=['test_alignment_40_traces'],
                rule='alignment',
            ):
            experiment.execute_simulation(para)

class TestAlignmentGenerateTraces_60(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "MeasureAlignment_60"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_alignment_60_traces",
            lbabytes=16*MB
        )
        para.update({
            "device_path": "/dev/sdc1",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestAlignment_60(unittest.TestCase):
    def test(self):
        for para in rule_parameter.ParaDict(
                expname='test_alignment_60',
                trace_expnames=['test_alignment_60_traces'],
                rule='alignment',
            ):
            experiment.execute_simulation(para)

class TestAlignmentGenerateTraces_80(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "MeasureAlignment_80"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_alignment_80_traces",
            lbabytes=16*MB
        )
        para.update({
            "device_path": "/dev/sdc1",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestAlignment_80(unittest.TestCase):
    def test(self):
        for para in rule_parameter.ParaDict(
                expname='test_alignment_80',
                trace_expnames=['test_alignment_80_traces'],
                rule='alignment',
            ):
            experiment.execute_simulation(para)

class TestAlignmentGenerateTraces_100(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['age_workload_class'] = "RandomWrite"
                self.conf['workload_class'] = "MeasureAlignment_100"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_alignment_100_traces",
            lbabytes=16*MB
        )
        para.update({
            "device_path": "/dev/sdc1",
            "ftl": "ftlcounter",
            "enable_simulation": True,
            "dump_ext4_after_workload": True,
            "only_get_traffic": False,
            "trace_issue_and_complete": True,
        })
        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestAlignment_100(unittest.TestCase):
    def test(self):
        for para in rule_parameter.ParaDict(
                expname='test_alignment_100',
                trace_expnames=['test_alignment_100_traces'],
                rule='alignment',
            ):
            experiment.execute_simulation(para)

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

class GenerateSeqWriteFwdReadTrace(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "SequentialWriteForwardRead"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_seq_write_fwd_read_trace",
            lbabytes=16*MB
        )
        para.update({
            'device_path': "/dev/sdc3",
            'ftl' : 'ftlcounter',
            'enable_simulation': True,
            'dump_ext4_after_workload': True,
            'only_get_traffic': False,
            'trace_issue_and_complete': True,
        })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestSeqWriteFwdReadLocality(unittest.TestCase):
    def test(self):
        for para in rule_parameter.ParaDict(
            expname="test_seq_write_fwd_read",
            trace_expnames=["test_seq_write_fwd_read_trace"],
            rule="locality"
        ):
            experiment.execute_simulation(para)

        for para in rule_parameter.ParaDict(
            expname="test_seq_write_fwd_read",
            trace_expnames=["test_seq_write_fwd_read_trace"],
            rule="localitysmall"
        ):
            experiment.execute_simulation(para)

class GenerateSeqWriteBckReadTrace(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "SequentialWriteBackwardRead"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_seq_write_bck_read_trace",
            lbabytes=16*MB
        )
        para.update({
            'device_path': "/dev/sdc3",
            'ftl' : 'ftlcounter',
            'enable_simulation': True,
            'dump_ext4_after_workload': True,
            'only_get_traffic': False,
            'trace_issue_and_complete': True,
        })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestSeqWriteBckReadLocality(unittest.TestCase):
    def test(self):
        for para in rule_parameter.ParaDict(
            expname="test_seq_write_bck_read",
            trace_expnames=["test_seq_write_bck_read_trace"],
            rule="locality"
        ):
            experiment.execute_simulation(para)

        for para in rule_parameter.ParaDict(
            expname="test_seq_write_bck_read",
            trace_expnames=["test_seq_write_bck_read_trace"],
            rule="localitysmall"
        ):
            experiment.execute_simulation(para)

class GenerateRandWriteRandReadTrace(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "RandomWriteRandomRead"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_rand_write_rand_read_trace",
            lbabytes=16*MB
        )
        para.update({
            'device_path': "/dev/sdc3",
            'ftl' : 'ftlcounter',
            'enable_simulation': True,
            'dump_ext4_after_workload': True,
            'only_get_traffic': False,
            'trace_issue_and_complete': True,
        })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestRandWriteRandReadLocality(unittest.TestCase):
    def test(self):
        for para in rule_parameter.ParaDict(
            expname="test_rand_write_rand_read",
            trace_expnames=["test_rand_write_rand_read_trace"],
            rule="locality"
        ):
            experiment.execute_simulation(para)

        for para in rule_parameter.ParaDict(
            expname="test_rand_write_rand_read",
            trace_expnames=["test_rand_write_rand_read_trace"],
            rule="localitysmall"
        ):
            experiment.execute_simulation(para)

############################################
## Test Grouping by Death Time - Attempt 1
############################################

class GenerateGroupingWorkloadTrace(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "GroupingWorkload"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_grouping_workload_trace",
            lbabytes=16*MB
        )
        para.update({
            'device_path': "/dev/sdc1",
            'ftl' : 'ftlcounter',
            'enable_simulation': True,
            'dump_ext4_after_workload': True,
            'only_get_traffic': False,
            'trace_issue_and_complete': True,
        })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestGroupingWorkload(unittest.TestCase):
    def test(self):
        for para in rule_parameter.ParaDict(
            expname="test_grouping_workload",
            trace_expnames=["test_grouping_workload_trace"],
            rule="grouping"
        ):
            experiment.execute_simulation(para)

class GenerateNonGroupingWorkloadTrace(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "NonGroupingWorkload"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_non_grouping_workload_trace",
            lbabytes=16*MB
        )
        para.update({
            'device_path': "/dev/sdc2",
            'ftl' : 'ftlcounter',
            'enable_simulation': True,
            'dump_ext4_after_workload': True,
            'only_get_traffic': False,
            'trace_issue_and_complete': True,
        })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestNonGroupingWorkload(unittest.TestCase):
    def test(self):
        for para in rule_parameter.ParaDict(
            expname="test_non_grouping_workload",
            trace_expnames=["test_non_grouping_workload_trace"],
            rule="grouping"
        ):
            experiment.execute_simulation(para)

############################################
## Test Grouping by Death Time - Attempt 2
############################################

class GenerateGroupingWorkloadTraceNew(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "GroupingWorkloadNew"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_grouping_workload_new_trace",
            lbabytes=16*MB
        )
        para.update({
            'device_path': "/dev/sdc1",
            'ftl' : 'ftlcounter',
            'enable_simulation': True,
            'dump_ext4_after_workload': True,
            'only_get_traffic': False,
            'trace_issue_and_complete': True,
        })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestGroupingWorkloadNew(unittest.TestCase):
    def test(self):
        for para in rule_parameter.ParaDict(
            expname="test_grouping_workload_new",
            trace_expnames=["test_grouping_workload_new_trace"],
            rule="grouping"
        ):
            experiment.execute_simulation(para)

class GenerateNonGroupingWorkloadTraceNew(unittest.TestCase):
    def test_run(self):
        class LocalExperiment(experiment.Experiment):
            def setup_workload(self):
                self.conf['workload_class'] = "NonGroupingWorkloadNew"

        para = experiment.get_shared_nolist_para_dict(
            expname="test_non_grouping_workload_new_trace",
            lbabytes=16*MB
        )
        para.update({
            'device_path': "/dev/sdc2",
            'ftl' : 'ftlcounter',
            'enable_simulation': True,
            'dump_ext4_after_workload': True,
            'only_get_traffic': False,
            'trace_issue_and_complete': True,
        })

        Parameters = collections.namedtuple("Parameters", ','.join(para.keys()))
        obj = LocalExperiment(Parameters(**para))
        obj.main()

class TestNonGroupingWorkloadNew(unittest.TestCase):
    def test(self):
        for para in rule_parameter.ParaDict(
            expname="test_non_grouping_workload_new",
            trace_expnames=["test_non_grouping_workload_new_trace"],
            rule="grouping"
        ):
            experiment.execute_simulation(para)

if __name__ == '__main__':
    unittest.main()
