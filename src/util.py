import subprocess
from os.path import join
from datetime import datetime


def str2date(date_str, fmt="%Y%m%d"):
    return datetime.datetime.strptime(date_str, fmt).date()


def date2str(date, fmt="%Y%m%d"):
    return date.strftime(fmt)


def date_before(date, days=1):
    return date - datetime.timedelta(days=days)


def date_after(date, days=1):
    return date + datetime.timedelta(days=days)


def log():
    def wrapper1(func):
        def wrapper2(*args, **kwargs):
            import logging
            info = func.__name__
            logging.info(info + " begins.")
            rlt = func(*args, **kwargs)
            logging.info(info + " ends.")
            return rlt
        return wrapper2
    return wrapper1


def check_log():
    def wrapper1(func):
        def wrapper2(*args, **kwargs):
            import logging
            info = func.__name__
            logging.info(info + " begins.")
            rc = func(*args, **kwargs)
            if rc == 0:
                logging.info(info + " succeed.")
            else:
                import os
                logging.error(info + " failed.")
                os._exit(255)
        return wrapper2
    return wrapper1


class HDFSUtil(object):
    HADOOP = 'hadoop'

    @staticmethod
    def exists(dir):
        cmd = [HDFSUtil.HADOOP, "fs", "-test", "-e", dir]
        rc = subprocess.call(cmd)
        return 0 == rc

    @staticmethod
    def is_dir(dir):
        cmd = [HDFSUtil.HADOOP, "fs", "-test", "-d", dir]
        rc = subprocess.call(cmd)
        return 0 == rc

    @staticmethod
    def is_file(dir):
        cmd = [HDFSUtil.HADOOP, "fs", "-test", "-f", dir]
        rc = subprocess.call(cmd)
        return 0 == rc

    @staticmethod
    def is_empty(dir):
        cmd = [HDFSUtil.HADOOP, "fs", "-test", "-s", dir]
        rc = subprocess.call(cmd)
        return 0 == rc

    @staticmethod
    def rm(dir):
        cmd = [HDFSUtil.HADOOP, "fs", "-rm", "-r", "-f", dir]
        return subprocess.call(cmd)

    @staticmethod
    def mkdir(dir):
        cmd = [HDFSUtil.HADOOP, "fs", "-mkdir", "-p", dir]
        return subprocess.call(cmd)

    @staticmethod
    def count_lines(filename):
        if HDFSUtil.is_dir(filename):
            filename = join(filename, "*")
        cmd1 = [HDFSUtil.HADOOP, "fs", "-cat", filename]
        cmd2 = ["wc", "-l"]
        p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE)
        p2 = subprocess.Popen(cmd2, stdin=p1.stdout, stdout=subprocess.PIPE)
        p1.stdout.close()
        return int(p2.communicate()[0])

    @staticmethod
    def append_to_file(dst, *local_files):
        cmd = [HDFSUtil.HADOOP, "fs", "-appendToFile"]
        if type(local_files[0]) == str:
            cmd += local_files
            cmd.append(dst)
            return subprocess.call(cmd)
        else:
            cmd += ["-", dst]
            p = subprocess.Popen(cmd, stdin=local_files[0], stdout=subprocess.PIPE)
            local_files[0].close()
            rc = p.communicate()[0]
            print
            type(rc), rc
            return rc

    @staticmethod
    def copy_from_local(local_file, dst):
        pass

    @staticmethod
    def multi_dir(basename, end_date, history_days, fmt="%Y%m%d"):
        """
        @param:
        basename:
        last_date:
        history_days:
        """
        from datetime import datetime
        dates = map(lambda i: end_date - datetime.timedelta(days=i), range(history_days))
        dirs = [join(basename, date.strftime(fmt)) for date in dates]
        return ",".join(dirs)

    @staticmethod
    def list_files(dir):
        cmd = [HDFSUtil.HADOOP, "fs", "-ls", dir]
        p1 = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        p2 = subprocess.Popen(
            ["awk", "{if(NR>1)print $NF}"],
            stdin=p1.stdout,
            stdout=subprocess.PIPE)
        p1.stdout.close()
        files = p2.communicate()[0].strip("\n").split("\n")
        return files


class HDFSStream(object):
    def __init__(self, path_hdfs):
        self.path_hdfs = path_hdfs

    def __iter__(self, map_fn=lambda x: x):
        import codecs
        UTF8Reader = codecs.getreader("utf-8")
        doc_stream = subprocess.Popen([HDFSUtil.HADOOP, "fs", "-cat", self.path_hdfs], stdout=subprocess.PIPE)
        for line in UTF8Reader(doc_stream.stdout):
            yield map_fn(line)


class ConfigUtil(object):
    def __init__(self, config, section="DEFAULT"):
        self.config = config
        self.sec = section

    def get(self, name):
        return self.config.get(self.sec, name)

    def get_int(self, name):
        return self.config.getint(self.sec, name)

    def get_float(self, name):
        return self.config.getfloat(self.sec, name)

    def get_boolean(self, name):
        return self.config.getboolean(self.sec, name)

    def items(self):
        return self.config.items(self.sec)

    def get_list(self, name, sep=",", dtype=str):
        lst = self.config.get(self.sec, name).split(sep)
        return [dtype(item) for item in lst]


class HiveUtil(object):
    HIVE = "hive"

    @staticmethod
    def run(script_file, **kwargs):
        #cmd = [HiveUtil.HIVE]
        cmd = [HiveUtil.HIVE, "--hiveconf", "mapred.job.name=feicha.ymm", "--hiveconf", "mapreduce.job.queuename=%s" % "root.normal"]
        for k, v in kwargs.items():
            cmd.extend(["-d", "{}={}".format(k, v)])
        cmd.extend(["-f", script_file])
        rc = subprocess.call(cmd)
        return rc


class SparkUtil(object):
    SPARK = 'spark-submit'

    @staticmethod
    def default_conf():
        return {
            "parallelism": 1200,
            "master": "yarn-cluster",
            "memory_overhead": 5120,
            "queue": "root.normal",
            "driver_memory": "20g",
            "executor_memory": "20g",
            "num_executors": 50,
            "executor_cores": 8,
            "job_name" : "feicha.ymm",
            "app_name" : "feicha.ymm"
        }

    @staticmethod
    def run(script_file, params, **kwargs):
        config = SparkUtil.default_conf()
        config.update(kwargs)
        cmd = [SparkUtil.SPARK,
               "--conf", "spark.default.parallelism={}".format(config["parallelism"]),
               "--conf", "spark.yarn.executor.memoryOverhead={}".format(config["memory_overhead"]),
               "--conf", "mapred.job.name={}".format(config["job_name"]),
               "--conf", "spark.app.name={}".format(config["app_name"]),
               "--master", config["master"],
               "--queue", config["queue"],
               "--driver-memory", config["driver_memory"],
               "--executor-memory", config["executor_memory"],
               "--num-executors", str(config["num_executors"]),
               "--executor-cores", str(config["executor_cores"])]
        if "files" in config:
            cmd.extend(["--files", config["files"]])
        if "py_files" in config:
            cmd.extend(["--py-files", config["py_files"]])
        if "archives" in config:
            cmd.extend(["--archives", config["archives"]])
        cmd.append(script_file)
        if params:
            cmd.extend(params)
        rc = subprocess.call(cmd)
        return rc


class IOUtil(object):
    @staticmethod
    def load_list(filename, dtype=str):
        import codecs
        import itertools
        with codecs.open(filename, 'r', encoding='utf-8') as fin:
            iter = itertools.imap(lambda line : line.strip(), fin)
            if not dtype == str:
                iter = itertools.imap(lambda x : dtype(x), iter)
            return list(iter)

    @staticmethod
    def load_set(filename, dtype=str):
        import codecs
        import itertools
        with codecs.open(filename, 'r', encoding='utf-8') as fin:
            iter = itertools.imap(lambda line : line.strip(), fin)
            if not dtype == str:
                iter = itertools.imap(lambda x : dtype(x), iter)
            return set(iter)

    def load_dict(filename, sep=u"\001", dtype_key=str, dtype_value=str):
        import codecs
        import itertools
        with codecs.open(filename, 'r', encoding='utf-8') as fin:
            iter = itertools.imap(lambda line : line.strip().split(sep), fin)
            iter = itertools.imap(lambda x : (dtype_key(x[0]), dtype_value(x[1])), iter)
            return dict(iter)

    @staticmethod
    def csvParser(fields, sep=u"\001"):
        def parser(line):
            segs = line.rstrip("\n").split(sep)
            if len(segs) > len(segs):
                return None
            else:
                return dict(zip(fields, segs))
        return parser
