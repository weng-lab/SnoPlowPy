#!/usr/bin/env python

from __future__ import print_function

import os, stat, sys, shutil, json, re, subprocess, errno, gzip, tarfile, zipfile
import urllib, tempfile
import time
from collections import defaultdict
from itertools import groupby
from subprocess import Popen, PIPE
import hashlib
import urllib2

# cluster may not have access to requests package
# this avoids reinstalling with every job
try:
    from requests.auth import HTTPBasicAuth
    import requests
    import dateutil.parser
except:
    from files_and_paths import Dirs
    sys.path.append(Dirs.ToolsFnp("python2.7"))
    import requests
    from requests.auth import HTTPBasicAuth
    import dateutil.parser

def pflush(s):
    print(s, end="")
    sys.stdout.flush()

def printWroteNumLines(fnp):
    print("\twrote", fnp, '(' + "{:,}".format(numLines(fnp)) +' lines)')

def cat(fnp):
    if fnp.endswith(".gz"):
        return "zcat " + fnp
    return "cat " + fnp

def numLines(fnp):
    cmds = [cat(fnp), "| wc -l"]
    return int(Utils.runCmds(cmds)[0])

def usage():
    print("USUAGE:")
    print(os.path.basename(sys.argv[0]), "submit")

def readFile(fnp, needFile = True):
    if not os.path.exists(fnp):
        if needFile:
            print("ERROR: file missing:")
            print(fnp)
            sys.exit(1)
        return None
    with open(fnp) as f:
        return f.readlines()

def status(cr):
    outFolder, errFolder = cr.logFolders()

    batchLog = readFile(cr.batchSubmitLogFnp)
    match = re.search(r"Job\ <(\d+)>.*", batchLog[0])
    if match:
        jobID = match.group(1)
    else:
        print("could not find jobID:")
        print(batchLog)
        sys.exit(1)

    numDone = 0
    numError = 0
    output = []
    for i, cmd in enumerate(cr.all_cmds):
        idx = str(i + 1)

        status = "..."

        errFnp = os.path.join(errFolder, ".".join([jobID, idx]))
        errors = readFile(errFnp, False)
        if errors:
            #status = "ERROR: " + " ".join(errors)
            status = "ERROR " + errFnp
            numError += 1
        else:
            outFnp = os.path.join(outFolder, ".".join([jobID, idx]))
            out = readFile(outFnp, False)
            if out:
                if "Successfully completed." in " ".join(out):
                    status = None
                    numDone += 1
        if status:
            output.append([i + 1, status])

    ongoing = filter(lambda x: x[1] == "...", output)
    rest = filter(lambda x: x[1] != "...", output)
    for r in ranges([x[0] for x in ongoing]):
        if r[0] == r[1]:
            rest.append([r[0], "..."])
        else:
            rest.append([r[0], "to", r[1], "..."])

    for r in sorted(rest, key=lambda x: x[0]):
        print(" ".join([str(x) for x in r]))

    print("completed:", numDone, "of", len(cr.all_cmds))
    if numError:
        print("error:", numError)

def doRun(crf, chromNum):
    cr = crf(chromNum)
    func = sys.argv[2]

    if "status" == func:
        return status(cr)
    if "submit" == func:
        return cr.setupCluster()

    idx = int(func) - 1
    return cr.runCmd(idx)

def runCr(crf):
    if 3 != len(sys.argv):
        usage()
        sys.exit(1)

    chromNum = sys.argv[1]
    if "all" == chromNum:
        for chromNum in range(1, 23) + ["X", "Y"]:
            print("chr", chromNum, "**************")
            try:
                doRun(crf, chromNum)
            except:
                pass
    else:
        doRun(crf, chromNum)

def request_until_success(url, request_method, headers={}, data=None, attempt_limit=100):
    attempts = 0
    while True:
        try:
            return request_method(url, headers=headers, data=data)
        except:
            attempts += 1
            if attempts >= attempt_limit: raise

def get_request_until_success(url, attempt_limit=100):
    return request_until_success(url, requests.get, attempt_limit=attempt_limit)

def post_request_until_success(url, headers={}, data="", attempt_limit=100):
    return request_until_success(url, requests.post, headers=headers, data=data, attempt_limit=attempt_limit)

def put_request_until_success(url, headers={}, data="", attempt_limit=10):
    return request_until_success(url, requests.put, headers=headers, data=data, attempt_limit=attempt_limit)

class ChromImputeRunnerBase:
    def __init__(self, chromNum):
        self.chromNum = chromNum
        self.chrom = "chr" + str(chromNum)
        self.metaFnp = os.path.join(BASEDIR, "cellType_mark_table.{chrom}.txt".format(chrom=self.chrom))
        self.chromFnp = os.path.join(BASEDIR, "hg19sizes_{chrom}.txt".format(chrom=self.chrom))

        self.baseDir = os.path.join(BASEDIR, self.chrom)
        self.inputDir = os.path.join(self.baseDir, "input")
        self.trainDir = os.path.join(self.baseDir, "trainingData")
        self.predictDir = os.path.join(self.baseDir, "predictor")
        self.convertedDataDir = os.path.join(self.baseDir, "convertedData")
        self.distanceDir = os.path.join(self.baseDir, "distance")
        self.outputDir = os.path.join(self.baseDir, "output")
        self.evalDir = os.path.join(self.baseDir, "eval")
        self.peaksDir = os.path.join(self.baseDir, "peaks")

        self.queue = "long"

        for d in [self.inputDir, self.trainDir, self.predictDir,
                  self.convertedDataDir,
                  self.distanceDir, self.outputDir, self.evalDir,
                  self.peaksDir, self.outputDir + ".bigWig",
                  self.outputDir + ".bedGraph"]:
            if not os.path.exists(d):
                os.makedirs(d)

        with open(self.metaFnp) as f:
            self.meta = [line.strip().split("\t") for line in f]

    def runCmd(self, idx):
        cmds = self.all_cmds[idx]
        print("running", " ".join(cmds))
        return runCmds(cmds)

    def logFolders(self):
        logDir = os.path.join(LOGDIR, "chromImpute/")
        logDir = os.path.join(logDir, self.stageName)
        logDir = os.path.join(logDir, self.chrom)
        outFolder = os.path.join(logDir, "out")
        errFolder = os.path.join(logDir, "err")
        return outFolder, errFolder

    def setupCluster(self):
        outFolder, errFolder = self.logFolders()

        if not os.path.exists(outFolder):
            os.makedirs(outFolder)
        if not os.path.exists(errFolder):
            os.makedirs(errFolder)

        batchSubmit = batchCmd(self.all_cmds, errFolder, outFolder,
                               self.thisCmd, self.mem, self.time, self.chromNum,
                               self.cores, self.queue)

        batchSubmitFnp = os.path.join(BSUBSDIR, self.stageName + "." + self.chrom + ".sh")
        with open(batchSubmitFnp, 'w') as f:
            f.write(batchSubmit)

        cmds = ["bsub <", batchSubmitFnp]
        bsubRet = runCmds(cmds)
        print("submitted", batchSubmitFnp)

        with open(self.batchSubmitLogFnp, 'w') as f:
            f.write(bsubRet)
        print("wrote", self.batchSubmitLogFnp)

def ranges(i):
    # http://stackoverflow.com/a/4629241
    for a, b in itertools.groupby(enumerate(i), lambda x, y: y - x):
        b = list(b)
        yield b[0][1], b[-1][1]

class Utils:
    FileUmask = stat.S_IRUSR | stat.S_IWUSR # user read/write
    FileUmask |= stat.S_IRGRP | stat.S_IWGRP # group read/write
    FileUmask |= stat.S_IROTH # others read

    DirUmask = stat.S_IRUSR | stat.S_IWUSR # user read/write
    DirUmask |= stat.S_IRGRP | stat.S_IWGRP # group read/write
    DirUmask |= stat.S_IROTH # others read
    
    @staticmethod
    def deleteFileIfSizeNotMatch(fnp, file_size_bytes):
        if not os.path.exists(fnp):
            return
        if not file_size_bytes:
            return
        if os.path.getsize(fnp) == file_size_bytes:
            return
        os.remove(fnp)

    @staticmethod
    def get_file_if_size_diff(url, d):
        fn = url.split('/')[-1]
        out_fnp = os.path.join(d, fn)
        net_file_size = int(urllib.urlopen(url).info()['Content-Length'])
        if os.path.exists(out_fnp):
            fn_size = os.path.getsize(out_fnp)
            if fn_size == net_file_size:
                print("skipping download of", fn)
                return out_fnp
            else:
                print("files sizes differed:")
                print("\t", "on disk:", fn_size)
                print("\t", "from net:", net_file_size)
        print("retrieving", fn)
        urllib.urlretrieve(url, out_fnp)
        return out_fnp

    @staticmethod
    def getHttpFileSizeBytes(url, auth):
        if url.startswith("ftp://"):
            return None

        if not auth:
            r = requests.head(url)
        if auth or 403 == r.status_code:
            keyFnp = os.path.expanduser('~/.encode.txt')
            if os.path.exists(keyFnp):
                with open(keyFnp) as f:
                    toks = f.read().strip().split('\n')
                r = requests.head(url, auth=HTTPBasicAuth(toks[0], toks[1]))
            else:
                raise Exception("no ENCODE password file found at: " +
                                keyFnp)
        if 200 != r.status_code:
            print("could not get file size for", url)
            print("status_code:", r.status_code)
            return None

		# does not (always) work
        if "Content-Length" in r.headers:
            return int(r.headers["Content-Length"])
        else:
            return -1 # invalid filesize...

    @staticmethod
    def quietPrint(quiet, *args):
        if not quiet:
            print(*args)

    @staticmethod
    def download(url, fnp, auth=None, force=None,
                 file_size_bytes=0, skipSizeCheck=None,
                 quiet = False, umask = FileUmask):
        Utils.ensureDir(fnp)
        fn = os.path.basename(fnp)
        if not skipSizeCheck:
            if 0 == file_size_bytes:
                fsb = Utils.getHttpFileSizeBytes(url, auth)
                if fsb:
                    file_size_bytes = fsb
            Utils.deleteFileIfSizeNotMatch(fnp, file_size_bytes)

        if os.path.exists(fnp):
            if force:
                os.remove(fnp)
            else:
                return True

        Utils.quietPrint(quiet, "downloading", url, "...")

        if url.startswith("ftp://"):
            fnpTmp = urllib.urlretrieve(url)[0]
            shutil.move(fnpTmp, fnp)
            # chmod g+w
            st = os.stat(fnp)
            os.chmod(fnp, st.st_mode | umask)
            return True

        if not auth:
            r = requests.get(url)
        if auth or 403 == r.status_code:
            keyFnp = os.path.expanduser('~/.encode.txt')
            if os.path.exists(keyFnp):
                with open(keyFnp) as f:
                    toks = f.read().strip().split('\n')
                r = requests.get(url, auth=HTTPBasicAuth(toks[0], toks[1]))
            else:
                raise Exception("no ENCODE password file found at: " +
                                keyFnp)
        if 200 != r.status_code:
            Utils.quietPrint(quiet, "could not download", url)
            Utils.quietPrint(quiet, "status_code:", r.status_code)
            return False

        #with open(fnpTmp, "wb") as f:
        with tempfile.NamedTemporaryFile("wb", delete=False) as f:
            f.write(r.content)
            fnpTmp = f.name
        shutil.move(fnpTmp, fnp)
        # chmod g+w
        st = os.stat(fnp)
        os.chmod(fnp, st.st_mode | umask)
        return True

    @staticmethod
    def query(url, auth=None, quiet=False):
        Utils.quietPrint(quiet, "downloading", url, "...")

        if not auth:
            r = requests.get(url)
        if auth or 403 == r.status_code:
            keyFnp = os.path.expanduser('~/.encode.txt')
            if os.path.exists(keyFnp):
                with open(keyFnp) as f:
                    toks = f.read().strip().split('\n')
                r = requests.get(url, auth=HTTPBasicAuth(toks[0], toks[1]))
            else:
                raise Exception("no ENCODE password file found at: " +
                                keyFnp)
        if 200 != r.status_code:
            Utils.quietPrint(quiet, "could not download", url)
            Utils.quietPrint(quiet, "status_code:", r.status_code)
            return None

        return r.content

    @staticmethod
    def sanitize(s):
        return re.sub(r'\W+', '', s).lower()

    @staticmethod
    def ensureDir(fnp, umask = DirUmask):
        d = os.path.dirname(fnp)
        if d != "" and not os.path.exists(d):
            Utils.mkdir_p(d, umask)
        return d

    @staticmethod
    def mkdir(path, umask = DirUmask):
        try:
            os.mkdir(path)
            # chmod g+w
            st = os.stat(path)
            os.chmod(path, st.st_mode | umask)
        except OSError as exc: # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    @staticmethod
    def mkdir_p(path, umask = DirUmask):
        abspath = os.path.abspath(path)
        dirname = os.path.dirname(abspath)
        if dirname != abspath: # i.e. dirname("/") == "/"
            Utils.mkdir_p(dirname, umask)
        Utils.mkdir(abspath, umask)

    @staticmethod
    def run_in_dir(cmd, d):
        cmd = "cd " + Utils.shellquote(d) + " && " + cmd + " && cd -"
        Utils.run(cmd)

    @staticmethod
    def run(cmd):
        # from http://stackoverflow.com/a/4418193
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # Poll process for new output until finished
        while True:
            nextline = process.stdout.readline()
            if nextline == '' and process.poll() != None:
                break
            sys.stdout.write(nextline)
            sys.stdout.flush()

        output = process.communicate()[0]
        exitCode = process.returncode

        if (exitCode == 0):
            return output
        raise Exception(cmd, exitCode, output)

    @staticmethod
    def runCmds(cmds, verbose = False, cwd=None):
        cmd = " ".join(cmds)
        if verbose:
            print("running: ", cmd)

        ret = []

        # from http://stackoverflow.com/a/4418193
        if cwd is not None:
            process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, cwd=cwd,
                                       stderr=subprocess.STDOUT, executable='/bin/bash')
        else:
            process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, executable='/bin/bash')
        # Poll process for new output until finished
        while True:
            nextline = process.stdout.readline()
            if nextline == '' and process.poll() != None:
                break
            if nextline:
                ret.append(nextline)
                if verbose:
                    print(nextline)

        output = process.communicate()[0]
        exitCode = process.returncode

        if (exitCode == 0):
            return ret
        print("ERROR\noutput was:\n", output, file=sys.stderr)
        print("exitCode:", exitCode, file=sys.stderr)
        raise Exception(cmd, exitCode, output)

    @staticmethod
    def gzfilelen(fname):
        try:
            with gzip.open(fname) as f:
                for i, l in enumerate(f):
                    pass
            return i + 1
        except:
            return 0

    @staticmethod
    def titleCase(s):
        # http://stackoverflow.com/a/3729957
        articles = ['a', 'an', 'of', 'the', 'is']
        exceptions = articles
        word_list = re.split(' ', s)       #re.split behaves as expected
        final = [word_list[0].capitalize()]
        for word in word_list[1:]:
            final.append(word in exceptions and word or word.capitalize())
        return " ".join(final)

    @staticmethod
    def color(json, force=None):
        # https://stackoverflow.com/questions/25638905/coloring-json-output-in-python
        if force or sys.stdout.isatty():
            try:    # if we can load pygments, return color
                from pygments import highlight, lexers, formatters
                colorful_json = highlight(unicode(json, 'UTF-8'), lexers.JsonLexer(),
                    formatters.TerminalFormatter())
                return colorful_json
            except: # otherwise, return original json
                pass
        return json

    @staticmethod
    def pager(text):
        # be nice
        pager = os.getenv('PAGER')
        if not pager:
            pager = ['less', '-F', '-R', '-X']
        p = Popen(pager, stdin=PIPE)
        try:
            p.stdin.write(text)
        except IOError as e:
            if e.errno == errno.EPIPE or e.errno == errno.EINVAL:
                pass
            else:
                raise
        p.stdin.close()
        p.wait()

    @staticmethod
    def md5(fnp, chunk_size=1048576):
        # from http://stackoverflow.com/a/3431838
        _hex = hashlib.md5()
        with open(fnp, 'rb') as f:
            for chunk in iter(lambda: f.read(chunk_size), b''):
                _hex.update(chunk)
        return _hex.hexdigest()

    @staticmethod
    def is_gzipped(fnp):
        return open(fnp, 'rb').read(2) == b'\x1f\x8b'

    @staticmethod
    def sortFile(fnp):
        cmds = ["sort", "-o", fnp, "-k1,1 -k2,2n", fnp]
        return Utils.runCmds(cmds)

    @staticmethod
    def checkIfUrlExists(url):
        # http://stackoverflow.com/a/7347995
        try:
            ret = urllib2.urlopen(url)
            return ret.code == 200
        except:
            return False

    @staticmethod
    def touch(fnp):
        if not os.path.exists(os.path.dirname(fnp)):
            os.makedirs(os.path.dirname(fnp))
        with open(fnp, "w") as f:
            f.write(" ")
            f.close()

    @staticmethod
    def ftouch(fnp):
        if not os.path.exists(os.path.dirname(fnp)):
            os.makedirs(os.path.dirname(fnp))
        Utils.touch(fnp)

    @staticmethod
    def fileOlderThanDays(fnp, days):
        # from http://stackoverflow.com/a/5799209
        if not os.path.exists(fnp):
            return True
        import datetime
        today_dt = datetime.datetime.today()
        modified_dt = datetime.datetime.fromtimestamp(os.path.getmtime(fnp))
        duration = today_dt - modified_dt
        return duration.days > days

    @staticmethod
    def getStringFromListOrString(s):
        if isinstance(s, basestring):
            return s
        toks = list(set(s))
        if 1 == len(toks):
            return toks[0]
        print("warning: multiple fields found: " + ", ".join(s))
        return toks[0]

    @staticmethod
    def which(program):
        # from http://stackoverflow.com/a/377028
        def is_exe(fpath):
            return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

        fpath, fname = os.path.split(program)
        if fpath:
            if is_exe(program):
                return program
        else:
            for path in os.environ["PATH"].split(os.pathsep):
                path = path.strip('"')
                exe_file = os.path.join(path, program)
                if is_exe(exe_file):
                    return exe_file

        return None

    @staticmethod
    def rm_rf(d):
        if os.path.exists(d):
            print("rm -rf", d)
            shutil.rmtree(d)

    @staticmethod
    def num_cores():
        import multiprocessing
        return multiprocessing.cpu_count()

    @staticmethod
    def un_xz_tar(fnp, d):
        cmds = ["tar", "xvf", fnp, "-C", d]
        Utils.run(' '.join(cmds))

    @staticmethod
    def untar(fnp, d):
        if fnp.endswith(".tar.gz"):
            tar = tarfile.open(fnp, "r:gz", errorlevel=2)
        elif fnp.endswith(".tar.xz"):
            return Utils.un_xz_tar(fnp, d)
        elif fnp.endswith(".tar.bz2"):
            tar = tarfile.open(fnp, "r:bz2", errorlevel=2)
        elif fnp.endswith(".tar"):
            tar = tarfile.open(fnp, "r", errorlevel=2)
        elif fnp.endswith(".zip"):
            with zipfile.ZipFile(fnp, "r") as z:
                z.extractall(d)
                return
        else:
            raise Exception("invalid file? " + fnp)
        print("untarring", fnp, "to", d)
        tar.extractall(d)
        tar.close()

    @staticmethod
    def clear_dir(d):
        Utils.rm_rf(d)
        Utils.mkdir_p(d)

    @staticmethod
    def shellquote(s):
        " from http://stackoverflow.com/a/35857"
        return "'" + s.replace("'", "'\\''") + "'"

    @staticmethod
    def merge_two_dicts(x, y):
        # http://stackoverflow.com/a/26853961
        # Given two dicts, merge into a new dict using shallow copy
        z = x.copy()
        z.update(y)
        return z

    @staticmethod
    def dictCompare(d1, d2):
        # from http://stackoverflow.com/a/18860653
        d1_keys = set(d1.keys())
        d2_keys = set(d2.keys())
        intersect_keys = d1_keys.intersection(d2_keys)
        added = d1_keys - d2_keys
        removed = d2_keys - d1_keys
        modified = {o : (d1[o], d2[o]) for o in intersect_keys if d1[o] != d2[o]}
        same = set(o for o in intersect_keys if d1[o] == d2[o])
        return added, removed, modified, same

    @staticmethod
    def uuidStr():
        import uuid
        return str(uuid.uuid4())

    @staticmethod
    def timeDateStr():
        # from http://stackoverflow.com/a/10607768
        # ex:  20120515-155045
        import time
        return time.strftime("%Y%m%d-%H%M%S")

    @staticmethod
    def remove_non_ascii(s):
        # http://drumcoder.co.uk/blog/2012/jul/13/removing-non-ascii-chars-string-python/
        return "".join(i for i in s if ord(i)<128)

class Timer(object):
    # http://stackoverflow.com/a/5849861
    def __init__(self, name=None):
        self.name = name

    def __enter__(self):
        self.tstart = time.time()

    def __exit__(self, type, value, traceback):
        if self.name:
            print('[%s]' % self.name,)
        print('Elapsed: %s' % (time.time() - self.tstart))

class UtilsTests:
    def numLines(self):
        n = 173
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            fnpTmp = f.name
            for i in xrange(n):
                f.write("hi " + str(i) + "\n")
        count = numLines(fnpTmp)
        if count != n:
            print("numLines returned", count, "but n is", n)
            assert(count == n)
        os.remove(fnpTmp)

class dotdict(dict):
    # dot.notation access to dictionary attributes
    # http://stackoverflow.com/a/23689767
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

def main():
    ut = UtilsTests()
    ut.numLines()

if __name__ == "__main__":
    sys.exit(main())

