#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import os
import tempfile

from snoPlowPy.utils import Utils, numLines

class TestUtils:
    def test_numLines(self):
        n = 173
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            fnpTmp = f.name
            for i in xrange(n):
                f.write("hi " + str(i) + "\n")
        count = numLines(fnpTmp)
        os.remove(fnpTmp)
        assert(count == n)

    def test_deleteFileIfSizeNotMatch(self):
        ints = [1,2,3]
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            fnpTmp = f.name
            f.write(bytes(ints))
        numBytes = 8 * len(ints)
        Utils.deleteFileIfSizeNotMatch(fnpTmp, numBytes)
        assert not os.path.exists(fnpTmp)

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
            return -1  # invalid filesize...

    @staticmethod
    def quietPrint(quiet, *args):
        if not quiet:
            print(*args)

    @staticmethod
    def download(url, fnp, auth=None, force=None,
                 file_size_bytes=0, skipSizeCheck=None,
                 quiet=False, umask=Utils.FileUmask):
        Utils.ensureDir(fnp)
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

        # with open(fnpTmp, "wb") as f:
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

    def test_sanitize(self):
        bad = "aBc1234%^&*"
        good = "aBc1234".lower()
        assert good == Utils.sanitize(bad)

    @staticmethod
    def ensureDir(fnp, umask=Utils.DirUmask):
        d = os.path.dirname(fnp)
        if d != "" and not os.path.exists(d):
            Utils.mkdir_p(d, umask)
        return d

    @staticmethod
    def mkdir(path, umask=Utils.DirUmask):
        try:
            os.mkdir(path)
            # chmod g+w
            st = os.stat(path)
            os.chmod(path, st.st_mode | umask)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    @staticmethod
    def mkdir_p(path, umask=Utils.DirUmask):
        abspath = os.path.abspath(path)
        dirname = os.path.dirname(abspath)
        if dirname != abspath:  # i.e. dirname("/") == "/"
            Utils.mkdir_p(dirname, umask)
        Utils.mkdir(abspath, umask)

    @staticmethod
    def run_in_dir(cmd, d):
        cmd = "cd " + Utils.shellquote(d) + " && " + cmd + " && cd -"
        Utils.run(cmd)

    @staticmethod
    def run(cmd):
        # from http://stackoverflow.com/a/4418193
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
        # Poll process for new output until finished
        while True:
            nextline = process.stdout.readline()
            if nextline == '' and process.poll() is not None:
                break
            sys.stdout.write(nextline)
            sys.stdout.flush()

        output = process.communicate()[0]
        exitCode = process.returncode

        if (exitCode == 0):
            return output
        raise Exception(cmd, exitCode, output)

    @staticmethod
    def runCmds(cmds, verbose=False, cwd=None):
        cmd = " ".join(cmds)
        if verbose:
            print("running: ", cmd)

        ret = []

        # from http://stackoverflow.com/a/4418193
        if cwd is not None:
            process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                       cwd=cwd, stderr=subprocess.STDOUT,
                                       executable='/bin/bash')
        else:
            process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT,
                                       executable='/bin/bash')
        # Poll process for new output until finished
        while True:
            nextline = process.stdout.readline()
            if nextline == '' and process.poll() is not None:
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

    def test_titleCase(self):
        old = "convert to titlecase"
        new = "Convert to Titlecase"
        assert new == Utils.titleCase(old)

        old = "a silly story"
        new = "A Silly Story"
        assert new == Utils.titleCase(old)
        
    @staticmethod
    def color(json, force=None):
        # https://stackoverflow.com/questions/25638905/coloring-json-output-in-python
        if force or sys.stdout.isatty():
            try:    # if we can load pygments, return color
                from pygments import highlight, lexers, formatters
                colorful_json = highlight(unicode(json, 'UTF-8'),
                                          lexers.JsonLexer(),
                                          formatters.TerminalFormatter())
                return colorful_json
            except:  # otherwise, return original json
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

    def test_md5(self):
        fnp = "/bin/bash"
        import hashlib # slow but correct
        good = hashlib.md5(open(fnp, 'rb').read()).hexdigest()
        assert Utils.md5(fnp) == good

    @staticmethod
    def is_gzipped(fnp):
        return open(fnp, 'rb').read(2) == b'\x1f\x8b'

    @staticmethod
    def sortFile(fnp):
        cmds = ["sort", "-o", fnp, "-k1,1 -k2,2n", fnp]
        return Utils.runCmds(cmds)

    @staticmethod
    def checkIfUrlExists(url):
        # http://stackoverflow.com/a/19582542
        try:
            ret = requests.head(url)
            return ret.status_code == 200
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

    def test_num_cores(self):
        import multiprocessing
        assert multiprocessing.cpu_count() == Utils.num_cores()

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
        modified = {o: (d1[o], d2[o]) for o in intersect_keys
                    if d1[o] != d2[o]}
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

    def test_remove_non_ascii(self):
        a = u"aaaàçççñññ"
        b = 'aaa'
        assert b == Utils.remove_non_ascii(a)
