#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import os
import tempfile
import gzip
import tarfile
import shutil
from builtins import str, range

from snoPlowPy.utils import Utils, numLines


class TestUtils(object):
    def test_numLines(self):
        for n in [0, 173]:
            with tempfile.NamedTemporaryFile("w", delete=False) as f:
                fnpTmp = f.name
                for i in range(n):
                    f.write("hi " + str(i) + "\n")
            count = numLines(fnpTmp)
            os.remove(fnpTmp)
            assert(count == n), "n is " + str(n)

    def test_deleteFileIfSizeNotMatch(self):
        for ints in [[], [1, 2, 3]]:
            with tempfile.NamedTemporaryFile("wb", delete=False) as f:
                fnpTmp = f.name
                f.write(bytearray(ints))
            numBytes = len(ints)
            Utils.deleteFileIfSizeNotMatch(fnpTmp, numBytes)
            assert os.path.exists(fnpTmp)
            numBytes += 1
            Utils.deleteFileIfSizeNotMatch(fnpTmp, numBytes)
            assert not os.path.exists(fnpTmp)

    def test_get_file_if_size_diff(self, tmpdir, remote_f):
        # file not exists
        fn = Utils.get_file_if_size_diff(remote_f, str(tmpdir))
        assert open(fn).read() == 'testting data!!!\n'
        # file exists
        fn = Utils.get_file_if_size_diff(remote_f, str(tmpdir))
        assert open(fn).read() == 'testting data!!!\n'
        # file differs
        local_f = tmpdir.join('a')
        local_f.write('different content~')
        fn = Utils.get_file_if_size_diff(remote_f, str(tmpdir))
        assert open(fn).read() == 'testting data!!!\n'

    def test_getHttpFileSizeBytes(self, remote_f):
        # TODO: auth???
        # starts with ftp
        url = 'ftp://balabala'
        assert Utils.getHttpFileSizeBytes(url, auth=None) is None
        # wrong url
        url = 'https://github.com/kepbod/tmp'
        assert Utils.getHttpFileSizeBytes(url, auth=None) is None
        # not file url
        url = 'https://github.com'
        assert Utils.getHttpFileSizeBytes(url, auth=None) == -1
        # file url
        assert Utils.getHttpFileSizeBytes(remote_f, auth=None) == 37

    def test_download(self, tmpdir, remote_f):
        # TODO: other parameters???
        # TODO: ftp url
        fn = os.path.join(str(tmpdir), 'a')
        # no file
        Utils.download(remote_f, fn)
        assert open(fn).read() == 'testting data!!!\n'
        # have file
        Utils.download(remote_f, fn)
        assert open(fn).read() == 'testting data!!!\n'
        # different content
        local_f = tmpdir.join('a')
        local_f.write('different content~')
        Utils.download(remote_f, fn)
        assert open(fn).read() == 'testting data!!!\n'

    def test_query(self, remote_f):
        # wrong url
        url = 'https://github.com/kepbod/tmp'
        assert Utils.query(url) is None
        # file url
        assert Utils.query(remote_f) == b'testting data!!!\n'

    def test_sanitize(self):
        bad = "aBc1234%^&*"
        good = "aBc1234".lower()
        assert good == Utils.sanitize(bad)

    def test_ensureDir(self, tmpdir):
        Utils.ensureDir(os.path.join(str(tmpdir), 'tmp/tmp/a'))
        dir_path = os.path.join(str(tmpdir), 'tmp/tmp')
        assert os.path.isdir(dir_path)

    def test_gzfilelen(self, tmpdir):
        zipf = os.path.join(str(tmpdir), 'a.gz')
        with gzip.open(zipf, 'wb') as f:
            f.write(b'abc\nabc\n')
        assert Utils.gzfilelen(zipf) == 2

    def test_titleCase(self):
        old = "convert to titlecase"
        new = "Convert to Titlecase"
        assert new == Utils.titleCase(old)

        old = "a silly story"
        new = "A Silly Story"
        assert new == Utils.titleCase(old)

    def test_md5(self):
        fnp = "/bin/bash"
        import hashlib  # slow but correct
        good = hashlib.md5(open(fnp, 'rb').read()).hexdigest()
        assert Utils.md5(fnp) == good

    def test_is_gzipped(self, tmpdir):
        zipf = os.path.join(str(tmpdir), 'a.gz')
        with gzip.open(zipf, 'wb') as f:
            f.write(b'abc\nabc\n')
        assert Utils.is_gzipped(zipf) is True
        nonzipf = tmpdir.join('b.gz')
        nonzipf.write('abc')
        nonzipf = os.path.join(str(tmpdir), 'b.gz')
        assert Utils.is_gzipped(nonzipf) is False

    def test_sortFile(self, tmpdir):
        fn = tmpdir.join('a')
        fn.write('b\t1\na\t2\na\t1\n')
        fn = os.path.join(str(tmpdir), 'a')
        Utils.sortFile(fn)
        assert open(fn).read() == 'a\t1\na\t2\nb\t1\n'

    def test_checkIfUrlExists(self, remote_f):
        # wrong url
        url = 'https://github.com/kepbod/tmp'
        assert Utils.checkIfUrlExists(url) is False
        # correct url
        assert Utils.checkIfUrlExists(remote_f) is True

    def test_touch(self, tmpdir):
        fn = os.path.join(str(tmpdir), 'a')
        Utils.touch(fn)
        assert os.path.isfile(fn)
        assert open(fn).read() == ' '

    def test_ftouch(self, tmpdir):
        fn = os.path.join(str(tmpdir), 'tmp/a')
        Utils.touch(fn)
        assert os.path.isfile(fn)
        assert open(fn).read() == ' '

    def test_fileOlderThanDays(self, tmpdir):
        # file not exists
        fn = os.path.join(str(tmpdir), 'a')
        assert Utils.fileOlderThanDays(fn, 1) is True  # TODO: True???
        # file exists
        fn = tmpdir.join('a')
        fn.write('abc')
        fn = os.path.join(str(tmpdir), 'a')
        assert Utils.fileOlderThanDays(fn, 1) is False

    def test_getStringFromListOrString(self):
        assert Utils.getStringFromListOrString('a') == 'a'
        assert Utils.getStringFromListOrString(['a']) == 'a'
        assert Utils.getStringFromListOrString(['a', 'a']) == 'a'

    def test_which(self):
        assert Utils.which('las') is None
        assert Utils.which('ls') == '/bin/ls'

    def test_rm_rf(self, tmpdir):
        fn = tmpdir.mkdir('tmp').join('a')
        fn.write('abc')
        fn = os.path.join(str(tmpdir), 'tmp')
        Utils.rm_rf(fn)
        assert not os.path.isfile(fn)

    def test_num_cores(self):
        import multiprocessing
        assert multiprocessing.cpu_count() == Utils.num_cores()

    def test_un_xz_tar(self, tmpdir):
        tmp = tmpdir.mkdir('tmp')
        tmp.join('a').write('abc')
        tmp.join('b').write('abc')
        source_dir = os.path.join(str(tmpdir), 'tmp')
        tarf = os.path.join(str(tmpdir), 'zip.tar')
        with tarfile.open(tarf, 'w:gz') as tar:
            tar.add(source_dir, arcname=os.path.basename(source_dir))
        shutil.rmtree(source_dir)
        dest = str(tmpdir)
        Utils.un_xz_tar(tarf, dest)
        assert os.path.isdir(source_dir)
        flst = os.listdir(source_dir)
        assert 'a' in flst
        assert 'b' in flst

    def test_clear_dir(self, tmpdir):
        fn = tmpdir.mkdir('tmp').join('a')
        fn.write('abc')
        dir = os.path.join(str(tmpdir), 'tmp')
        Utils.clear_dir(dir)
        assert os.path.isdir(dir)
        assert not os.path.isfile(os.path.join(dir, 'a'))

    def test_merge_two_dicts(self):
        x = {'a': 1, 'b': 2}
        y = {'b': 3, 'c': 4}
        z = Utils.merge_two_dicts(x, y)
        assert 'a' in z
        assert 'b' in z
        assert 'c' in z

    def test_dictCompare(self):
        x = {'a': 1, 'b': 2, 'd': 1}
        y = {'b': 3, 'c': 4, 'd': 1}
        added, removed, modified, same = Utils.dictCompare(x, y)
        assert added == {'a'}
        assert removed == {'c'}
        assert modified == {'b': (2, 3)}
        assert same == {'d'}

    def test_remove_non_ascii(self):
        assert "" == Utils.remove_non_ascii("")
        a = u"aaaàçççñññ"
        b = 'aaa'
        assert b == Utils.remove_non_ascii(a)
