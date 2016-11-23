#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import os
import tempfile
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

    def test_sanitize(self):
        bad = "aBc1234%^&*"
        good = "aBc1234".lower()
        assert good == Utils.sanitize(bad)

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

    def test_num_cores(self):
        import multiprocessing
        assert multiprocessing.cpu_count() == Utils.num_cores()

    def test_remove_non_ascii(self):
        assert "" == Utils.remove_non_ascii("")
        a = u"aaaàçççñññ"
        b = 'aaa'
        assert b == Utils.remove_non_ascii(a)
