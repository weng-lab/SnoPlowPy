#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

from snoPlowPy.exp import Exp


class TestExp(object):
    def test_fromJson_withfake(self, exp_jsondata_generator):
        exp = Exp.fromJson(exp_jsondata_generator)
        assert exp.accessionID == exp.accessionID
        assert exp.accessionID == exp_jsondata_generator['accession']
