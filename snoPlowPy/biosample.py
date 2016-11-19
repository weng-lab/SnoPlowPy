#!/usr/bin/env python

from __future__ import print_function
import os
import sys
import json
import shutil
import collections
from files_and_paths import Dirs, Urls, Datasets, Genome, Tools
from datetime import datetime
from utils import Utils, cat


class Biosample:
    def __init__(self, accessionID, force=False):
        self.accessionID = accessionID

        self.jsonFnp = os.path.join(Dirs.encode_json, "biosamples",
                                    accessionID + ".json")
        self.jsonUrl = os.path.join(Urls.base, "biosamples", accessionID,
                                    "?format=json")

        Utils.download(self.jsonUrl, self.jsonFnp, True, force,
                       skipSizeCheck=True)
        with open(self.jsonFnp) as f:
            self.jsondata = json.load(f)
        self._parse()

    def __repr__(self):
        return "\t".join([self.accessionID, self.biosample_term_id,
                          self.biosample_term_name, self.biosample_type])

    def _parse(self):
        self.biosample_term_name = self.jsondata["biosample_term_name"]
        self.biosample_term_id = self.jsondata["biosample_term_id"]
        self.biosample_type = self.jsondata["biosample_type"]
