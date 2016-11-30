#!/usr/bin/env python

from __future__ import print_function
import os
from .files_and_paths import Dirs, Urls


class ExpFileMetadata(object):
    def __init__(self):
        pass

    def _parseJson(self, expID, fileID, g):
        # NOTE! changes to fields during parsing could affect data import into database...

        self.jsonUrl = Urls.base + "/files/{fileID}/?format=json".format(fileID=fileID)

        self.jsondata = g
        self.encodeid = g["@id"]
        self.accession = g["accession"]

        self.url = Urls.base + g["href"]
        self.fnp_raw = os.path.join(Dirs.encode_data, os.path.basename(self.url))
        self.href = g["href"]
        self.file_type = g["file_type"]
        self.file_format = g["file_format"]
        self.output_type = g["output_type"]
        self.data_create = g["date_created"]
        self.date_created = g["date_created"]  # add w/o typo
        self.md5sum = g["md5sum"]
        self.file_status = g["status"]
        self.file_size_bytes = g["file_size"]

        self.assembly = g.get("assembly", None)
        self.submitted_file_name = g.get("submitted_file_name", None)
        self.biological_replicates = g.get("biological_replicates", None)
        self.technical_replicates = g.get("technical_replicates", None)

        self.isPooled = False
        if "biological_replicates" in g:
            self.isPooled = len(g["biological_replicates"]) > 1

    def _parseWS(self, r):
        self.jsonUrl = Urls.base + "/files/{fileID}/?format=json".format(fileID=self.fileID)
        self.url = Urls.base + r["file_href"]
        self.fnp_raw = os.path.join(Dirs.encode_data, os.path.basename(self.url))
        self.file_type = r["file_type"]
        self.file_format = r["file_format"]
        self.output_type = r["file_output_type"]
        self.file_size_bytes = r["file_size_bytes"]
        self.md5sum = r["file_md5sum"]
        self.file_status = r["file_status"]
        self.bio_rep = r["file_bio_rep"]
        self.tech_rep = r["file_tech_rep"]
        self.assembly = r["file_assembly"]
        self.submitted_file_name = r["submitted_file_name"]
        self.isPooled = r["file_ispooled"]
        self.isPairedEnd = True if "run_type" in r and r["run_type"] == "paired-ended" else False

        self.biological_replicates = None
        if "biological_replicates" in r:
            self.biological_replicates = r["biological_replicates"]
