#!/usr/bin/env python

from __future__ import print_function
from datetime import datetime

from .utils import Utils
from .exp_file import ExpFile


class ExpMetadata:
    def __init__(self):
        pass

    def _parseJson(self, force):
        # NOTE! changes to fields during parsing could affect data import into
        # database...

        g = self.jsondata
        self.encodeid = g["@id"]

        if "Annotation" in g["@type"]:
            self.assay_term_name = g["annotation_type"]
        else:
            self.assay_term_name = g["assay_term_name"]

        self.description = g["description"]
        self.isPairedEnd = True if "run_type" in g and g["run_type"] == "paired-ended" else False

        self.biosample_term_name = Utils.getStringFromListOrString(g["biosample_term_name"])
        self.biosample_term_id = Utils.getStringFromListOrString(g["biosample_term_id"])
        self.biosample_type = Utils.getStringFromListOrString(g["biosample_type"])

        self.accession = g["accession"]
        self.status = g["status"]
        self.lab = g["lab"]["name"]

        self.date_released = "UNKNOWN"
        if "date_released" in g:
            self.date_released = g["date_released"]
            self.date_released_obj = datetime.strptime(g["date_released"], "%Y-%m-%d")

        self.target = ""
        self.tf = ""
        if "target" in g:
            try:
                self.target = g["target"]["investigated_as"][0]
                self.tf = g["target"]["label"]
            except:
                try:  # ROADMAP-style Encode exp?
                    self.target = g["target"][0]["investigated_as"][0]
                    self.tf = g["target"][0]["label"]
                except:
                    pass
        self.label = self.tf

        self.age = ""
        try:
            self.age = g["replicates"][0]["library"]["biosample"]["age"]
        except:
            pass

        if "target" in self.jsondata and "dbxref" in self.jsondata["target"]:
            self.dbxref = self.jsondata["target"]["dbxref"]
        else:
            self.dbxref = []

        self.files = []
        for j in g["files"]:
            ef = ExpFile.fromJson(self.accession, j["accession"], j)
            self.files.append(ef)

    def unreleasedFiles(self, force):
        g = self.jsondata
        revokedFiles = set([f["accession"] for f in g["revoked_files"]])
        originalFiles = set([x.split('/')[2] for x in g["original_files"]]).difference(revokedFiles)
        self.unreleased_file_ids = originalFiles.difference(set([f["accession"]
                                                                 for f in g["files"]]))

        files = []
        for fileID in self.unreleased_file_ids:
            ef = ExpFile.fromJsonFile(self.accession, fileID, force)
            files.append(ef)
        return files

    def _parseWS(self, rows):
        r = rows[0][0]  # one row per file, so just grab info from first file
        self.age = r["age"]
        self.assay_term_name = r["assay_term_name"]
        self.biosample_term_id = r["biosample_term_id"]
        self.biosample_term_name = r["biosample_term_name"]
        self.biosample_type = r["biosample_type"]
        self.description = r["description"]
        self.lab = r["lab"]
        self.label = r["label"]
        self.status = r["status"]
        self.target = r["target"]
        self.tf = r["label"]

        self.organ_slims = ""
        if "organ_slims" in r:
            self.organ_slims = r["organ_slims"]

        self.files = [ExpFile.fromWebservice(self.encodeID, e[0]) for e in rows]
