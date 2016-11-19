#!/usr/bin/env python

from __future__ import print_function
import os
import json
from files_and_paths import Dirs, Urls
from utils import Utils


class ExpFile(object):
    def __init__(self, expID=None, fileID=None):
        self.expID = expID
        self.fileID = fileID

    # from http://stackoverflow.com/a/682545
    @classmethod
    def fromJsonFile(cls, expID, fileID, force):
        ret = cls(expID, fileID)
        ret._parse(fileID, force)
        return ret

    @classmethod
    def fromWebservice(cls, expID, r):
        ret = cls(expID, r["file"])
        ret.jsonUrl = Urls.base + "/files/{fileID}/?format=json".format(fileID=ret.fileID)
        ret.url = Urls.base + r["file_href"]
        ret.fnp_raw = os.path.join(Dirs.encode_data, os.path.basename(ret.url))
        ret.file_type = r["file_type"]
        ret.file_format = r["file_format"]
        ret.output_type = r["file_output_type"]
        ret.file_size_bytes = r["file_size_bytes"]
        ret.md5sum = r["file_md5sum"]
        ret.file_status = r["file_status"]
        ret.bio_rep = r["file_bio_rep"]
        ret.tech_rep = r["file_tech_rep"]
        ret.assembly = r["file_assembly"]
        ret.submitted_file_name = r["submitted_file_name"]
        ret.isPooled = r["file_ispooled"]
        ret.isPairedEnd = True if "run_type" in r and r["run_type"] == "paired-ended" else False

        ret.biological_replicates = None
        if "biological_replicates" in r:
            ret.biological_replicates = r["biological_replicates"]

        return ret

    @classmethod
    def fromRoadmap(cls, eid, assay_term_name):
        ret = cls(eid, eid)
        ret.assembly = "hg19"
        ret.assay_term_name = assay_term_name
        ret.isPooled = True
        return ret

    def __repr__(self):
        return "\t".join([str(x) for x in [self.fileID, self.file_format,
                                           self.output_type,
                                           "bio" + str(self.bio_rep),
                                           "tech" + str(self.tech_rep),
                                           "biological_replicates" +
                                           str(self.biological_replicates),
                                           self.jsonUrl, self.isPooled]])

    def isPeaks(self):
        return "peaks" == self.output_type

    def isReplicatedPeaks(self):
        return "replicated peaks" == self.output_type

    def isBedNarrowPeak(self):
        return "bed narrowPeak" == self.file_type

    def isBedBroadPeak(self):
        return "bed broadPeak" == self.file_type

    def isIDRoptimal(self):
        return "optimal idr thresholded peaks" == self.output_type

    def isBed(self):
        return "bed" == self.file_format

    def isBigBed(self):
        return "bigBed" == self.file_format

    def isBam(self):
        return "bam" == self.file_type

    def isGtf(self):
        return "gtf" == self.file_format

    def isHdf5(self):
        return "hdf5" == self.file_format

    def isBigWig(self):
        return "bigWig" == self.file_type

    def isSignal(self):
        return "signal" == self.output_type

    def isRawSignal(self):
        return "raw signal" == self.output_type

    def isHotSpot(self):
        return "hotspots" == self.output_type

    def isFoldChange(self):
        return "fold change over control" == self.output_type

    def isIDR(self):
        return "optimal idr thresholded peaks" == self.output_type

    def isFastqOrFasta(self):
        return "fasta" == self.file_type or "fastq" == self.file_type

    def isTAD(self):
        return "topologically associated domains" == self.output_type

    def isTSV(self):
        return "tsv" == self.file_type

    def getControls(self):
        x = set()
        if "derived_from" in self.jsondata:
            for i in self.jsondata["derived_from"]:
                if "controlled_by" in i:
                    x.add(i["controlled_by"][0])
        return list(x)

    def fnp(self, s4s=False):
        if self.expID.startswith("EN"):
            d = os.path.join(Dirs.encode_data, self.expID)
            fn = os.path.basename(self.url)
            fnp = os.path.join(d, fn)
            if s4s:
                fnp = fnp.replace("/project/umw_", "/s4s/s4s_")
            return fnp

        if "H3K27ac" == self.assay_term_name:
            fn = self.expID + "-H3K27ac.fc.signal.bigwig"
        elif "DNase-seq" == self.assay_term_name:
            fn = self.expID + "-DNase.fc.signal.bigwig"
        else:
            raise Exception("unknown ROADMAP file type")
        return os.path.join(Dirs.roadmap_base, self.expID, fn)

    def normFnp(self):
        fnp = self.fnp()
        fnp = fnp.replace("encode/data/", "encode/norm/")
        fnp = fnp.replace("roadmap/data/consolidated",
                          "roadmap/data/norm/consolidated")
        pre, ext = os.path.splitext(fnp)
        if ".bigwig" == ext:
            ext = ".bigWig"
        return pre + ".norm" + ext

    def download(self, force=None):
        fnp = self.fnp()
        Utils.ensureDir(fnp)
        return Utils.download(self.url, fnp,
                              True, force, self.file_size_bytes)

    def downloadPublic(self, force=None):
        fnp = self.fnp()
        Utils.ensureDir(fnp)
        return Utils.download(self.url, fnp,
                              False, force, self.file_size_bytes)

    def featurename(self):
        return self.fileID

    def jsonFileFnp(self, fileID):
        return os.path.join(Dirs.encode_json, "exps", self.expID, fileID + ".json")

    def _parse(self, fileID, force):
        # NOTE! changes to fields during parsing could affect data import into database...

        self.jsonUrl = Urls.base + "/files/{fileID}/?format=json".format(fileID=fileID)

        fnp = self.jsonFileFnp(fileID)
        Utils.ensureDir(fnp)
        Utils.download(self.jsonUrl, fnp, True, force, skipSizeCheck=True)
        with open(fnp) as f:
            g = json.load(f)

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

        self.file_size_bytes = -1
        if "file_size" in g:
            # missing file size for https://www.encodeproject.org/files/ENCFF408AMB/?format=json
            self.file_size_bytes = g["file_size"]

        self.bio_rep = ""
        self.tech_rep = ""
        if "replicate" in g:
            self.bio_rep = g["replicate"]["biological_replicate_number"]
            self.tech_rep = g["replicate"]["technical_replicate_number"]

        self.biological_replicates = None
        if "biological_replicates" in g:
            self.biological_replicates = g["biological_replicates"]

        self.assembly = ""
        if "assembly" in g:
            self.assembly = g["assembly"]

        self.submitted_file_name = ""
        if "submitted_file_name" in g:
            self.submitted_file_name = g["submitted_file_name"]

        self.isPooled = False
        if "biological_replicates" in g:
            self.isPooled = len(g["biological_replicates"]) > 1
