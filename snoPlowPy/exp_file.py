#!/usr/bin/env python

from __future__ import print_function
import os
import json
from .files_and_paths import Dirs, Urls
from .utils import Utils
from .exp_file_metadata import ExpFileMetadata


class ExpFile(ExpFileMetadata):
    def __init__(self, expID=None, fileID=None):
        ExpFileMetadata.__init__(self)
        self.expID = expID
        self.fileID = fileID

    # from http://stackoverflow.com/a/682545
    @classmethod
    def fromJson(cls, expID, fileID, j):
        ret = cls(expID, fileID)
        ret._parseJson(expID, fileID, j)
        return ret

    @classmethod
    # in case file JSON is not part of the experiment json, for some unknown reason (revoked?)
    def fromJsonFile(cls, expID, fileID, force):
        ret = cls(expID, fileID)

        jsonFnp = os.path.join(Dirs.encode_json, "exps", expID, fileID + ".json")
        jsonUrl = Urls.base + "/files/{fileID}/?format=json".format(fileID=fileID)
        Utils.ensureDir(jsonFnp)
        Utils.download(jsonUrl, jsonFnp, True, force, skipSizeCheck=True)
        with open(jsonFnp) as f:
            j = json.load(f)
        ret._parseJson(expID, fileID, j)
        return ret

    @classmethod
    def fromWebservice(cls, expID, r):
        ret = cls(expID, r["file"])
        ret.parseWS(r)
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
