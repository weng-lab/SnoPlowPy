#!/usr/bin/env python

from __future__ import print_function
import os, sys, json, shutil, collections
from files_and_paths import Dirs, Urls, Datasets, Genome, Tools
from datetime import datetime
from utils import Utils, cat

class QueryDCC:
    def __init__(self, host=None, auth=True, cache=None):
        self.auth = auth
        self.host = "https://www.encodeproject.org"
        if host:
            self.host = host
        self.cache = cache

    def getURL(self, url, quiet = False):
        if self.cache:
            ret = self.cache.getOrSet(url, lambda: Utils.query(url,
                                                               auth = self.auth,
                                                               quiet = quiet),
                                      quiet)
        else:
            ret = Utils.query(url, auth=self.auth, quiet=quiet)
        if not ret:
            raise Exception("could not download " + url)
        return ret

    def getFromAlias(self, alias, quiet = False):
        url = "%s/%s/?format=json" % (self.host, alias)
        ret = self.getURL(url, quiet)
        try:
            return json.loads(ret)
        except:
            print("could not load alias", alias)
            print(ret)
            raise

    def getIDs(self, url):
        ret = self.getURL(url)
        ret = json.loads(ret)
        eids = []
        for e in ret["@graph"]:
            eid = e["@id"]
            if not eid:
                continue
            eids.append(eid)
        print("found", len(eids), "ENCODE ids")
        return eids

    def getExps(self, url):
        ret = self.getURL(url)
        ret = json.loads(ret)
        exps = []
        for e in ret["@graph"]:
            accession = e["accession"]
            if not accession:
                continue
            exps.append(Exp.fromJsonFile(accession))
        print("found", len(exps), "experiments")
        return exps

class Exp(object):
    def __init__(self, encodeID):
        self.encodeID = encodeID
        self.accessionID = encodeID
        self.jsondata = None
        self.url = os.path.join(Urls.base, "experiments", encodeID)
        self.jsonUrl = self.url + "/?format=json"
        self.jsonFnp = Exp.makeJsonFnp(encodeID)

    @staticmethod
    def makeJsonFnp(encodeID):
        return os.path.join(Dirs.encode_json, "exps",
                            encodeID + ".json")

    @classmethod
    def fromJsonFile(cls, encodeID, force=False):
        ret = cls(encodeID)
        if force or not os.path.exists(ret.jsonFnp):
            Utils.download(ret.jsonUrl, ret.jsonFnp, True, force, skipSizeCheck=True)
        with open(ret.jsonFnp) as f:
            ret.jsondata = json.load(f)
        ret._parse(force)
        return ret

    @classmethod
    def fromJson(cls, jsondata, force=False):
        ret = cls(jsondata["accession"])
        ret.jsondata = jsondata
        ret._parse(force)
        return ret

    @classmethod
    def fromWebservice(cls, rows):
        r = rows[0][0] # one row per file, so just grab info from first file
        ret = cls(r["accession"])
        ret.age = r["age"]
        ret.assay_term_name = r["assay_term_name"]
        ret.assembly = r["assembly"]
        ret.biosample_term_id = r["biosample_term_id"]
        ret.biosample_term_name = r["biosample_term_name"]
        ret.biosample_type = r["biosample_type"]
        ret.description = r["description"]
        ret.lab = r["lab"]
        ret.label = r["label"]
        ret.status = r["status"]
        ret.target = r["target"]
        ret.tf = r["label"]

        ret.organ_slims = ""
        if "organ_slims" in r:
            ret.organ_slims = r["organ_slims"]

        ret.files = [ExpFile.fromWebservice(ret.encodeID, e[0]) for e in rows]
        return ret

    def __repr__(self):
        return '\t'.join([self.description, self.assay_term_name,
                          self.target, self.assembly, self.url])

    def __getitem__(self, key):
        if "biosample_term_name" == key:
            return self.biosample_term_name
        if "age" == key:
            return self.age
        raise Exception("undefined key " + key)

    def isImmortalizedCellLine(self):
        return self.biosample_type == "immortalized cell line"

    def isH3K27ac(self):
        return "H3K27ac" == self.label

    def isH3K4me3(self):
        return "H3K4me3" == self.label

    def isChipSeq(self):
        return "ChIP-seq" == self.assay_term_name

    def isChipSeqTF(self):
        return self.isChipSeq() and "transcription" in self.target

    def isChipSeqHistoneMark(self):
        return self.isChipSeq() and "histone" in self.target

    def isDNaseSeq(self):
        return "DNase-seq" == self.assay_term_name

    def isMnaseSeq(self):
        return "MNase-seq" == self.assay_term_name

    def isStam(self):
        return "john-stamatoyannopoulos" == self.lab

    def dbCrossRef(self): # database crossrefs
        return self.dbxref

    def getExpJson(self):
        if not self.jsondata:
            fnp = os.path.join(Dirs.encode_json, "exps",
                               self.encodeID + ".json")
            Utils.ensureDir(fnp)
            Utils.download(self.jsonUrl, fnp, True, skipSizeCheck=True)
            with open(fnp) as f:
                self.jsondata = json.load(f)
        return self.jsondata

    def _parse(self, force):
        # NOTE! changes to fields during parsing could affect data import into database...

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

        self.assembly = ""
        if g["assembly"]:
            self.assembly = ','.join(g["assembly"])

        self.target = ""
        self.tf = ""
        if "target" in g:
            try:
                self.target = g["target"]["investigated_as"][0]
                self.tf = g["target"]["label"]
            except:
                try: # ROADMAP-style Encode exp?
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

        revokedFiles = set([f["accession"] for f in g["revoked_files"]])
        originalFiles = set([x.split('/')[2] for x in g["original_files"]]).difference(revokedFiles)
        fileIDs = set([f["accession"] for f in g["files"]]).difference(revokedFiles).union(originalFiles)
        self.unreleased_files = originalFiles.difference(set([f["accession"] for f in g["files"]]))

        self.files = []
        for f in fileIDs:
            ef = ExpFile.fromJsonFile(self.accession, f, force)
            self.files.append(ef)

    def getMeanBigWigFnp(self, assembly, fnps):
        stems = [os.path.basename(fnp).split('.')[0] for fnp in fnps]
        meanFn = "_".join(["mean"] + sorted(stems)) + ".bigWig"
        return os.path.join(Dirs.mean_data, self.encodeID, assembly, meanFn)

    def computeMeanBigWig(self, assembly, fnps):
        meanFnp = self.getMeanBigWigFnp(assembly, fnps)
        tmpMeanFnp = meanFnp + ".tmp"
        Utils.ensureDir(meanFnp)

        cmds = [Dirs.ToolsFnp("wiggletools.static.git.7579e66"),
                "mean", " ".join(sorted(fnps)),
                "|", Dirs.ToolsFnp("ucsc.v287/wigToBigWig"),
                "-clip", "stdin",
                Genome.ChrLenByAssembly(assembly),
                tmpMeanFnp]

        print("\t computing mean bigwig...")
        Utils.runCmds(cmds)
        os.rename(tmpMeanFnp, meanFnp)
        print("\twrote", meanFnp)

    def bigWigFilters(self, assembly):
        files = filter(lambda x: x.isBigWig(), self.files)
        bfs = [lambda x: x.output_type == "fold change over control" and x.isPooled,
               lambda x: x.output_type == "fold change over control" and '1' in x.bio_rep,
               lambda x: x.output_type == "fold change over control" and '2' in x.bio_rep,
               lambda x: x.output_type == "fold change over control",
               lambda x: x.output_type == "signal of unique reads" and x.isPooled,
               lambda x: x.output_type == "signal of unique reads" and '1' in x.bio_rep,
               lambda x: x.output_type == "signal of unique reads" and '2' in x.bio_rep,
               lambda x: x.output_type == "signal of unique reads",
               lambda x: x.isRawSignal() and x.isPooled,
               lambda x: x.isRawSignal() and x.bio_rep == '1',
               lambda x: x.isRawSignal() and x.bio_rep == '2',
               lambda x: x.isRawSignal(),
               lambda x: x.isSignal() and x.isPooled,
               lambda x: x.isSignal() and x.bio_rep == '1',
               lambda x: x.isSignal() and x.bio_rep == '2',
               lambda x: x.isSignal()
               ]
        for bf in bfs:
            bws = filter(bf, files)
            bws = filter(lambda x: x.assembly == assembly, bws)
            if bws:
                return bws
        return []

    def bamFilters(self):
        return filter(lambda x: x.isBam(), self.files)

    def hotSpotFilters(self):
        return filter(lambda x: x.isBed() and x.isHotSpot(), self.files)

    def getSingleBigWigSingleFnp(self, assembly, args):
        bigwigs = self.bigWigFilters(assembly)

        if not bigwigs:
            #print("no bigwigs (raw) signal found: ", self.url)
            return None

        if 0 == len(bigwigs):
            print("no bigwigs found:", self.encodeID)
            return None

        if 1 == len(bigwigs):
            bw = bigwigs[0]
            if args and args.process:
                bw.download()
            return bw.fnp()

        fbigwigs = filter(lambda x: x.bio_rep and x.bio_rep in ['1','2','3','4','5'], bigwigs)
        if fbigwigs:
            bigwigs = fbigwigs
        if not bigwigs:
            print(bigwigs)
            print("unsure of bigwig filter")
            return None

        if len(bigwigs) < 5:
            fnps = [f.fnp() for f in bigwigs]
            meanFnp = self.getMeanBigWigFnp(assembly, fnps)
            if args and args.process:
                [f.download() for f in bigwigs]
                if not os.path.exists(meanFnp):
                    self.computeMeanBigWig(assembly, fnps)
            return meanFnp
        else:
            print("ERROR: too many bigWigs found for:")
            print("\t", self.url)
            for f in bigwigs:
                print("\t", f)
            return None

    def getSingleBamSingleFnp(self, args):
        bams = self.bamFilters()
        if not bams:
            return None, None
        if 0 == len(bams):
            print("no bams found:", self.encodeID)
            return None, None
        if 1 == len(bams):
            if not os.path.exists(bams[0].fnp()): bams[0].download()
            return bams[0].fnp(), bams[0].assembly
        print("too many bams (%d) found for" % len(bams), self.encodeID)
        print("bam IDs are")
        for bam in bams:
            if args and args.process: bams[0].download()
            return bam.fnp(), bam.assembly
        print("too many bams found for", self.encodeID)
        return None, None

    def hasSingleBigWigSignalFile(self, assembly, args):
        return self.getSingleBigWigSingleFnp(assembly, args)

    def hasSingleBamAlignmentsFile(self, args):
        return self.getSingleBamSingleFnp(args)

    def hasSinglePeaksFile(self, assembly, args):
        return self.getIDRnarrowPeak(assembly, args)

    def getMergePeaksFnp(self, assembly, fnps):
        stems = [os.path.basename(fnp).split('.')[0] for fnp in fnps]
        meanFn = "_".join(["intersectFirst"] + sorted(stems)) + ".bed.gz"
        return os.path.join(Dirs.mean_data, self.encodeID, assembly, meanFn)

    def computeMergePeaks(self, assembly, fnps):
        mergeFnp = self.getMergePeaksFnp(assembly, fnps)
        Utils.ensureDir(mergeFnp)
        tmpMergeFnp = mergeFnp + ".tmp"

        cmds = [cat(fnps[0]),
                "|", "sort -k1,1 -k2,2n",
                "|", Tools.bedtools, "intersect",
                "-a", "stdin", "-b", " ".join(sorted(fnps[1:])),
                "-u",
                "|", "gzip",
                ">", tmpMergeFnp]

        Utils.runCmds(cmds)
        os.rename(tmpMergeFnp, mergeFnp)
        print("\twrote", mergeFnp)

    def bedFilters(self, assembly):
        bfs = [
            lambda x: x.isBedNarrowPeak() and x.isIDRoptimal(),
            lambda x: x.isBedNarrowPeak() and x.isIDR(),
            lambda x: x.isBedNarrowPeak() and x.isReplicatedPeaks(),
            lambda x: x.isBedNarrowPeak() and isinstance(x.bio_rep, collections.Iterable) and
                                                    '1' in x.bio_rep and '2' in x.bio_rep,
            lambda x: x.isBedNarrowPeak() and 1 in x.biological_replicates and 2 in x.biological_replicates,
            lambda x: x.isBedNarrowPeak(),
            lambda x: x.isBedBroadPeak(),
            lambda x: x.isPeaks()
        ]
        for bf in bfs:
            beds = filter(bf, self.files)
            beds = filter(lambda x: x.assembly == assembly, beds)
            if beds:
                return beds
        return []

    def getPeakFiles(self):
        return self.bedFilters()

    def getTADs(self):
        return filter(lambda x: x.isTAD(), self.files)

    def getIDRnarrowPeak(self, assembly, args):
        beds = self.bedFilters(assembly)
        #if args and args.v: print(beds)

        for bed in beds:
            if args and args.process:
                bed.download()
        if 1 == len(beds):
            bed = beds[0]
            return bed.fnp()

        fbeds = filter(lambda x: x.bio_rep and x.bio_rep in ['1','2','3','4','5'], beds)
        if fbeds:
            beds = fbeds
        if len(beds) > 1:
            fnps = sorted(list(set([f.fnp() for f in beds])))

            mergeFnp = self.getMergePeaksFnp(assembly, fnps)
            if args and args.process:
                if not os.path.exists(mergeFnp):
                    self.computeMergePeaks(assembly, fnps)
            return mergeFnp
        else:
            #if args and args.v: print("no beds after replicate filtering")
            pass
        return None

class ExpFile(object):
    def __init__(self, expID = None, fileID = None):
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
                                           "biological_replicates" + str(self.biological_replicates),
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
            if s4s: fnp = fnp.replace("/project/umw_", "/s4s/s4s_")
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

    def dataCol(self):
        fnp = self.fnp_mm10()
        col = qColBed1based(fnp)
        if checkQvalueCol(fnp, col):
            return col
        return signalColBed1based(fnp)

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
        self.date_created = g["date_created"] # add w/o typo
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

class Biosample:
    def __init__(self, accessionID, force=False):
        self.accessionID = accessionID

        self.jsonFnp = os.path.join(Dirs.encode_json, "biosamples",
                                    accessionID + ".json")
        self.jsonUrl = os.path.join(Urls.base, "biosamples", accessionID, "?format=json")

        Utils.download(self.jsonUrl, self.jsonFnp, True, force, skipSizeCheck=True)
        with open(self.jsonFnp) as f:
            self.jsondata = json.load(f)
        self._parse()

    def __repr__(self):
        return "\t".join([self.accessionID, self.biosample_term_id, self.biosample_term_name,
                          self.biosample_type])

    def _parse(self):
        self.biosample_term_name = self.jsondata["biosample_term_name"]
        self.biosample_term_id = self.jsondata["biosample_term_id"]
        self.biosample_type = self.jsondata["biosample_type"]

def main():
    qd = QueryDCC("https://www.encodeproject.org/search/?type=Experiment&award.rfa=ENCODE3&assay_slims=DNA+accessibility&assay_title=DNase-seq&files.file_type=bed+narrowPeak&format=json&limit=all")
    print(qd.getExps())

if __name__ == "__main__":
    sys.exit(main())
