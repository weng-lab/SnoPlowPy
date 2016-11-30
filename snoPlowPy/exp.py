#!/usr/bin/env python

from __future__ import print_function
import os
import json
import collections
from .files_and_paths import Dirs, Urls, Genome, Tools
from .utils import Utils, cat
from .exp_metadata import ExpMetadata


class Exp(ExpMetadata):
    def __init__(self, encodeID):
        ExpMetadata.__init__(self)
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
            Utils.download(ret.jsonUrl, ret.jsonFnp, True, force,
                           skipSizeCheck=True)
        with open(ret.jsonFnp) as f:
            ret.jsondata = json.load(f)
        ret._parseJson(force)
        return ret

    @classmethod
    def fromJson(cls, jsondata, force=False):
        ret = cls(jsondata["accession"])
        ret.jsondata = jsondata
        ret._parseJson(force)
        return ret

    @classmethod
    def fromWebservice(cls, rows):
        r = rows[0][0]  # one row per file, so just grab info from first file
        ret = cls(r["accession"])
        ret._parseWS(rows)
        return ret

    def __repr__(self):
        return '\t'.join([self.description, self.assay_term_name,
                          self.target, self.url])

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

    def dbCrossRef(self):  # database crossrefs
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
            # print("no bigwigs (raw) signal found: ", self.url)
            return None

        if 0 == len(bigwigs):
            print("no bigwigs found:", self.encodeID)
            return None

        if 1 == len(bigwigs):
            bw = bigwigs[0]
            if args and args.process:
                bw.download()
            return bw.fnp()

        fbigwigs = filter(lambda x: x.bio_rep and x.bio_rep in ['1', '2', '3', '4', '5'],
                          bigwigs)
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
            if not os.path.exists(bams[0].fnp()):
                bams[0].download()
            return bams[0].fnp(), bams[0].assembly
        print("too many bams (%d) found for" % len(bams), self.encodeID)
        print("bam IDs are")
        for bam in bams:
            if args and args.process:
                bams[0].download()
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
            lambda x: x.isBedNarrowPeak() and 1 in x.biological_replicates and
            2 in x.biological_replicates,
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
        # if args and args.v: print(beds)

        for bed in beds:
            if args and args.process:
                bed.download()
        if 1 == len(beds):
            bed = beds[0]
            return bed.fnp()

        fbeds = filter(lambda x: x.bio_rep and x.bio_rep in ['1', '2', '3', '4', '5'], beds)
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
            # if args and args.v: print("no beds after replicate filtering")
            pass
        return None
