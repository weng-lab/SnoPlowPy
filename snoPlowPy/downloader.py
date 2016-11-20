#!/usr/bin/env python

from __future__ import print_function

import os
import sys
import argparse
import json
from .utils import Utils
from joblib import Parallel, delayed
import traceback

from .files_and_paths import Datasets
from .exp import Exp


def loadBedBigWigHdf5Bam(idx, total, accessionID, force, refresh, jsononly):
    doForce = force
    try:
        jsonFnp = Exp.makeJsonFnp(accessionID)
        if not os.path.exists(jsonFnp):
            doForce = True
        if refresh:
            if Utils.fileOlderThanDays(jsonFnp, 14):
                doForce = True
        exp = Exp.fromJsonFile(accessionID, doForce)
        print(idx + 1, "of", total, exp.encodeID, exp.assay_term_name,
              exp.label, exp.description)
        if not jsononly:
            for f in exp.files:
                if f.isBed() or f.isBigWig() or f.isGtf() or f.isHdf5() or f.isHotSpot():
                    f.download()
        print(idx + 1, "of", total, "done")
    except Exception:
        print(idx + 1, "of", total, "error")
        traceback.print_exc()


class Downloader:
    def __init__(self, dataset, args):
        self.dataset = dataset
        self.species = dataset.species
        self.args = args
        self.load()

    def load(self):
        # always redownload search
        Utils.ensureDir(self.dataset.jsonFnp)
        Utils.download(self.dataset.url, self.dataset.jsonFnp,
                       True, self.args.force)

        with open(self.dataset.jsonFnp) as f:
            self.data = json.load(f)

    def getFastqsHistone(self, dataset, args):
        expsJson = filter(lambda e: "ChIP-seq" == e["assay_term_name"],
                          self.data["@graph"])
        accessionIDs = sorted([e["accession"] for e in expsJson])
        total = len(accessionIDs)
        for idx, accessionID in enumerate(accessionIDs):
            try:
                exp = Exp.fromJsonFile(accessionID, False)
                print(idx + 1, "of", total, exp.encodeID, exp.assay_term_name,
                      exp.label, exp.description)
                if not exp.isChipSeqHistoneMark():
                    continue
                for f in exp.files:
                    if f.isFastqOrFasta():
                        f.download()
                print(idx + 1, "of", total, "done")
            except Exception:
                print(idx + 1, "of", total, "error")
                traceback.print_exc()

    def getBams(self, dataset, args):
        expsJson = filter(lambda e: "DNase-seq" == e["assay_term_name"],
                          self.data["@graph"])
        accessionIDs = sorted([e["accession"] for e in expsJson])
        total = len(accessionIDs)
        for idx, accessionID in enumerate(accessionIDs):
            try:
                exp = Exp.fromJsonFile(accessionID, False)
                print(idx + 1, "of", total, exp.encodeID, exp.assay_term_name,
                      exp.label, exp.description)
                for f in exp.bamFilters():
                    if "Dgf" in f.submitted_file_name:
                        continue
                    print("\t", f.fileID)
                    f.download()
                print(idx + 1, "of", total, "done")
            except Exception:
                print(idx + 1, "of", total, "error")
                traceback.print_exc()

    @staticmethod
    def checkBedBigWigHdf5BamParallel(n_jobs, accessionIDs, force, refresh,
                                      jsononly):
        t = len(accessionIDs)
        return Parallel(n_jobs=n_jobs)(delayed(loadBedBigWigHdf5Bam)(i, t, e,
                                                                     force,
                                                                     refresh,
                                                                     jsononly)
                                       for i, e in enumerate(accessionIDs))

    def _checkBedBigWigHdf5(self, assay_term_name, force, refresh, jsononly):
        expsJson = self.data["@graph"]
        if assay_term_name:
            expsJson = filter(lambda e: assay_term_name == e["assay_term_name"],
                              self.data["@graph"])
        accessionIDs = sorted([e["accession"] for e in expsJson])
        Downloader.checkBedBigWigHdf5BamParallel(self.args.j, accessionIDs,
                                                 force, refresh, jsononly)

    def chipseqs(self):
        self._checkBedBigWigHdf5("ChIP-seq", self.args.force,
                                 self.args.refresh, False)

    def dnases(self):
        self._checkBedBigWigHdf5("DNase-seq", self.args.force,
                                 self.args.refresh, False)

    def mnases(self):
        self._checkBedBigWigHdf5("MNase-seq", self.args.force,
                                 self.args.refresh, False)

    def checkAllBedBigWigHdf5(self, force=False, jsononly=False):
        self._checkBedBigWigHdf5("", force, self.args.refresh, jsononly)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--process', action="store_true", default=False)
    parser.add_argument('--refresh', action="store_true", default=False)
    parser.add_argument('--force', action="store_true", default=False)
    parser.add_argument('--fastq', action="store_true", default=False)
    parser.add_argument('--bams', action="store_true", default=False)
    parser.add_argument('--dnase', action="store_true", default=False)
    parser.add_argument('--chips', action="store_true", default=False)
    parser.add_argument('--jsononly', action="store_true", default=False)
    parser.add_argument('-j', type=int, default=4)
    parser.add_argument('--ids', type=str, default="")
    args = parser.parse_args()
    return args


def main():
    args = parse_args()

    if args.ids:
        accessionIDs = args.ids.split(',')
        Downloader.checkBedBigWigHdf5BamParallel(args.j, accessionIDs, True,
                                                 args.refresh, args.jsononly)
        return 0

    if args.dnase:
        datasets = [Datasets.all_mouse]
        for dataset in datasets:
            down = Downloader(dataset, args)
            down.dnases()

    datasets = [Datasets.roadmap, Datasets.all_mouse, Datasets.all_human]
    for dataset in datasets:
        down = Downloader(dataset, args)
        if args.fastq:
            down.getFastqsHistone(dataset, args)
        if args.bams:
            down.getBams(dataset, args)
        elif args.chips:
            down.chipseqs()
        else:
            down.checkAllBedBigWigHdf5(args.force, args.jsononly)


if __name__ == "__main__":
    sys.exit(main())
