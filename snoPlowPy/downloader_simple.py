#!/usr/bin/env python

from __future__ import print_function

import sys
import argparse
import traceback

from .exp import Exp


class DownloaderSimple:
    def __init__(self, accessionIDs, args):
        self.accessionIDs = accessionIDs
        self._args = args

    def run(self):
        total = len(self.accessionIDs)
        for idx, accessionID in enumerate(self.accessionIDs):
            try:
                exp = Exp.fromJsonFile(accessionID, False)
                print(idx + 1, "of", total, exp.encodeID, exp.assay_term_name,
                      exp.label, exp.description)
                for f in exp.files:
                    if f.isBigBed() or self._args.fastq and f.isFastqOrFasta() \
                            or self._args.tsv and f.isTSV() or self._args.bam and f.isBam() \
                            or self._args.bigwig and f.isBigWig():
                        f.download()
                        print(f.fnp())
                print(idx + 1, "of", total, "done")
            except Exception:
                print(idx + 1, "of", total, "error")
                traceback.print_exc()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--process', action="store_true", default=False)
    parser.add_argument('--fastq', action="store_true", default=False)
    parser.add_argument('--bigwig', action="store_true", default=False)
    parser.add_argument('--bam', action="store_true", default=False)
    parser.add_argument('--tsv', action="store_true", default=False)
    parser.add_argument('-f', type=str, default=False)
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    if args.f:
        with open(args.f, 'r') as f:
            for line in f:
                ds = DownloaderSimple([line.strip("\n")], args)
                ds.run()


if __name__ == "__main__":
    sys.exit(main())
