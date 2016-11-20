#!/usr/bin/env python

from __future__ import print_function
import json
from .utils import Utils
from .exp import Exp


class QueryDCC:
    def __init__(self, host=None, auth=True, cache=None):
        self.auth = auth
        self.host = "https://www.encodeproject.org"
        if host:
            self.host = host
        self.cache = cache

    def getURL(self, url, quiet=False):
        if self.cache:
            ret = self.cache.getOrSet(url, lambda: Utils.query(url,
                                                               auth=self.auth,
                                                               quiet=quiet),
                                      quiet)
        else:
            ret = Utils.query(url, auth=self.auth, quiet=quiet)
        if not ret:
            raise Exception("could not download " + url)
        return ret

    def getFromAlias(self, alias, quiet=False):
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
