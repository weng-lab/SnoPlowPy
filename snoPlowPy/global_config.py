#!/usr/bin/env python
import os
import ConfigParser


class GlobalConfig:
    fnp = os.path.join(os.path.dirname(__file__), "../global_config.ini")

    c = ConfigParser.ConfigParser()
    c.read(fnp)

    metadataDirs = [os.getenv("METADATA_BASEDIR")] + c.get("Paths", "metadata").split(',')
