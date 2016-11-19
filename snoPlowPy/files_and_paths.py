#!/usr/bin/env python

import os
import sys
from global_config import GlobalConfig


def FindMetadataBaseDir():
    dirs = GlobalConfig.metadataDirs
    return BaseDirFromList(dirs, "ENCODE metadata", "METADATA_BASEDIR")


def FindJobmonitorBaseDir():
    dirs = [os.getenv("JOBMONITOR_BASEDIR"),
            "/project/umw_zhiping_weng/0_jobmonitor/",
            "/nfs/0_jobmonitor@bib5/"]
    return BaseDirFromList(dirs, "job monitor", "JOBMONITOR_BASEDIR", False)


def FindRepositoryDir():
    dirs = [os.getenv("WENG_LAB"),
            os.path.expanduser("~/weng-lab")]
    return BaseDirFromList(dirs, "weng lab repository", "WENG_LAB", False)


def BaseDirFromList(dirs, dirname, envsearched, exit_on_failure=True):
    dirs = [d for d in dirs if d]  # remove "None"s
    for d in dirs:
        if os.path.exists(d):
            return d
    print "missing %s base folder; searched " % dirname, ";".join(dirs)
    print "check directory or %s environment variable." % envsearched
    if exit_on_failure:
        sys.exit(1)
    return "/tmp"


class Dirs:
    metadata_base = FindMetadataBaseDir()
    jobmonitor_base = FindJobmonitorBaseDir()
    wenglab_base = FindRepositoryDir()
    wenglab_metadata = os.path.join(wenglab_base, "metadata")
    wenglab_encyclopedia = os.path.join(wenglab_base, "encyclopedia")

    encode_base = os.path.join(metadata_base, "encode")
    encode_data = os.path.join(encode_base, "data")
    encode_json = os.path.join(encode_base, "json")
    encode_experiment_json = os.path.join(encode_json, "exps")
    encode_project_json = os.path.join(encode_json, "project")
    encode_dataset_json = os.path.join(encode_json, "datasets")
    encode_validation_data = os.path.join(metadata_base, "tools/ENCODE/validation/encValData")
    mean_data = os.path.join(encode_base, "mean")

    roadmap_base = os.path.join(metadata_base, "roadmap", "data", "consolidated")

    tools = os.path.join(metadata_base, "tools")
    genomes = os.path.join(metadata_base, "genome")
    gencode_m8 = os.path.join(genomes, "gencode.m8")

    encyclopedia = os.path.join(metadata_base, "encyclopedia")
    dbsnps = os.path.join(genomes, "dbsnps")

    enhancerTracksBase = os.path.join("Enhancer-Prediction-Tracks", "March-2016")
    enhancerTracks = os.path.join(encyclopedia,
                                  enhancerTracksBase)
    promoterTracksBase = os.path.join("Promoter-Prediction-Tracks")
    promoterTracks = os.path.join(encyclopedia,
                                  promoterTracksBase)
    targetGeneTracksBase = os.path.join("Target-Gene-Prediction-Tracks",
                                        "convert")
    targetGeneTracks = os.path.join(encyclopedia,
                                    targetGeneTracksBase)

    job_output_base = os.path.join(jobmonitor_base, "joboutput")

    liftOverChainFiles = os.path.join(tools, "ucsc.liftOver")

    @staticmethod
    def ToolsFnp(fn):
        fnp = os.path.join(Dirs.tools, fn)
        if not os.path.exists(fnp):
            print "WARN: tool missing:", fnp
        return fnp

    @staticmethod
    def GenomeFnp(fn):
        fnp = os.path.join(Dirs.genomes, fn)
        if not os.path.exists(fnp):
            print "genome file missing:", fnp
            raise Exception("file missing: " + fnp)
        return fnp

    @staticmethod
    def LiftOverChainFnp(fn):
        fnp = os.path.join(Dirs.liftOverChainFiles, fn)
        if not os.path.exists(fnp):
            print "liftOver chain file missing:", fnp
            raise Exception("file missing: " + fnp)
        return fnp


class Genome:
    hg19_chr_lengths = Dirs.GenomeFnp("hg19.chromInfo")
    hg19_2bit = Dirs.GenomeFnp("hg19.2bit")
    hg38_chr_lengths = Dirs.GenomeFnp("hg38.chrom.sizes")
    hg38_2bit = Dirs.GenomeFnp("hg38.2bit")
    GRCh38_chr_lengths = Dirs.GenomeFnp("GRCh38.chrom.sizes")
    GRCh38_2bit = Dirs.GenomeFnp("hg38.2bit")
    mm9_chr_lengths = Dirs.GenomeFnp("mm9.chromInfo")
    mm9_2bit = Dirs.GenomeFnp("mm9.2bit")
    mm10_chr_lengths = Dirs.GenomeFnp("mm10.chromInfo")
    mm10_minimal_chr_lengths = Dirs.GenomeFnp("mm10-minimal.chromInfo")
    mm10_2bit = Dirs.GenomeFnp("mm10.2bit")

    human_gencode_tss = Dirs.GenomeFnp("gencode.v19.annotation.tss.bed")
    mouse_gencode_m1_tss = Dirs.GenomeFnp("gencode.vM1.annotation.tss.bed")
    mouse_gencode_m8_tss = Dirs.GenomeFnp("gencode.m8.annotation.tss.bed")

    mouse_gencode_m8_gtf_url = ("ftp://ftp.sanger.ac.uk/pub/gencode/" +
                                "Gencode_mouse/release_M8/gencode.vM8.annotation.gtf.gz")
    mouse_gencode_m8_gff_url = ("ftp://ftp.sanger.ac.uk/pub/gencode/" +
                                "Gencode_mouse/release_M8/gencode.vM8.annotation.gff3.gz")

    hg19_idr_blacklist = Dirs.GenomeFnp("blacklist/hg19/" +
                                        "wgEncodeDacMapabilityConsensusExcludable.bed")
    mm9_idr_blacklist = Dirs.GenomeFnp("blacklist/mm9/mm9-blacklist.bed")
    mm10_idr_blacklist = Dirs.GenomeFnp("blacklist/mm10/mm10-blacklist.bed")

    hg19_mm10_liftOver_chain = Dirs.GenomeFnp("hg19ToMm10.over.chain.gz")
    mm9_mm10_liftOver_chain = Dirs.GenomeFnp("mm9ToMm10.over.chain.gz")

    @staticmethod
    def ChrLenByAssembly(a):
        files = {"hg19": Genome.hg19_chr_lengths,
                 "hg38": Genome.hg38_chr_lengths,
                 "GRCh38": Genome.GRCh38_chr_lengths,
                 "mm9": Genome.mm9_chr_lengths,
                 "mm10": Genome.mm10_chr_lengths,
                 "mm10-minimal": Genome.mm10_chr_lengths}
        return files[a]

    @staticmethod
    def BlacklistByAssembly(a):
        files = {"hg19": Genome.hg19_idr_blacklist,
                 "mm9": Genome.mm9_idr_blacklist,
                 "mm10": Genome.mm10_idr_blacklist,
                 "mm10-minimal": Genome.mm10_idr_blacklist}
        return files[a]

    @staticmethod
    def GencodeTSSByAssembly(a):
        files = {"hg19": Genome.human_gencode_tss,
                 "mm9": Genome.mouse_gencode_m1_tss,
                 "mm10": Genome.mouse_gencode_m8_tss,
                 "mm10-minimal": Genome.mouse_gencode_m8_tss}
        return files[a]

    @staticmethod
    def TwoBitByAssembly(a):
        files = {"hg19": Genome.hg19_2bit,
                 "mm9": Genome.mm9_2bit,
                 "mm10": Genome.mm10_2bit,
                 "mm10-minimal": Genome.mm10_2bit}
        return files[a]


class Tools:
    ASdnaseTrack = Dirs.ToolsFnp("ucsc.v287/as/dnase.track.as")
    CLIPper = Dirs.ToolsFnp("clipper/bin/clipper")
    bedClip = Dirs.ToolsFnp("ucsc.v287/bedClip")
    bedGraphToBigWig = Dirs.ToolsFnp("ucsc.v287/bedGraphToBigWig")
    bedToBigBed = Dirs.ToolsFnp("ucsc.v287/bedToBigBed")
    bedtools = "bedtools"
    bigWigAverageOverBed = Dirs.ToolsFnp("ucsc.v287/bigWigAverageOverBed")
    bigWigToBedGraph = Dirs.ToolsFnp("ucsc.v287/bigWigToBedGraph")
    ceqlogo = Dirs.ToolsFnp("meme_4.10.2/bin/ceqlogo")
    dfilter = Dirs.ToolsFnp("DFilter1.6/run_dfilter.sh")
    fastaCenter = Dirs.ToolsFnp("meme_4.10.2/bin/fasta-center")
    fimo = Dirs.ToolsFnp("meme_4.10.2/bin/fimo")
    headRest = os.path.join(Dirs.ToolsFnp("ucsc.v287"), "headRest")
    liftOver = os.path.join(Dirs.ToolsFnp("ucsc.v287"), "liftOver")
    meme = Dirs.ToolsFnp("meme_4.10.2/bin/meme")
    # randomLines = os.path.join(Dirs.ToolsFnp("ucsc.v287"), "randomLines")
    randomLines = Dirs.ToolsFnp("randomLines")
    tomtom = Dirs.ToolsFnp("meme_4.10.2/bin/tomtom")
    twoBitToFa = Dirs.ToolsFnp("ucsc.v287/twoBitToFa")
    validateFiles = Dirs.ToolsFnp("ENCODE/validation/validateFiles")
    wigToBigWig = Dirs.ToolsFnp("ucsc.v287/wigToBigWig")
    wiggleTools = Dirs.ToolsFnp("wiggletools.static.git.7579e66")


class Urls:
    base = "https://www.encodeproject.org"


class Webservice:
    urlBase = "http://bib7.umassmed.edu/ws/metadata/"
    jobmonitorBase = "http://bib7.umassmed.edu/ws/job_monitor/"
    localhostJMBase = "http://127.0.0.1:9191/job_monitor/"
    localhostBase = "http://127.0.0.1:9191/metadata/"
    rsyncBase = "http://bib7.umassmed.edu:%d/rsync/"

    @staticmethod
    def rsyncUrl(localpath, remotepath, rsync_port):
        return (Webservice.rsyncBase + "/request_rsync?local-path=%s&remote-path=%s") % (rsync_port,
                                                                                         localpath,
                                                                                         remotepath)

    @staticmethod
    def localExp(encodeID, localhost=False):
        if localhost:
            return os.path.join(Webservice.localhostBase, "exp", encodeID)
        return os.path.join(Webservice.urlBase, "exp", encodeID)

    @staticmethod
    def localUrl(uri):
        return Webservice.localhostBase + uri

    @staticmethod
    def JobMonitorPutUrl(uri, localhost=False):
        return os.path.join(Webservice.localhostJMBase
                            if localhost else Webservice.jobmonitorBase,
                            "output", uri)

    @staticmethod
    def JobMonitorSelectUrl(localhost=False):
        return os.path.join(Webservice.localhostJMBase
                            if localhost else Webservice.jobmonitorBase,
                            "select")

    @staticmethod
    def JobMonitorActionUrl(localhost=False):
        return os.path.join(Webservice.localhostJMBase
                            if localhost else Webservice.jobmonitorBase,
                            "db_act")

    @staticmethod
    def JobMonitorUrl(uri, localhost=False):
        return os.path.join(Webservice.localhostJMBase
                            if localhost else Webservice.jobmonitorBase, uri)


class AllHumanDataset:
    url = (Urls.base + "/search/?type=Experiment" +
           "&replicates.library.biosample.donor.organism.scientific_name=Homo" +
           "+sapiens&limit=all&format=json")
    jsonFnp = os.path.join(Dirs.encode_json, "datasets", "all_human.json")
    species = "human"
    chr_lengths = Genome.hg19_chr_lengths
    twoBit = Genome.hg19_2bit
    genome = "hg19"
    assemblies = ["hg19", "GRCh38"]
    webserviceAll = os.path.join(Webservice.urlBase, "encode/all_human/")
    webserviceTF = os.path.join(Webservice.urlBase, "encode/all_human/chipseq/tf")
    webserviceHistone = os.path.join(Webservice.urlBase, "encode/all_human/chipseq/histone")
    webserviceDNase = os.path.join(Webservice.urlBase, "encode/all_human/dnase")
    webserviceMNase = os.path.join(Webservice.urlBase, "encode/all_human/mnase")
    webserviceMethylation = os.path.join(Webservice.urlBase, "encode/all_human/methylation")
    webserviceDnaseChipHg19 = os.path.join(Webservice.urlBase, "encode/all_human/dnaseAndChip/hg19")
    webserviceAllBeds = os.path.join(Webservice.urlBase, "encode/all_human/beds")
    webservice_eCLIP = os.path.join(Webservice.urlBase, "encode/all_human/eCLIP")
    Webservice_biosample_term_name = os.path.join(Webservice.urlBase,
                                                  "encode/biosample_term_name/")


class RoadmapConsolidatedDataset:
    url = (Urls.base + "/search/?type=ReferenceEpigenome" +
           "&organism.scientific_name=Homo+sapiens&lab.title=Anshul" +
           "+Kundaje%2C+Stanford&limit=all&format=json")
    jsonFnp = os.path.join(Dirs.encode_json, "datasets", "roadmap.consolidated.json")
    species = "human"
    chr_lengths = Genome.hg19_chr_lengths
    twoBit = Genome.hg19_2bit
    genome = "hg19"
    assemblies = ["hg19", "GRCh38"]


class RoadmapDataset:
    url = (Urls.base + "/search/?searchTerm=roadmap&type=Experiment" +
           "&award.project=Roadmap&limit=all&format=json")
    jsonFnp = os.path.join(Dirs.encode_json, "datasets", "roadmap.json")
    species = "human"
    chr_lengths = Genome.hg19_chr_lengths
    twoBit = Genome.hg19_2bit
    genome = "hg19"
    assemblies = ["hg19", "GRCh38"]


class AllMouseDataset:
    url = (Urls.base + "/search/?type=experiment" +
           "&replicates.library.biosample.donor.organism.scientific_name=Mus%20musculus" +
           "&limit=all&format=json")
    jsonFnp = os.path.join(Dirs.encode_json, "datasets", "all_mouse.json")
    species = "mouse"
    assemblies = ["mm9", "mm10-minimal", "mm10"]
    webserviceAll = os.path.join(Webservice.urlBase, "encode/all_mouse/")
    webserviceDnaseChip = os.path.join(Webservice.urlBase, "encode/all_mouse/dnaseAndChip")
    webserviceDnaseChipMm9 = os.path.join(Webservice.urlBase, "encode/all_mouse/dnaseAndChip/mm9")
    webserviceDnaseChipMm10 = os.path.join(Webservice.urlBase, "encode/all_mouse/dnaseAndChip/mm10")
    webserviceTF = os.path.join(Webservice.urlBase, "encode/all_mouse/chipseq/tf")
    webserviceHistone = os.path.join(Webservice.urlBase, "encode/all_mouse/chipseq/histone")
    webserviceDNase = os.path.join(Webservice.urlBase, "encode/all_mouse/dnase")
    webserviceMethylation = os.path.join(Webservice.urlBase, "encode/all_mouse/methylation")
    webserviceAllBeds = os.path.join(Webservice.urlBase, "encode/all_mouse/beds")
    webservice_eCLIP = os.path.join(Webservice.urlBase, "encode/all_mouse/eCLIP")


class smallRNAdataset:
    url = ("https://www.encodeproject.org/search/?type=experiment" +
           "&assay_term_name=RNA-seq" +
           "&replicates.library.biosample.donor.organism.scientific_name=Homo+sapiens" +
           "&replicates.library.size_range=%3C200&status=released&limit=all" +
           "&files.file_type=fastq&files.read_length=101&format=json")
    jsonFnp = os.path.join(Dirs.encode_json, "datasets", "junko.smallRNA.json")
    species = "NA"


class cricketDataset:
    url = ("https://www.encodeproject.org/search/?type=project" +
           "&lab.title=Zhiping%20Weng,%20UMass&status=released&format=json")
    jsonFnp = os.path.join(Dirs.encode_json, "datasets", "cricket.json")
    species = "NA"


class encode3DNaseHuman:
    # this is a (authenticated) search for datasets using the "'award.rfa=ENCODE3' trick"
    # to select for ENCODE3 data only: all human ENCODE3 DNase-seq datasets from John Stam's lab.
    url = ("https://www.encodeproject.org/search/?type=experiment&award.rfa=ENCODE3" +
           "&assay_term_name=DNase-seq" +
           "&replicates.library.biosample.donor.organism.scientific_name=Homo%20sapiens" +
           "&limit=all&lab.title=John%20Stamatoyannopoulos,%20UW&format=json")
    jsonFnp = os.path.join(Dirs.encode_json, "datasets", "encode3_human_dnase.json")
    species = "human"


class RoadmapFromEncode:
    url = ("https://www.encodeproject.org/search/?award.project=Roadmap" +
           "&type=experiment&limit=all&format=json")
    jsonFnp = os.path.join(Dirs.encode_json, "datasets", "roadmap.json")
    species = "NA"


class ENCODE3MouseForChromHMM:
    # all histone mods for mouse as defined in listed projects.
    webserviceAll = os.path.join(Webservice.urlBase,
                                 "encode/byDataset/ENCSR215KPY/ENCSR557UVG/" +
                                 "ENCSR837UJN/ENCSR846VTS/ENCSR647ZZB/ENCSR392ERD")


class Datasets:
    all_human = AllHumanDataset
    all_mouse = AllMouseDataset
    roadmap = RoadmapDataset
    smallRNA = smallRNAdataset
    cricket = cricketDataset
    encode3DNaseHuman = encode3DNaseHuman
    roadmapFromEncode = RoadmapFromEncode
    ENCODE3MouseForChromHMM = ENCODE3MouseForChromHMM
