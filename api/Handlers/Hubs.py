import os
import requests
import threading
import json
import tempfile
import gzip
import pandas as pd
from api.util import PLConfig as cfg, PLdb as db
from api.Handlers.Handler import Handler
from api.Handlers import Labels, Tracks, Models, Jobs


class HubHandler(Handler):
    """Handles Hub Commands"""
    def do_GET(self, data):
        args = data['args']
        if args['file'] == 'trackList.json':

            hubInfo = db.HubInfo(self.query['user'], self.query['hub']).get()

            return createTrackListWithHubInfo(hubInfo)
        else:
            print('no handler for %s' % self.query['handler'])

    def do_POST(self, data):
        return self.getCommands()[data['command']](data['args'], self.query)

    @classmethod
    def getCommands(cls):
        return {'goTo': goToRegion}


def goToRegion(data, query):
    user = query['user']
    hub = query['hub']

    hubInfo = db.HubInfo(user, hub).get()
    genome = hubInfo['genome']
    problems = db.Problems(genome).get()
    tracks = list(hubInfo['tracks'].keys())
    toGoTo = problems.apply(checkProblem, axis=1, args=(user, hub, tracks, data['type'].lower()))

    possibleRegions = problems[toGoTo]

    if possibleRegions.empty:
        return

    regionToGoTo = possibleRegions.sample()

    # Need to convert to something jbrowse will understand
    regionToGoTo = regionToGoTo.to_dict('records')[0]

    region = {
        'ref': regionToGoTo['chrom'],
        'start': regionToGoTo['chromStart'],
        'end': regionToGoTo['chromEnd']
    }

    return region


def checkProblem(row, user, hub, tracks, toCheck):

    trackDf = pd.DataFrame(tracks, columns=['track'])

    output = trackDf.apply(checkLabels, axis=1, args=(user, hub, row, toCheck))

    if toCheck == 'labeled':
        return output.any()
    else:
        return output.all()


def checkLabels(row, user, hub, problem, toCheck):
    labels = db.Labels(user,
                       hub,
                       row['track'],
                       problem['chrom']).getInBounds(problem['chrom'],
                                                     problem['chromStart'],
                                                     problem['chromEnd'])

    if labels.empty:
        if toCheck == 'unlabeled':
            return True
        else:
            return False
    else:
        if toCheck == 'unlabeled':
            return False
        else:
            return True


def createTrackListWithHubInfo(info):
    if info is None:
        return
    refSeqPath = os.path.join('genomes', info['genome'], 'trackList.json')

    tracklist = {'include': [refSeqPath], 'tracks': []}

    tracks = info['tracks']

    for track in tracks.keys():
        trackData = tracks[track]
        trackConf = {'label': track, 'key': trackData['key'], 'type': 'PeakLearnerBackend/View/Track/Model',
                     'showLabels': 'true', 'urlTemplates': [], 'category': trackData['categories']}

        urlTemplate = {'url': trackData['url'], 'name': track, 'color': '#235'}

        trackConf['urlTemplates'].append(urlTemplate)

        trackConf['storeClass'] = 'MultiBigWig/Store/SeqFeature/MultiBigWig'
        trackConf['storeConf'] = {'storeClass': 'PeakLearnerBackend/Store/SeqFeature/Labels',
                                  'modelClass': 'PeakLearnerBackend/Store/SeqFeature/Models'}

        tracklist['tracks'].append(trackConf)

    return tracklist


def parseHub(data):
    parsed = parseUCSC(data)
    # Add a way to configure hub here somehow instead of just loading everythingS
    return createHubFromParse(parsed)


# All this should probably be done asynchronously
def createHubFromParse(parsed):
    # Will need to add a way to add additional folder depth for userID once authentication is added
    hub = parsed['hub']
    user = parsed['user']
    genomesFile = parsed['genomesFile']

    # This will need to be updated if there are multiple genomes in file
    genome = genomesFile['genome']

    hubInfo = {'genome': genome}

    dataPath = os.path.join(cfg.jbrowsePath, cfg.dataPath)

    includes = getGeneTracks(genome, dataPath)

    # Generate problems for this genome

    txn = db.getTxn()
    problems = generateProblems(genome, dataPath, txn)
    txn.commit()

    problemPath = generateProblemTrack(problems)

    includes.append(problemPath)

    getRefSeq(genome, dataPath, includes)

    path = storeHubInfo(user, hub, genomesFile['trackDb'], hubInfo, genome)

    return path


def storeHubInfo(user, hub, tracks, hubInfo, genome):
    superList = []
    trackList = []
    hubInfoTracks = {}

    # Load the track list into something which can be converted
    for track in tracks:
        if 'superTrack' in track:
            superList.append(track)
            continue

        if 'parent' in track:
            for super in superList:
                if super['track'] == track['parent']:
                    trackList.append(track)
                    continue
            for parent in trackList:
                if parent['track'] == track['parent']:
                    if 'children' not in parent:
                        parent['children'] = []
                        parent['children'].append(track)
                    else:
                        parent['children'].append(track)

    for track in trackList:
        # Determine which track is the coverage data
        coverage = None
        for child in track['children']:
            file = child['bigDataUrl'].rsplit('/', 1)
            if 'coverage' in file[1]:
                coverage = child

        # Add Data Url to config
        if coverage is not None:
            categories = 'Data'
            for category in track['longLabel'].split(' | ')[:-1]:
                categories = categories + ' / %s' % category

            hubInfoTracks[track['track']] = {'categories': categories,
                                             'key': track['shortLabel'],
                                             'url': coverage['bigDataUrl']}

            checkForPrexistingLabels(coverage['bigDataUrl'], user, hub, track, genome)

    txn = db.getTxn()
    hubInfo['tracks'] = hubInfoTracks
    db.HubInfo(user, hub).put(hubInfo, txn=txn)
    txn.commit()

    return '/%s/' % os.path.join(str(user), hub)


def checkForPrexistingLabels(coverageUrl, user, hub, track, genome):
    trackUrl = coverageUrl.rsplit('/', 1)[0]
    labelUrl = '%s/labels.bed' % trackUrl
    with requests.get(labelUrl) as r:
        if not r.status_code == 200:
            return

        with tempfile.TemporaryFile() as f:
            f.write(r.content)
            f.flush()
            f.seek(0)
            labels = pd.read_csv(f, sep='\t', header=None)
            labels.columns = Labels.labelColumns

    grouped = labels.groupby('chrom')
    grouped.apply(saveLabelGroup, user, hub, track, genome, coverageUrl)


def saveLabelGroup(group, user, hub, track, genome, coverageUrl):
    group = group.sort_values('chromStart', ignore_index=True)

    group['annotation'] = group.apply(fixNoPeaks, axis=1)

    chrom = group['chrom'].loc[0]

    txn = db.getTxn()

    db.Labels(user, hub, track['track'], chrom).put(group, txn=txn)

    chromProblems = Tracks.getProblemsForChrom(genome, chrom, txn)

    withLabels = chromProblems.apply(checkIfProblemHasLabels, axis=1, args=(group,))

    doPregen = chromProblems[withLabels]

    submitPregenWithData(doPregen, user, hub, track, coverageUrl)

    txn.commit()


def submitPregenWithData(doPregen, user, hub, track, coverageUrl, txn=None):
    recs = doPregen.to_dict('records')
    for problem in recs:
        penalties = Models.getPrePenalties()
        job = Jobs.PregenJob(user,
                             hub,
                             track['track'],
                             problem,
                             penalties,
                             trackUrl=coverageUrl)

        job.putNewJob()


def checkIfProblemHasLabels(problem, labels):
    inBounds = labels.apply(db.checkInBounds,
                            axis=1,
                            args=(problem['chrom'],
                                  problem['chromStart'],
                                  problem['chromEnd']))

    return inBounds.any()


def fixNoPeaks(row):

    if row['annotation'] == 'noPeaks':
        return 'noPeak'
    return row['annotation']


def getRefSeq(genome, path, includes):
    genomeRelPath = os.path.join('genomes', genome)

    genomePath = os.path.join(path, genomeRelPath)

    includes = formatIncludes(includes, genomePath)

    if not os.path.exists(genomePath):
        try:
            os.makedirs(genomePath)
        except OSError:
            print(genomePath, "does not exist")
            return

    genomeUrl = cfg.geneUrl + genome + '/bigZips/' + genome + '.fa.gz'
    genomeFaPath = os.path.join(genomePath, genome + '.fa')
    genomeFaiPath = genomeFaPath + '.fai'

    downloadRefSeq(genomeUrl, genomeFaPath, genomeFaiPath)

    genomeConfigPath = os.path.join(genomePath, 'trackList.json')

    with open(genomeConfigPath, 'w') as genomeCfg:
        genomeFile = genome + '.fa.fai'
        output = {'refSeqs': genomeFile, 'include': includes}
        json.dump(output, genomeCfg)

    genomeConfigRelPath = os.path.join(genomeRelPath, 'trackList.json')

    return genomeConfigRelPath


def downloadRefSeq(genomeUrl, genomeFaPath, genomeFaiPath):
    if not os.path.exists(genomeFaiPath):
        if not os.path.exists(genomeFaPath):
            with open(genomeFaPath + '.gz', 'wb') as temp:
                # Gets FASTA file for genome
                with requests.get(genomeUrl, allow_redirects=True) as r:
                    temp.write(r.content)
                    temp.flush()
                    temp.seek(0)

            os.system('gzip -d %s' % genomeFaPath + '.gz')

        # Run samtools faidx {genome Fasta File}, creating an indexed Fasta file
        os.system('samtools faidx %s' % genomeFaPath)


def getGeneTracks(genome, dataPath):
    genomePath = os.path.join(dataPath, 'genomes', genome)

    genesPath = os.path.join(genomePath, 'genes')

    if not os.path.exists(genesPath):
        try:
            os.makedirs(genesPath)
        except OSError:
            return

    genesUrl = "%s%s/database/" % (cfg.geneUrl, genome)

    genes = ['ensGene', 'knownGene', 'ncbiRefSeq', 'refGene', 'ccdsGene']

    getDbFiles('trackDb', genesUrl, genesPath)

    includes = []

    geneThreads = []

    for gene in genes:
        geneTrackPath = os.path.join(genomePath, gene)

        args = (gene, genesUrl, genesPath, geneTrackPath)

        geneThread = threading.Thread(target=getAndProcessGeneTrack, args=args)
        geneThreads.append(geneThread)
        geneThread.start()

        includes.append(os.path.join(geneTrackPath, 'trackList.json'))
    for thread in geneThreads:
        thread.join()

    return includes


def getAndProcessGeneTrack(gene, genesUrl, genesPath, geneTrackPath):
    getDbFiles(gene, genesUrl, genesPath)

    trackListPath = os.path.join(geneTrackPath, 'trackList.json')

    if not os.path.exists(geneTrackPath):
        try:
            os.makedirs(geneTrackPath)
        except OSError:
            return

    if not os.path.exists(trackListPath):
        command = os.path.join(cfg.jbrowsePath, 'bin', 'ucsc-to-json.pl')

        generateTrack = "%s -q --in %s --out %s --track %s" % (command, genesPath, geneTrackPath, gene)

        # This will use Jbrowse perl files to generate a track for that specific gene
        os.system(generateTrack)

        addGeneCategory(geneTrackPath, 'Reference / Genes')


def generateProblemTrack(path):
    trackFolder = '%s/' % path.rsplit('.', 1)[0]

    if not os.path.exists(trackFolder):
        try:
            os.makedirs(trackFolder)
        except OSError:
            return

        command = os.path.join(cfg.jbrowsePath, 'bin', 'flatfile-to-json.pl')

        generateTrack = '%s --bed %s --out %s --trackLabel Contigs' % (command, path, trackFolder)

        # Will generate a jbrowse track using the problems.bed flatfile
        os.system(generateTrack)

        addGeneCategory(trackFolder, 'Reference')

    return os.path.join(trackFolder, 'trackList.json')


def getDbFiles(name, url, output):
    files = ['%s.txt.gz' % name, '%s.sql' % name]

    for file in files:
        path = os.path.join(output, file)
        if not os.path.exists(path):
            with requests.get(url + file, allow_redirects=True) as r:
                if not r.status_code == 200:
                    print("getDbFile Error", r.status_code)
                with open(path, 'wb') as f:
                    f.write(r.content)


def addGeneCategory(genePath, label):
    confFile = os.path.join(genePath, 'trackList.json')

    conf = json.loads(open(confFile, 'r').read())

    # Only one track in these confs
    if 'category' not in conf['tracks'][0]:
        conf['tracks'][0]['category'] = label

    with open(confFile, 'w') as newConfFile:
        json.dump(conf, newConfFile)


def formatIncludes(includes, prePath):
    output = []

    for include in includes:
        output.append(os.path.relpath(include, start=prePath))

    return output


def parseUCSC(data):
    url = data['url']
    hubReq = requests.get(url, allow_redirects=True)
    if not hubReq.status_code == 200:
        return None
    path = ""
    if url.find('/'):
        vals = url.rsplit('/', 1)
        path = vals[0]

    lines = hubReq.text.split('\n')

    hub = readUCSCLines(lines)

    # TODO: Add User to query

    hub['user'] = data['user']

    if hub['genomesFile']:
        hub['genomesFile'] = loadGenomeUCSC(hub, path)

    return hub


def loadGenomeUCSC(hub, path):
    genomeUrl = path + '/' + hub['genomesFile']

    genomeReq = requests.get(genomeUrl, allow_redirects=True)

    lines = genomeReq.text.split('\n')

    output = readUCSCLines(lines)

    if output['trackDb'] is not None:
        output['trackDb'] = loadTrackDbUCSC(output, path)

    return output


def loadTrackDbUCSC(genome, path):
    trackUrl = path + '/' + genome['trackDb']

    trackReq = requests.get(trackUrl, allow_redirects=True)

    lines = trackReq.text.split('\n')

    return readUCSCLines(lines)


# Reads the lines of the current file
def readUCSCLines(lines):
    output = []
    current = {}

    outputList = False

    start = True

    added = False

    for line in lines:
        line = line.strip()
        if line == "":
            if start:
                start = False
                continue
            else:
                if not added:
                    added = True
                    output.append(current)
                    current = {}
        else:
            # If it gets to this point after adding one, then there are multiple
            if added:
                outputList = True
                added = False
            vals = line.split(" ", 1)

            current[vals[0]] = vals[1]

    if outputList:
        return output
    else:
        return output[0]


def generateProblems(genome, path, txn=None):
    genesUrl = "%s%s/database/" % (cfg.geneUrl, genome)
    genomePath = os.path.join(path, 'genomes', genome)
    outputFile = os.path.join(genomePath, 'problems.bed')

    if db.Problems.has_key(genome):
        return outputFile

    if not os.path.exists(genomePath):
        try:
            os.makedirs(genomePath)
        except OSError:
            return

    files = []

    for file in ['chromInfo', 'gap']:
        outputPath = os.path.join(genomePath, file + '.txt')
        fileUrl = genesUrl + file + '.txt.gz'
        files.append(downloadAndUnpackFile(fileUrl, outputPath))

    chromInfo = pd.read_csv(files[0], sep='\t', header=None).iloc[:, 0:2]
    chromInfo.columns = ['chrom', 'bases']

    gap = pd.read_csv(files[1], sep='\t', header=None).iloc[:, 1:4]
    gap.columns = ['chrom', 'gapStart', 'gapEnd']

    join = gap.merge(chromInfo, on='chrom', how='outer')

    nan = join.isnull()['gapStart' and 'gapEnd']
    nonNan = join.notnull()['gapStart' and 'gapEnd']

    nanOutput = join[nan].groupby(['chrom']).apply(createNanProblems)

    nonNanOutput = join[nonNan].groupby(['chrom']).apply(createProblems)

    frames = [nonNanOutput, nanOutput]

    output = pd.concat(frames, sort=False)

    # Removes all entries with an _, not needed because these are genome "Fixes"
    output = output[~output['chrom'].str.contains('_')]

    output['chromStart'] = output['chromStart'].astype(int)
    output['chromEnd'] = output['chromEnd'].astype(int)

    db.Problems(genome).put(output, txn=txn)

    output.to_csv(outputFile, sep='\t', index=False, header=False)

    return outputFile


def createNanProblems(args):
    data = {'chrom': args['chrom'], 'chromStart': [0], 'chromEnd': args['bases']}

    return pd.DataFrame(data)


def createProblems(group):
    chrom = group['chrom'].iloc[0]
    bases = group['bases'].iloc[0]
    chromStart = [0, ]
    gapEnd = group['gapEnd'].tolist()
    chromEnd = group['gapStart'].tolist()
    chromStart.extend(gapEnd)
    chromEnd.append(bases)

    if len(chromStart) != len(chromStart):
        return

    data = {'chrom': chrom, 'chromStart': chromStart, 'chromEnd': chromEnd}

    df = pd.DataFrame(data)
    return df[df['chromStart'] < df['chromEnd']]


def downloadAndUnpackFile(url, path):
    if not os.path.exists(path):
        with tempfile.NamedTemporaryFile(suffix='.txt.gz') as temp:
            # Gets FASTA file for genome
            with requests.get(url, allow_redirects=True) as r:
                temp.write(r.content)
                temp.flush()
                temp.seek(0)
            with gzip.GzipFile(fileobj=temp, mode='r') as gz:
                # uncompress the flatfile
                with open(path, 'w+b') as faFile:
                    # Save to file
                    faFile.write(gz.read())
    return path


def getHubInfo(user, hub):
    return db.HubInfo(user, hub).get()



