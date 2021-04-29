from api.Handlers import Handler, Hubs
from api.util import PLdb as db, PLConfig as cfg
import pandas as pd
import os

problemColumns = ['chrom', 'chromStart', 'chromEnd']


class TrackInfoHandler(Handler.TrackHandler):
    """Handles Label Commands"""
    key = 'info'

    def do_GET(self, data, txn=None):
        return getTrackInfo(data['args'], txn=txn)

    def do_POST(self, data, txn=None):
        try:
            return self.getCommands()[data['command']](data['args'], txn=txn)
        except KeyError:
            print(data['command'], 'not yet implemented\n', data)

    @classmethod
    def getCommands(cls):
        return {'getGenome': getGenome,
                'getProblems': getProblems}


def getProblemsForChrom(genome, chrom, txn=None):
    problems = db.Problems(genome).get(txn=txn)

    return problems[problems['chrom'] == chrom].copy()


def getProblems(data, txn=None):
    if 'genome' not in data:
        data['genome'] = getGenome(data, txn=txn)

    problems = db.Problems(data['genome'])

    problemsInBounds = problems.getInBounds(data['ref'], data['start'], data['end'])

    if problemsInBounds is None:
        problemsPath = os.path.join(cfg.jbrowsePath, cfg.dataPath, 'genomes', data['genome'], 'problems.bed')

        if not os.path.exists(problemsPath):
            location = Hubs.generateProblems(data['genome'], problemsPath)
            if not location == problemsPath:
                raise Exception

        problemsDf = pd.read_csv(problemsPath, sep='\t', header=None)
        problemsDf.columns = problemColumns
        problems.put(problemsDf, txn=txn)

        problemsIsInBounds = problemsDf.apply(db.checkInBounds, axis=1, args=(data['ref'], data['start'], data['end']))

        return problemsDf[problemsIsInBounds].to_dict('records')
    else:
        return problemsInBounds.to_dict('records')


def getGenome(data, txn=None):
    hubInfo = db.HubInfo(data['user'], data['hub']).get(txn=txn)

    return hubInfo['genome']


def getTrackInfo(data, txn=None):
    hubInfo = db.HubInfo(data['user'], data['hub']).get(txn=txn)

    return hubInfo['tracks'][data['track']]
