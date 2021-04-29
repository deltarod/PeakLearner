import pandas as pd
from api.util import PLdb as db
from .Handler import TrackHandler

lossColumns = ['penalty',
               'segments',
               'peaks',
               'totalBases',
               'bedGraphLines',
               'meanPenalizedCost',
               'totalUnpenalizedCost',
               'numConstraints',
               'meanIntervals',
               'maxIntervals']


class LossHandler(TrackHandler):
    """Handles Label Commands"""
    key = 'loss'

    def do_POST(self, data, txn=None):
        return self.getCommands()[data['command']](data['args'], txn=txn)

    @classmethod
    def getCommands(cls):
        return {'put': putLoss}


def putLoss(data, txn=None):
    penalty = data['penalty']
    lossInfo = data['lossInfo']
    user = lossInfo['user']
    hub = lossInfo['hub']
    track = lossInfo['track']
    problem = lossInfo['problem']

    lossData = pd.read_json(data['lossData'])
    lossData['meanLoss'] = lossData['meanPenalizedCost'] - lossData['peaks']*lossData['penalty']

    lossDb = db.Loss(user, hub, track, problem['chrom'], problem['chromStart'], penalty)

    lossDb.put(lossData, txn=txn)

    return True
