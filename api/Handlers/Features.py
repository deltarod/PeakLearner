from api.Handlers.Handler import TrackHandler
from api.util import PLdb as db
import pandas as pd


class FeatureHandler(TrackHandler):
    key = 'features'
    """Handles Feature Commands"""

    def do_POST(self, data):
        return self.getCommands()[data['command']](data['args'])

    @classmethod
    def getCommands(cls):
        return {'get': getFeatures,
                'put': putFeatures,
                'getAll': getAllFeatures}


def getFeatures(data):
    problem = data['problem']
    features = db.Features(data['user'], data['hub'], data['track'], problem['chrom'], problem['chromStart'])
    return features.get()


def getAllFeatures(data):
    print(data)


def putFeatures(data):
    """ Saves features to be later used for prediction and learning"""
    if not isinstance(data['data'], list) and not len(data['data']) == 1:
        raise Exception(data['data'])

    problem = data['problem']
    features = db.Features(data['user'], data['hub'], data['track'], problem['chrom'], problem['chromStart'])
    txn = db.getTxn()
    features.put(pd.Series(data['data'][0]), txn=txn)
    txn.commit()
