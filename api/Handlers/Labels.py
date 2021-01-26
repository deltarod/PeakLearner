import pandas as pd
from api.util import PLdb as db
from api.Handlers import Models, Handler
from api.Prediction import PeakSegDiskPredict as psgPredict

labelColumns = ['chrom', 'chromStart', 'chromEnd', 'annotation']
jbrowseLabelColumns = ['ref', 'start', 'end', 'label']


class LabelHandler(Handler.TrackHandler):
    """Handles Label Commands"""
    key = 'labels'

    def do_POST(self, data):
        return self.getCommands()[data['command']](data['args'])

    @classmethod
    def getCommands(cls):
        return {'add': addLabel,
                'remove': removeLabel,
                'update': updateLabel,
                'get': getLabels}


def addLabel(data):
    label = 'unknown'
    print('adding label for track', data['track'])

    # Duplicated because calls from updateLabel are causing freezing
    newLabel = pd.Series({'chrom': data['ref'],
                          'chromStart': data['start'],
                          'chromEnd': data['end'],
                          'annotation': label})

    db.Labels(data['user'], data['hub'], data['track'], data['ref']).add(newLabel)

    return data


# Removes label from label file
def removeLabel(data):
    toRemove = pd.Series({'chrom': data['ref'],
                          'chromStart': data['start'],
                          'chromEnd': data['end']})

    labels = db.Labels(data['user'], data['hub'], data['track'], data['ref'])
    removed, after = labels.remove(toRemove)
    Models.updateAllModelLabels(data, after)
    return removed.to_dict()


def updateLabel(data):
    label = data['label']

    updateLabel = pd.Series({'chrom': data['ref'],
                          'chromStart': data['start'],
                          'chromEnd': data['end'],
                          'annotation': label})
    labelDb = db.Labels(data['user'], data['hub'], data['track'], data['ref'])
    item, labels = labelDb.add(updateLabel)
    Models.updateAllModelLabels(data, labels)
    return item.to_dict()


def getLabels(data):
    labels = db.Labels(data['user'], data['hub'], data['track'], data['ref'])
    labelsDf = labels.getInBounds(data['ref'], data['start'], data['end'])
    if len(labelsDf.index) < 1:
        return []

    labelsDf = labelsDf[labelColumns]
    labelsDf.columns = jbrowseLabelColumns

    return labelsDf.to_dict('records')
