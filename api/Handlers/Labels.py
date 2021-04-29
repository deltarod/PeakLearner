import pandas as pd
from api.util import PLdb as db
from api.Handlers import Models, Handler
from berkeleydb.dbutils import DeadlockWrap

labelColumns = ['chrom', 'chromStart', 'chromEnd', 'annotation']
jbrowseLabelColumns = ['ref', 'start', 'end', 'label']


class LabelHandler(Handler.TrackHandler):
    """Handles Label Commands"""
    key = 'labels'

    def do_POST(self, data, txn=None):
        return self.getCommands()[data['command']](data['args'], txn=txn)

    @classmethod
    def getCommands(cls):
        return {'add': addLabel,
                'remove': removeLabel,
                'update': updateLabel,
                'updateAligned': updateAlignedLabels,
                'removeAligned': removeAlignedLabels,
                'get': getLabels}


def addLabel(data, txn=None):
    newLabel = pd.Series({'chrom': data['ref'],
                          'chromStart': data['start'],
                          'chromEnd': data['end'],
                          'annotation': data['label']})

    labelsDb = db.Labels(data['user'], data['hub'], data['track'], data['ref'])
    labels = labelsDb.get(txn=txn, write=True)
    if not labels.empty:
        inBounds = labels.apply(db.checkInBounds, axis=1, args=(data['ref'], data['start'], data['end']))
        # If there are any labels currently stored within the region which the new label is being added
        if inBounds.any():
            raise db.AbortTXNException

    labels = labels.append(newLabel, ignore_index=True).sort_values('chromStart', ignore_index=True)

    labelsDb.put(labels, txn=txn)
    db.Prediction('changes').increment(txn=txn)
    Models.updateAllModelLabels(data, labels, txn)
    return data


# Removes label from label file
def removeLabel(data, txn=None):
    toRemove = pd.Series({'chrom': data['ref'],
                          'chromStart': data['start'],
                          'chromEnd': data['end']})

    labels = db.Labels(data['user'], data['hub'], data['track'], data['ref'])
    removed, after = labels.remove(toRemove, txn=txn)
    db.Prediction('changes').increment(txn=txn)
    Models.updateAllModelLabels(data, after, txn)
    return removed.to_dict()


def removeAlignedLabels(data, txn=None):
    labelToRemove = pd.Series({'chrom': data['ref'],
                             'chromStart': data['start'],
                             'chromEnd': data['end']})

    user = data['user']
    hub = data['hub']

    for track in data['tracks']:
        trackTxn = db.getTxn(parent=txn)
        labelDb = db.Labels(user, hub, track, data['ref'])
        item, labels = labelDb.remove(labelToRemove, txn=trackTxn)
        db.Prediction('changes').increment(txn=trackTxn)
        Models.updateAllModelLabels(data, labels, trackTxn)
        trackTxn.commit()


def updateLabel(data, txn=None):
    labelToUpdate = pd.Series({'chrom': data['ref'],
                          'chromStart': data['start'],
                          'chromEnd': data['end'],
                          'annotation': data['label']})
    labelDb = db.Labels(data['user'], data['hub'], data['track'], data['ref'])
    item, labels = labelDb.add(labelToUpdate, txn=txn)
    db.Prediction('changes').increment(txn=txn)
    Models.updateAllModelLabels(data, labels, txn)
    return item.to_dict()


def updateAlignedLabels(data, txn=None):
    labelToUpdate = pd.Series({'chrom': data['ref'],
                             'chromStart': data['start'],
                             'chromEnd': data['end'],
                             'annotation': data['label']})

    user = data['user']
    hub = data['hub']

    for track in data['tracks']:
        trackTxn = db.getTxn(parent=txn)
        labelDb = db.Labels(user, hub, track, data['ref'])
        db.Prediction('changes').increment(txn=trackTxn)
        item, labels = labelDb.add(labelToUpdate, txn=trackTxn)
        Models.updateAllModelLabels(data, labels, trackTxn)
        trackTxn.commit()


def getLabels(data, txn=None):
    labels = db.Labels(data['user'], data['hub'], data['track'], data['ref'])
    labelsDf = DeadlockWrap(labels.getInBounds, data['ref'], data['start'], data['end'], txn=txn)
    if len(labelsDf.index) < 1:
        return []

    labelsDf = labelsDf[labelColumns]
    labelsDf.columns = jbrowseLabelColumns

    return labelsDf.to_dict('records')


def stats():
    chroms = labels = 0

    for key in db.Labels.db_key_tuples():
        labelsDf = db.Labels(*key).get()

        if labelsDf.empty:
            continue

        chroms = chroms + 1

        labels = labels + len(labelsDf.index)

    return chroms, labels
