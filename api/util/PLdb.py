import os
import json
import atexit
import pandas as pd
import datetime
import api.util.PLConfig as cfg
import simpleBDB as db
from api.Handlers import Jobs


dbPath = os.path.join(cfg.jbrowsePath, cfg.dataPath, 'db')

db.createEnvWithDir(dbPath)


def close():
    db.close_db()


atexit.register(close)


def getTxn():
    return db.getEnvTxn()


class Model(db.PandasDf):
    keys = ("user", "hub", "track", "chrom", "problemstart", "penalty")

    def getInBounds(self, chrom, start, end):
        model = self.get()
        isInBounds = model.apply(checkInBounds, axis=1, args=(chrom, start, end))

        return model[isInBounds]
    pass


class Loss(db.Resource):
    keys = ("user", "hub", "track", "chrom", "problemstart", "penalty")

    pass


# TODO: Add chrom as key, would lead to speed increases
class Problems(db.PandasDf):
    keys = ("Genome",)

    def getInBounds(self, chrom, start, end):
        problems = self.get()

        if problems is None:
            return None

        isInBounds = problems.apply(checkInBounds, axis=1, args=(chrom, start, end))

        return problems[isInBounds]

    def make_details(self):
        return None
    pass


class JobInfo(db.Resource):
    keys = ("stat",)

    def make_details(self):
        return 0

    def incrementId(self, txn=None):
        current = self.get(txn=txn, write=True)

        incremented = current + 1

        self.put(incremented, txn=txn)

        return current

    pass


class Job(db.Resource):
    keys = ("ID",)

    def make_details(self):
        return {}

    def get(self, txn=None, write=False):
        storable = db.Resource.get(self, txn=txn, write=write)
        return Jobs.Job.fromStorable(storable)

    def put(self, value, txn=None):
        if value is not None:
            if not isinstance(value, dict):
                value = value.__dict__()
        db.Resource.put(self, value, txn=txn)
    pass


class Labels(db.PandasDf):
    keys = ("user", "hub", "track", "chrom")

    def conditional(self, item, df):
        startSame = item['chromStart'] == df['chromStart']

        endSame = item['chromEnd'] == df['chromEnd']

        return startSame & endSame

    def sortDf(self, df):
        df['floatStart'] = df['chromStart'].astype(float)

        df = df.sort_values('floatStart', ignore_index=True)

        return df.drop(columns='floatStart')

    def getInBounds(self, chrom, start, end, txn=None):
        labels = self.get(txn=txn)

        if labels is None:
            return None
        if len(labels.index) < 1:
            return labels

        isInBounds = labels.apply(checkInBounds, axis=1, args=(chrom, start, end))

        return labels[isInBounds]

    pass


def checkInBounds(row, chrom, chromStart, chromEnd):
    try:
        if not chrom == row['chrom']:
            return False
    except KeyError:
        print('row count', row.count)
        print("CheckInBoundsKeyError\nRow\n", row, '\n chrom', chrom, 'start', chromStart, 'end', chromEnd)
        return False

    if chromStart <= row['chromStart'] <= chromEnd:
        return True
    elif chromStart <= row['chromEnd'] <= chromEnd:
        return True
    else:
        return (row['chromStart'] < chromEnd) and (row['chromEnd'] > chromEnd)


def checkLabelExists(row, dfToCheck):
    duplicate = dfToCheck['chromStart'] == row['chromStart']
    return duplicate.any()


def updateLabelInDf(row, item):
    if row['chromStart'] == item['chromStart']:
        return item
    return row


class ModelSummaries(db.PandasDf):
    keys = ("user", "hub", "track", "chrom", "problemstart")

    def conditional(self, item, df):
        return item['penalty'] == df['penalty']

    def sortDf(self, df):
        df['floatPenalty'] = df['penalty'].astype(float)

        df = df.sort_values('floatPenalty', ignore_index=True)

        return df.drop(columns='floatPenalty')

    def fileToStorable(self, filePath):
        df = pd.read_csv(filePath, sep='\t', dtype={'penalty': str})
        return df

    pass


class Features(db.Resource):
    keys = ("user", "hub", "track", "chrom", "chromStart")

    def make_details(self):
        return {}

    def convert(self, value, *args):
        return db.Resource.convert(self, value[0])
    
    def saveToFile(self, filePath, *args):
        value = self.get()
        if isinstance(value, pd.Series):
            valueToWrite = value
        elif isinstance(value, dict):
            if len(value) < 1:
                return True
            else:
                print(value)
                raise Exception
        elif isinstance(value, list):
            if not len(value) == 1:
                print(value)
                raise Exception

            valueToWrite = pd.Series(value[0])
        else:
            print(value)
            raise Exception

        # Load the series value into a df so pandas knows how to write it right
        pd.DataFrame().append(valueToWrite, ignore_index=True).to_csv(filePath, sep='\t')
        return True

    def fileToStorable(self, filePath):
        return pd.read_csv(filePath, sep='\t', squeeze=True).loc[0]

    pass


def updateSummaryInDf(row, item):
    if row['penalty'] == item['penalty']:
        return item
    return row


class HubInfo(db.Resource):
    keys = ("User", "Hub")

    def make_details(self):
        return None

    pass


class Prediction(db.Resource):
    keys = ("Key",)

    def make_details(self):
        return 0

    def increment(self, txn=None):
        current = self.get(txn=txn, write=True)

        incremented = current + 1

        self.put(incremented, txn=txn)

        return current

    pass


class Iteration(db.Resource):
    keys = ("user", "hub", "track", "chrom", "chromStart")

    def make_details(self):
        return 0

    def increment(self, txn=None):
        current = self.get(txn=txn, write=True)

        incremented = current + 1

        self.put(incremented, txn=txn)

        return current


backup_restore = [HubInfo, Features, ModelSummaries, Labels, Problems, Model]


def getLastBackup():
    if not os.path.exists(cfg.backupPath):
        return

    last_backup = None
    firstCheck = True

    for backup in os.listdir(cfg.backupPath):
        backupTime = datetime.datetime.strptime(backup, '%Y-%m-%d %H:%M:%S.%f')
        if firstCheck:
            firstCheck = False
            last_backup = backupTime
            continue

        if backupTime > last_backup:
            last_backup = backupTime

    return last_backup


def getAvailableBackups():
    if not os.path.exists(cfg.backupPath):
        return []

    return os.listdir(cfg.backupPath)


# TODO: Maybe make it so users can only backup/restore what they have permission to do?
def doBackup(*args):
    if not os.path.exists(cfg.backupPath):
        try:
            os.makedirs(cfg.backupPath)
        except OSError:
            return False

    backupTime = str(datetime.datetime.now())

    currentBackupPath = os.path.join(cfg.backupPath, backupTime)

    if not os.path.exists(currentBackupPath):
        try:
            os.makedirs(currentBackupPath)
        except OSError:
            return False
    for backup in backup_restore:
        backup.doBackup(currentBackupPath, args)

    return backupTime


def doRestore():
    if not os.path.exists(cfg.backupPath):
        return False

    lastBackup = getLastBackup()

    return doRestoreWithSelected(str(lastBackup))


def doRestoreWithSelected(backup):
    backupPath = os.path.join(cfg.backupPath, backup)

    for restore in backup_restore:
        restore.doRestore(backupPath)

    return backup




