import PeakError
import LOPART
import pandas as pd
import numpy as np
from glmnet_python import cvglmnetPredict
from api.util import PLConfig as pl, PLdb as db, bigWigUtil as bw
from api.Handlers import Jobs, Tracks, Handler

summaryColumns = ['regions', 'fp', 'possible_fp', 'fn', 'possible_fn', 'errors']
modelColumns = ['chrom', 'chromStart', 'chromEnd', 'annotation', 'height']
jbrowseModelColumns = ["ref", "start", "end", "type", "score"]


class ModelHandler(Handler.TrackHandler):
    """Handles model Commands"""
    key = 'models'

    def do_POST(self, data):
        return self.getCommands()[data['command']](data['args'])

    @classmethod
    def getCommands(cls):
        return {'get': getModels,
                'getModelSummary': getModelSummary,
                'put': putModel}


def getModels(data):
    problems = Tracks.getProblems(data)

    output = []

    for problem in problems:
        modelSummaries = db.ModelSummaries(data['user'], data['hub'], data['track'], problem['chrom'],
                                           problem['chromStart']).get()

        if len(modelSummaries.index) < 1:
            lopartOutput = generateLOPARTModel(data, problem)
            output.extend(lopartOutput)
            continue

        nonZeroRegions = modelSummaries[modelSummaries['regions'] > 0]

        if len(nonZeroRegions.index) < 1:
            lopartOutput = generateLOPARTModel(data, problem)
            output.extend(lopartOutput)
            continue

        withPeaks = nonZeroRegions[nonZeroRegions['numPeaks'] > 0]

        if len(withPeaks.index) < 1:
            lopartOutput = generateLOPARTModel(data, problem)
            output.extend(lopartOutput)
            continue

        noError = withPeaks[withPeaks['errors'] < 1]

        if len(noError.index) < 1:
            lopartOutput = generateLOPARTModel(data, problem)
            output.extend(lopartOutput)
            continue

        elif len(noError.index) > 1:
            # Select which model to display from modelSums with 0 error
            noError = whichModelToDisplay(data, problem, noError)

        penalty = noError['penalty'].iloc[0]

        minErrorModel = db.Model(data['user'], data['hub'], data['track'], problem['chrom'], problem['chromStart'],
                                 penalty)
        model = minErrorModel.getInBounds(data['ref'], data['start'], data['end'])
        onlyPeaks = model[model['annotation'] == 'peak']
        # Organize the columns
        onlyPeaks = onlyPeaks[modelColumns]
        onlyPeaks.columns = jbrowseModelColumns
        output.extend(onlyPeaks.to_dict('records'))
    return output


def whichModelToDisplay(data, problem, summary):
    prediction = doPrediction(data, problem)

    # If no prediction, use traditional system
    if prediction is None or prediction is False:
        return summary[summary['numPeaks'] == summary['numPeaks'].min()]

    logPenalties = np.log10(summary['penalty'].astype(float))

    compared = abs(prediction - logPenalties)

    toDisplay = compared[compared == compared.min()]

    toDisplayIndex = toDisplay.index[0]

    outputDf = pd.DataFrame([summary.iloc[toDisplayIndex]])

    return outputDf


def updateAllModelLabels(data, labels):
    # This is the problems that the label update is in
    txn = db.getTxn()
    problems = Tracks.getProblems(data)

    for problem in problems:
        modelSummaries = db.ModelSummaries(data['user'], data['hub'], data['track'], problem['chrom'],
                                           problem['chromStart'])

        modelsums = modelSummaries.get(txn=txn, write=True)

        if len(modelsums.index) < 1:
            submitPregenJob(problem, data, txn=txn)
            continue

        newSum = modelsums.apply(modelSumLabelUpdate, axis=1, args=(labels, data, problem, txn))

        modelSummaries.put(newSum, txn=txn)

        checkGenerateModels(newSum, problem, data, txn=txn)

    txn.commit()


def modelSumLabelUpdate(modelSum, labels, data, problem, txn):
    model = db.Model(data['user'], data['hub'], data['track'], problem['chrom'],
                     problem['chromStart'], modelSum['penalty']).get(txn=txn)

    return calculateModelLabelError(model, labels, problem, modelSum['penalty'])


def checkGenerateModels(modelSums, problem, data, txn=None):
    nonZeroLabels = modelSums[modelSums['regions'] > 0]

    if len(nonZeroLabels.index) == 0:
        return False

    nonZeroRegions = nonZeroLabels[nonZeroLabels['numPeaks'] > 0]

    if len(nonZeroRegions.index) == 0:
        return False

    minError = nonZeroRegions[nonZeroRegions['errors'] == nonZeroRegions['errors'].min()]

    numMinErrors = len(minError.index)

    if numMinErrors == 0:
        return False

    # If a current model is consistent with the labels
    if minError.iloc[0]['errors'] == 0:
        return False

    if numMinErrors > 1:
        first = minError.iloc[0]
        last = minError.iloc[-1]

        biggerFp = first['fp'] > last['fp']
        smallerFn = first['fn'] < last['fn']

        # Sanity check for bad labels, if the minimum is still the same values
        # With little labels this could not generate new models
        if biggerFp or smallerFn:
            minPenalty = first['penalty']
            maxPenalty = last['penalty']
            submitGridSearch(problem, data, minPenalty, maxPenalty, txn=txn)
            return True
        return False

    elif numMinErrors == 1:

        index = minError.index[0]

        model = minError.iloc[0]
        if model['fp'] > model['fn']:
            try:
                compare = modelSums.iloc[index + 1]
            except IndexError:
                submitOOMJob(problem, data, model['penalty'], '*', txn=txn)
                return True

            # If the next model only has 1 more peak, not worth searching
            if model['numPeaks'] <= compare['numPeaks'] + 1:
                return False
        else:

            try:
                compare = modelSums.iloc[index - 1]
            except IndexError:
                submitOOMJob(problem, data, model['penalty'], '/', txn=txn)
                return True

            # If the previous model is only 1 peak away, not worth searching
            if compare['numPeaks'] + 1 >= model['numPeaks']:
                return False

        if compare['penalty'] > model['penalty']:
            top = compare
            bottom = model
        else:
            top = model
            bottom = compare
        submitSearch(data, problem, bottom, top, txn=txn)

        return True

    submitPregenJob(problem, data, txn=txn)

    return True


def submitOOMJob(problem, data, penalty, jobType, txn=None):
    if jobType == '*':
        penalty = float(penalty) * 10
    elif jobType == '/':
        penalty = float(penalty) / 10
    else:
        print("Invalid OOM Job")
        return

    job = Jobs.SingleModelJob(data['user'],
                              data['hub'],
                              data['track'],
                              problem,
                              penalty)

    job.putNewJobWithTxn(txn=txn)


def submitPregenJob(problem, data, txn=None):
    penalties = getPrePenalties()

    job = Jobs.PregenJob(data['user'],
                         data['hub'],
                         data['track'],
                         problem,
                         penalties)

    print('pregen before put')
    job.putNewJobWithTxn(txn=txn)


def submitGridSearch(problem, data, minPenalty, maxPenalty, num=pl.gridSearchSize, txn=None):
    minPenalty = float(minPenalty)
    maxPenalty = float(maxPenalty)
    penalties = np.linspace(minPenalty, maxPenalty, num + 2).tolist()[1:-1]
    if 'trackUrl' in data:
        job = Jobs.GridSearchJob(data['user'],
                                 data['hub'],
                                 data['track'],
                                 problem,
                                 penalties,
                                 trackUrl=data['trackUrl'])
    else:
        job = Jobs.GridSearchJob(data['user'],
                                 data['hub'],
                                 data['track'],
                                 problem,
                                 penalties)

    job.putNewJobWithTxn(txn=txn)


def submitSearch(data, problem, bottom, top, txn=None):
    bottomLoss = db.Loss(data['user'],
                         data['hub'],
                         data['track'],
                         problem['chrom'],
                         problem['chromStart'],
                         bottom['penalty']).get()

    topLoss = db.Loss(data['user'],
                      data['hub'],
                      data['track'],
                      problem['chrom'],
                      problem['chromStart'],
                      top['penalty']).get()

    penalty = abs((topLoss['meanLoss'] - bottomLoss['meanLoss']) / (bottomLoss['peaks'] - topLoss['peaks']))

    job = Jobs.SingleModelJob(data['user'],
                              data['hub'],
                              data['track'],
                              problem,
                              penalty)

    job.putNewJobWithTxn(txn=txn)


def putModel(data):
    modelData = pd.read_json(data['modelData'])
    modelData.columns = modelColumns
    modelInfo = data['modelInfo']
    problem = modelInfo['problem']
    penalty = data['penalty']
    user = modelInfo['user']
    hub = modelInfo['hub']
    track = modelInfo['track']

    txn = db.getTxn()
    db.Model(user, hub, track, problem['chrom'], problem['chromStart'], penalty).put(modelData, txn=txn)
    labels = db.Labels(user, hub, track, problem['chrom']).get()
    errorSum = calculateModelLabelError(modelData, labels, problem, penalty)
    db.ModelSummaries(user, hub, track, problem['chrom'], problem['chromStart']).add(errorSum, txn=txn)
    txn.commit()

    return modelInfo


def calculateModelLabelError(modelDf, labels, problem, penalty):
    labels = labels[labels['annotation'] != 'unknown']
    peaks = modelDf[modelDf['annotation'] == 'peak']
    numPeaks = len(peaks.index)
    numLabels = len(labels.index)

    if numLabels < 1:
        return getErrorSeries(penalty, numPeaks, numLabels)

    labelsIsInProblem = labels.apply(db.checkInBounds, axis=1,
                                     args=(problem['chrom'], problem['chromStart'], problem['chromEnd']))

    if numPeaks < 1:
        return getErrorSeries(penalty, numPeaks, numLabels)

    labelsInProblem = labels[labelsIsInProblem]

    numLabelsInProblem = len(labelsInProblem.index)

    if numLabelsInProblem < 1:
        return getErrorSeries(penalty, numPeaks, numLabels)

    error = PeakError.error(peaks, labelsInProblem)

    if error is None:
        return getErrorSeries(penalty, numPeaks, numLabels)

    summary = PeakError.summarize(error)
    summary.columns = summaryColumns
    summary['penalty'] = penalty
    summary['numPeaks'] = numPeaks

    singleRow = summary.iloc[0]

    return singleRow


def getModelSummary(data):
    txn = db.getTxn()
    problems = Tracks.getProblems(data, txn=txn)

    output = {}

    for problem in problems:
        # TODO: Replace 1 with user of hub NOT current user
        modelSummaries = db.ModelSummaries(1,
                                           data['hub'],
                                           data['track'],
                                           problem['chrom'],
                                           problem['chromStart']).get(txn=txn)

        if len(modelSummaries.index) < 1:
            continue

        output[problem['chromStart']] = modelSummaries.to_dict('records')
    txn.commit()
    return output


def getErrorSeries(penalty, numPeaks, regions=0):
    return pd.Series({'regions': regions, 'fp': 0, 'possible_fp': 0, 'fn': 0, 'possible_fn': 0,
                      'errors': 0, 'penalty': penalty, 'numPeaks': numPeaks})


def getPrePenalties():
    return [1000, 10000, 100000, 1000000]


# TODO: This could be better (learning a penalty based on PeakSegDisk Models?)
def getLOPARTPenalty(data):
    tempPenalties = {0.005: 2000, 0.02: 5000, 0.1: 10000}
    try:
        return tempPenalties[data['scale']]
    except KeyError:
        return 5000


def generateLOPARTModel(data, problem):
    user = data['user']
    hub = data['hub']
    track = data['track']
    chrom = data['ref']
    blockStart = data['start']
    blockEnd = data['end']
    scale = data['scale']
    hubInfo = db.HubInfo(user, hub).get()
    trackUrl = hubInfo['tracks'][data['track']]['url']
    datapoints = data['width']

    start = max(data['visible']['start'], problem['chromStart'])
    end = min(data['visible']['end'], problem['chromEnd'])

    scaledBins = int(scale * (end - start))

    sumData = bw.bigWigSummary(trackUrl, chrom, start, end, scaledBins)
    if len(sumData) < 1:
        return []

    dbLabels = db.Labels(user, hub, track, chrom)
    labels = dbLabels.getInBounds(chrom, start, end)
    denom = end - start

    if len(labels.index) < 1:
        labelsToUse = pd.DataFrame({'start': [1], 'end': [2], 'change': [-1]})
    else:
        lopartLabels = labels[(labels['annotation'] != 'unknown') & (labels['annotation'] != 'peak')]

        if len(lopartLabels.index) < 1:
            labelsToUse = pd.DataFrame({'start': [1], 'end': [2], 'change': [-1]})
        else:
            labelsToUse = labels.apply(convertLabelsToLopart, axis=1, args=(start, end, denom, scaledBins))

    lopartOut = LOPART.runSlimLOPART(sumData, labelsToUse, getLOPARTPenalty(data))

    if lopartOut.empty:
        return []

    lopartPeaks = lopartToPeaks(lopartOut)

    if lopartPeaks.empty:
        return []

    blockBinStart = round((blockStart - start) * scale)
    blockBinEnd = round((blockEnd - start) * scale)

    isInBounds = lopartPeaks.apply(checkBlockInBounds, axis=1, args=(blockBinStart, blockBinEnd))

    lopartInBlock = lopartPeaks[isInBounds].copy()

    if lopartInBlock.empty:
        return []

    lopartInBlock['ref'] = chrom
    lopartInBlock['type'] = 'lopart'

    return convertLopartOutToJbrowse(lopartInBlock, blockBinStart, blockBinEnd, datapoints)


def lopartToPeaks(lopartOut):
    output = lopartOut.copy()
    output['peak'] = False
    meanHeight = lopartOut['height'].mean()

    prev = None
    for index, row in lopartOut.iterrows():
        if prev is None:
            prev = row
            if row['height'] > meanHeight:
                output['peak'][index] = True
            continue

        if row['height'] > prev['height']:
            output['peak'][index] = True

        prev = row

    peaks = output[output['peak']]

    if peaks.empty:
        return peaks

    return peaks.drop(columns=['peak'])


def checkBlockInBounds(row, start, end):
    # If start in range
    if start <= row['start'] <= end:
        return True

    # If end in range
    if start <= row['end'] <= end:
        return True

    # If wraps around whole range
    if (row['start'] < start) and (row['end'] > end):
        return True

    return False


def convertLopartOutToJbrowse(lopartOut, blockStart, blockEnd, datapoints):
    blockData = lopartOut.apply(lopartToBlock, axis=1, args=(blockStart, blockEnd, datapoints))

    return blockData.to_dict('records')


def lopartToBlock(row, start, end, datapoints):
    output = row.copy()

    outputStart = row['start'] - start
    output['start'] = outputStart

    outputEnd = row['end'] - start
    output['end'] = outputEnd

    output['score'] = output['height']

    return output


def convertLabelsToLopart(row, modelStart, modelEnd, denom, bins):
    scaledStart = round(((row['chromStart'] - modelStart) * bins) / denom)
    scaledEnd = round(((row['chromEnd'] - modelStart) * bins) / denom)

    output = row.copy()
    if scaledStart <= 1:
        scaledStart = 1
    output['start'] = scaledStart
    if scaledEnd > bins:
        scaledEnd = bins
    output['end'] = scaledEnd

    if output['annotation'] == 'peakStart' or output['annotation'] == 'peakEnd':
        output['change'] = 1
    else:
        output['change'] = 0
    return output


def doPrediction(data, problem, txn=None):
    features = db.Features(data['user'], data['hub'], data['track'], problem['chrom'], problem['chromStart']).get()

    if features.empty:
        return False

    model = db.Prediction('model').get()

    if not isinstance(model, dict):
        return False

    colsToDrop = db.Prediction('badCols').get()

    featuresDropped = features.drop(labels=colsToDrop)

    prediction = predictWithFeatures(featuresDropped, model)

    if prediction is None:
        return False
    return prediction


def predictWithFeatures(features, model):
    if not isinstance(features, pd.Series):
        raise Exception(features)

    featuresDf = pd.DataFrame().append(features, ignore_index=True)
    guess = cvglmnetPredict(model, newx=featuresDf, s='lambda_min')[0][0]

    if np.isnan(guess):
        return

    return guess


def numModels():
    return db.Model.length()


def numCorrectModels():
    correct = 0

    for key in db.ModelSummaries.db_key_tuples():
        modelSum = db.ModelSummaries(*key).get()

        if modelSum.empty:
            continue

        zeroErrors = modelSum[modelSum['errors'] < 0]

        correct = correct + len(zeroErrors.index)

    return correct
