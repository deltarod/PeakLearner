import os
import bbi
import sys
import time
import shutil
import requests
import PeakSegDisk
import pandas as pd

try:
    import Slurm.SlurmConfig as cfg
except ModuleNotFoundError:
    import SlurmConfig as cfg

genFeaturesPath = os.path.join('Slurm', 'GenerateFeatures.R')


def model(task, dataPath, coveragePath, trackUrl):
    segmentsPath = '%s_penalty=%f_segments.bed' % (coveragePath, task['penalty'])
    lossPath = '%s_penalty=%f_loss.tsv' % (coveragePath, task['penalty'])

    PeakSegDisk.FPOP_files(coveragePath, segmentsPath, lossPath, str(task['penalty']))

    if os.path.exists(segmentsPath):
        if not sendSegments(segmentsPath, task, trackUrl):
            return False
    else:
        return False

    if os.path.exists(lossPath):
        if not sendLoss(lossPath, task, trackUrl):
            return False
    else:
        return False

    if not cfg.debug:
        os.remove(segmentsPath)
        os.remove(lossPath)

    return True


def feature(task, dataPath, coveragePath, trackUrl):
    command = 'Rscript %s %s' % (genFeaturesPath, dataPath)
    os.system(command)

    featurePath = os.path.join(dataPath, 'features.tsv')

    featureDf = pd.read_csv(featurePath, sep='\t')

    featureQuery = {'command': 'put', 'args': {'data': featureDf.to_dict('records'),
                                               'problem': task['problem']}}

    featureUrl = '%sfeatures/' % trackUrl

    r = requests.post(featureUrl, json=featureQuery)

    if not r.status_code == 200:
        return False

    print("Sent feature for ", task)

    return True


def sendSegments(segmentsFile, task, trackUrl):
    strPenalty = str(task['penalty'])

    modelData = pd.read_csv(segmentsFile, sep='\t', header=None)
    modelData.columns = ['chrom', 'start', 'end', 'annotation', 'mean']
    sortedModel = modelData.sort_values('start', ignore_index=True)

    modelInfo = {'user': task['user'],
                 'hub': task['hub'],
                 'track': task['track'],
                 'problem': task['problem'],
                 'jobId': task['id']}

    modelUrl = '%smodels/' % trackUrl

    query = {'command': 'put',
             'args': {'modelInfo': modelInfo, 'penalty': strPenalty, 'modelData': sortedModel.to_json()}}

    r = requests.post(modelUrl, json=query)

    if r.status_code == 200:
        print('model successfully sent with penalty', strPenalty, 'and with modelInfo:\n', modelInfo, '\n')
        return True

    return False


def sendLoss(lossFile, task, trackUrl):
    strPenalty = str(task['penalty'])
    lossUrl = '%sloss/' % trackUrl

    lossData = pd.read_csv(lossFile, sep='\t', header=None)
    lossData.columns = ['penalty',
                        'segments',
                        'peaks',
                        'totalBases',
                        'bedGraphLines',
                        'meanPenalizedCost',
                        'totalUnpenalizedCost',
                        'numConstraints',
                        'meanIntervals',
                        'maxIntervals']

    lossInfo = {'user': task['user'],
                'hub': task['hub'],
                'track': task['track'],
                'problem': task['problem'],
                'jobId': task['id']}

    query = {'command': 'put',
             'args': {'lossInfo': lossInfo, 'penalty': strPenalty, 'lossData': lossData.to_json()}}

    r = requests.post(lossUrl, json=query)

    if r.status_code == 200:
        print('loss successfully sent with penalty', strPenalty, 'and with lossInfo:\n', lossInfo, '\n')
        return True

    return True


def getCoverageFile(task, dataPath):
    problem = task['problem']

    coveragePath = os.path.join(dataPath, 'coverage.bedGraph')

    coverageUrl = task['trackUrl']

    with bbi.open(coverageUrl) as coverage:
        try:
            coverageInterval = coverage.fetch_intervals(problem['chrom'], problem['chromStart'],
                                                        problem['chromEnd'], iterator=True)
            return fixAndSaveCoverage(coverageInterval, coveragePath, problem)
        except KeyError:
            return


def fixAndSaveCoverage(interval, outputPath, problem):
    output = []

    prevEnd = problem['chromStart']

    for data in interval:
        # If current data's start doesn't have the previous end
        if prevEnd < data[1]:
            # Add zero valued data from prev end to current start
            output.append((problem['chrom'], prevEnd, data[1], 0))

        output.append(data)

        prevEnd = data[2]

    # If end of data doesn't completely go to end of problem
    # I don't think this is strictly necessary
    if prevEnd < problem['chromEnd']:
        # Output[0][0] = the chrom
        output.append((problem['chrom'], prevEnd, problem['chromEnd'], 0))

    output = pd.DataFrame(output)
    output.columns = ['chrom', 'chromStart', 'chromEnd', 'count']

    output.to_csv(outputPath, sep='\t', float_format='%d', header=False, index=False)
    return outputPath


def runTask():
    try:
        query = {'command': 'processNextQueuedTask'}
        r = requests.post(cfg.jobUrl, json=query)
    except requests.exceptions.ConnectionError:
        return False

    if not r.status_code == 200:
        raise Exception(r.status_code)

    task = r.json()

    print(task)

    trackUrl = '%s%s/%s/%s/' % (cfg.remoteServer, task['user'], task['hub'], task['track'])

    dataPath = os.path.join(cfg.dataPath, 'PeakLearner-%s-%s' % (task['id'], task['taskId']))

    if not os.path.exists(dataPath):
        try:
            os.makedirs(dataPath)
        except OSError:
            raise Exception

    startTime = time.time()

    coveragePath = getCoverageFile(task, dataPath)

    funcToRun = getTaskFunc(task)

    status = funcToRun(task, dataPath, coveragePath, trackUrl)

    endTime = time.time()

    totalTime = endTime - startTime

    if not cfg.debug:
        os.remove(coveragePath)
        shutil.rmtree(dataPath)

    if status:
        query = {'command': 'update',
                 'args': {'id': task['id'],
                          'task': {'taskId': task['taskId'],
                                   'status': 'Done',
                                   'totalTime': str(totalTime)}}}
        try:
            r = requests.post(cfg.jobUrl, json=query)
        except requests.exceptions.ConnectionError:
            raise Exception(query)

        if not r.status_code == 200:
            raise Exception(r.status_code)

        return True

    query = {'command': 'update',
             'args': {'id': task['id'],
                      'task': {'taskId': task['taskId'],
                               'status': 'Error',
                               'totalTime': str(totalTime)}}}
    try:
        r = requests.post(cfg.jobUrl, json=query)
    except requests.exceptions.ConnectionError:
        raise Exception(query)

    if not r.status_code == 200:
        raise Exception(r.status_code)


def getTaskFunc(task):
    tasks = {'model': model,
             'feature': feature}

    return tasks[task['type']]


if __name__ == '__main__':
    runTask()
