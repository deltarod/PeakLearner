import os
import time
import threading
import pandas as pd
import requests

server = 'http://localhost:8080'
user = 1
hub = 'TestHub'
track = 'aorta_ENCFF115HTK'
hubURL = '%s/%s/%s/' % (server, user, hub)
trackURL = '%s%s/' % (hubURL, track)
trackInfoUrl = '%sinfo/' % trackURL
labelURL = '%slabels/' % trackURL
modelsUrl = '%smodels/' % trackURL
jobsURL = '%s/jobs/' % server
problem = {'chrom': 'chr1', 'chromStart': 13607162, 'chromEnd': 17125658}

syncro = threading.Event()





# Add a number of labels
def addLabelThread():
    df = pd.read_csv('tests/data/labels.csv', sep='\t')
    records = df.to_dict('records')
    syncro.wait()
    for label in records:
        query = {'command': 'add', 'args': label}
        requests.post(labelURL, json=query, timeout=10)
        print('after label send')


def putModelThread():

    models = {}
    dir = 'tests/data/'
    for filename in os.listdir(dir):
        if '.bed' in filename:
            first = filename.replace('_segments.bed', '')
            penalty = first.replace('coverage.bedGraph_penalty=', '')
            df = pd.read_csv(dir + filename, sep='\t', header=None)
            df.columns = ['chrom', 'start', 'end', 'annotation', 'mean']
            models[penalty] = df.sort_values('start', ignore_index=True)

    syncro.wait()

    for penalty in models.keys():
        model = models[penalty]


        modelInfo = {'user': user,
                     'hub': hub,
                     'track': track,
                     'problem': problem}

        query = {'command': 'put',
                 'args': {'modelInfo': modelInfo, 'penalty': penalty, 'modelData': model.to_json()}}

        r = requests.post(modelsUrl, json=query, timeout=10)

        print('after send')
        if r.status_code != 200:
            raise Exception

syncro.clear()
labelThread = threading.Thread(target=addLabelThread)
modelThread = threading.Thread(target=putModelThread)

labelThread.start()
modelThread.start()

time.sleep(1)

syncro.set()

modelThread.join()
labelThread.join()

