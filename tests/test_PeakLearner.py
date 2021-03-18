import os
import unittest
import time
from pyramid import testing
from api.util import PLConfig as cfg

cfg.testing()
sleepTime = 600


class PeakLearnerTests(unittest.TestCase):
    user = 1
    hub = 'TestHub'
    track = 'aorta_ENCFF115HTK'
    hubURL = '/%s/%s/' % (user, hub)
    trackURL = '%s%s/' % (hubURL, track)
    trackInfoUrl = '%sinfo/' % trackURL
    labelURL = '%slabels/' % trackURL
    modelsUrl = '%smodels/' % trackURL
    jobsURL = '/jobs/'
    rangeArgs = {'ref': 'chr1', 'start': 0, 'end': 120000000, 'label': 'peakStart'}
    startLabel = rangeArgs.copy()
    startLabel['start'] = 15250059
    startLabel['end'] = 15251519
    endLabel = startLabel.copy()
    endLabel['start'] = 15251599
    endLabel['end'] = 15252959
    endLabel['label'] = 'peakEnd'
    noPeakLabel = startLabel.copy()
    noPeakLabel['start'] = 16089959
    noPeakLabel['end'] = 16091959
    noPeakLabel['label'] = 'noPeak'

    def setUp(self):
        self.config = testing.setUp()
        from PeakLearner import main
        app = main({})
        from webtest import TestApp

        self.testapp = TestApp(app)

    def test_serverWorking(self):
        res = self.testapp.get('/')
        assert res.status_code == 200

    def test_addHub(self):
        query = {'command': 'parseHub',
                 'args': {'hubUrl': 'https://rcdata.nau.edu/genomic-ml/PeakLearner/testHub/hub.txt'}}

        request = self.testapp.post_json('/uploadHubUrl/', query)

        assert request.status_code == 200

        assert request.json == '/1/TestHub/'

        dataPath = os.path.join(cfg.jbrowsePath, cfg.dataPath)

        assert os.path.exists(dataPath)

        hg19path = os.path.join(dataPath, 'genomes', 'hg19')

        assert os.path.exists(hg19path)

        problemsTrackList = os.path.join(hg19path, 'problems', 'trackList.json')

        assert os.path.exists(problemsTrackList)

    def test_getHubInfo(self):
        expectedTrackKeys = ['aorta_ENCFF115HTK', 'aorta_ENCFF502AXL']
        hubInfoURL = '%sinfo/' % self.hubURL
        request = self.testapp.get(hubInfoURL)

        assert request.status_code == 200

        requestOutput = request.json

        assert requestOutput['genome'] == 'hg19'

        assert len(requestOutput['tracks']) == 2
        for trackKey in requestOutput['tracks']:
            assert trackKey in expectedTrackKeys

    def test_getTrackInfo(self):
        expectedUrl = 'https://rcdata.nau.edu/genomic-ml/PeakSegFPOP/labels/H3K4me3_TDH_ENCODE/samples/aorta' \
                      '/ENCFF115HTK/coverage.bigWig'

        request = self.testapp.get(self.trackInfoUrl)

        assert request.status_code == 200

        requestOutput = request.json

        assert requestOutput['key'] == self.track

        assert requestOutput['url'] == expectedUrl

    def test_labels(self):
        query = {'command': 'getAll', 'args': {}}

        request = self.testapp.post_json(self.jobsURL, query)

        numJobsBefore = len(request.json)

        # Blank Label Test
        query = {'command': 'get', 'args': self.rangeArgs}
        request = self.testapp.post_json(self.labelURL, query)

        numLabelsBefore = len(request.json)

        # Add label
        query = {'command': 'add', 'args': self.startLabel}
        request = self.testapp.post_json(self.labelURL, query)

        query = {'command': 'get', 'args': self.rangeArgs}
        request = self.testapp.post_json(self.labelURL, query)

        assert request.status_code == 200

        assert len(request.json) == numLabelsBefore + 1

        numLabelsBefore = len(request.json)

        serverLabel = request.json[0]

        assert serverLabel['ref'] == self.startLabel['ref']
        assert serverLabel['start'] == self.startLabel['start']
        assert serverLabel['end'] == self.startLabel['end']
        assert serverLabel['label'] == 'peakStart'

        query = {'command': 'get', 'args': self.rangeArgs}
        request = self.testapp.post_json(self.labelURL, query)

        assert request.status_code == 200

        serverLabel = request.json[0]

        assert serverLabel['label'] == 'peakStart'

        query = {'command': 'getAll', 'args': {}}

        request = self.testapp.post_json(self.jobsURL, query)

        assert request.status_code == 200

        assert len(request.json) == numJobsBefore + 1

        numJobsBefore = len(request.json)

        # Try adding another label
        query = {'command': 'add', 'args': self.endLabel}

        request = self.testapp.post_json(self.labelURL, query)

        assert request.status_code == 200

        query = {'command': 'get', 'args': self.rangeArgs}
        request = self.testapp.post_json(self.labelURL, query)

        assert request.status_code == 200

        assert len(request.json) == numLabelsBefore + 1

        # Update second label
        updateAnother = self.endLabel.copy()
        updateAnother['label'] = 'peakEnd'
        query = {'command': 'update', 'args': updateAnother}

        request = self.testapp.post_json(self.labelURL, query)

        assert request.status_code == 200

        query = {'command': 'getAll', 'args': {}}

        request = self.testapp.post_json(self.jobsURL, query)

        # Check that system doesn't create duplicate jobs
        assert len(request.json) == numJobsBefore

        query = {'command': 'get', 'args': self.rangeArgs}
        request = self.testapp.post_json(self.labelURL, query)

        assert request.status_code == 200

        assert len(request.json) == numLabelsBefore + 1

        numLabelsBefore = len(request.json)

        # Remove Labels
        for label in request.json:
            query = {'command': 'remove', 'args': label}
            request = self.testapp.post_json(self.labelURL, query)

            assert request.status_code == 200

        query = {'command': 'get', 'args': self.rangeArgs}
        request = self.testapp.post_json(self.labelURL, query)

        assert len(request.json) == 0

    # Tests are ran in alphabetical order?
    def test_zbackupSystem(self):

        # add label to backup
        query = {'command': 'add', 'args': self.endLabel}

        request = self.testapp.post_json(self.labelURL, query)

        assert request.status_code == 200

        backupUrl = '/doBackup/'
        request = self.testapp.get(backupUrl)
        assert request.status_code == 200

        assert not isinstance(bool, type(request.json))

        backupFolder = os.path.join(cfg.backupPath, request.json)

        assert os.path.exists(backupFolder)

    def test_zrestoreSystem(self):
        restoreUrl = '/doRestore/'

        # Make change that can be checked after restore
        updateAnother = self.endLabel.copy()
        updateAnother['label'] = 'peakStart'
        query = {'command': 'update', 'args': updateAnother}
        request = self.testapp.post_json(self.labelURL, query)

        assert request.status_code == 200

        query = {'command': 'get', 'args': self.rangeArgs}
        request = self.testapp.post_json(self.labelURL, query)

        assert request.status_code == 200

        labels = request.json

        assert len(labels) == 1

        assert labels[0]['label'] == 'peakStart'

        request = self.testapp.get(restoreUrl)
        assert request.status_code == 200

        query = {'command': 'get', 'args': self.rangeArgs}
        request = self.testapp.post_json(self.labelURL, query)

        assert request.status_code == 200

        labels = request.json

        assert len(labels) == 1

        assert labels[0]['label'] == 'peakEnd'

        query = {'command': 'remove', 'args': updateAnother}
        request = self.testapp.post_json(self.labelURL, query)
        assert request.status_code == 200


    # TODO: Fix test
    def dont_test_models(self):
        # Add initial label
        query = {'command': 'add', 'args': self.startLabel}
        request = self.testapp.post_json(self.labelURL, query)
        assert request.status_code == 200

        # Update Label to create job
        updateLabel = self.startLabel.copy()
        updateLabel['label'] = 'peakStart'
        query = {'command': 'update', 'args': updateLabel}
        request = self.testapp.post_json(self.labelURL, query)
        assert request.status_code == 200

        query = {'command': 'getAll', 'args': {}}
        request = self.testapp.post_json(self.jobsURL, query)
        jobs = request.json
        assert len(jobs) == 1

        query = {'command': 'getProblems', 'args': self.startLabel}
        request = self.testapp.post_json(self.trackInfoUrl, query)
        assert request.status_code == 200

        problems = request.json

        assert len(problems) == 1

        problem = problems[0]

        job = jobs[0]

        numModels = job['numModels']

        # started = slurmrun.startAllNewJobs()

        # assert started

        startTime = time.time()

        problemSum = self.checkModelSumLoop(self.startLabel, startTime, problem, numModels)

        expected = [{'errors': 1.0, 'fn': 0.0, 'fp': 1.0, 'numPeaks': 262.0, 'penalty': '1000', 'possible_fn': 1.0,
                     'possible_fp': 1.0, 'regions': 1.0},
                    {'errors': 0.0, 'fn': 0.0, 'fp': 0.0, 'numPeaks': 48.0, 'penalty': '10000', 'possible_fn': 1.0,
                     'possible_fp': 1.0, 'regions': 1.0},
                    {'errors': 1.0, 'fn': 1.0, 'fp': 0.0, 'numPeaks': 18.0, 'penalty': '100000', 'possible_fn': 1.0,
                     'possible_fp': 1.0, 'regions': 1.0},
                    {'errors': 0.0, 'fn': 0.0, 'fp': 0.0, 'numPeaks': 0.0, 'penalty': '1000000', 'possible_fn': 0.0,
                     'possible_fp': 0.0, 'regions': 0.0}]

        assert problemSum == expected

        # Add label with no update
        query = {'command': 'add', 'args': self.endLabel}
        request = self.testapp.post_json(self.labelURL, query)
        assert request.status_code == 200

        updateLabel = self.endLabel.copy()
        updateLabel['label'] = 'peakEnd'
        query = {'command': 'update', 'args': updateLabel}
        request = self.testapp.post_json(self.labelURL, query)
        assert request.status_code == 200

        query = {'command': 'getAll', 'args': {}}
        request = self.testapp.post_json(self.jobsURL, query)

        assert request.json is None

        query = {'command': 'getModelSummary', 'args': self.startLabel}
        request = self.testapp.post_json(self.modelsUrl, query)
        assert request.status_code == 200

        sums = request.json

        contig = sums[str(problem['chromStart'])]

        expected = [{'regions': 2, 'fp': 2, 'possible_fp': 2, 'fn': 0, 'possible_fn': 2, 'errors': 2, 'penalty': '1000',
                     'numPeaks': 262},
                    {'regions': 2, 'fp': 0, 'possible_fp': 2, 'fn': 0, 'possible_fn': 2, 'errors': 0,
                     'penalty': '10000', 'numPeaks': 48},
                    {'regions': 2, 'fp': 0, 'possible_fp': 2, 'fn': 2, 'possible_fn': 2, 'errors': 2,
                     'penalty': '100000', 'numPeaks': 18},
                    {'regions': 0, 'fp': 0, 'possible_fp': 0, 'fn': 0, 'possible_fn': 0, 'errors': 0,
                     'penalty': '1000000', 'numPeaks': 0}]

        assert contig == expected

        # Add Label with grid search
        query = {'command': 'add', 'args': self.noPeakLabel}
        request = self.testapp.post_json(self.labelURL, query)
        assert request.status_code == 200

        updateLabel = self.noPeakLabel.copy()
        updateLabel['label'] = 'noPeak'
        query = {'command': 'update', 'args': updateLabel}
        request = self.testapp.post_json(self.labelURL, query)
        assert request.status_code == 200

        query = {'command': 'getAll', 'args': {}}
        request = self.testapp.post_json(self.jobsURL, query)
        assert request.status_code == 200

        jobs = request.json

        assert len(jobs) == 1

        job = jobs[0]

        numModels += job['numModels']

        # started = slurmrun.startAllNewJobs()

        # assert started

        startTime = time.time()

        gridContig = self.checkModelSumLoop(self.startLabel, startTime, problem, numModels)

        expected = [{'regions': 3, 'fp': 3, 'possible_fp': 3, 'fn': 0, 'possible_fn': 2, 'errors': 3, 'penalty': '1000',
                     'numPeaks': 262},
                    {'regions': 3, 'fp': 1, 'possible_fp': 3, 'fn': 0, 'possible_fn': 2, 'errors': 1,
                     'penalty': '10000', 'numPeaks': 48},
                    {'regions': 3, 'fp': 0, 'possible_fp': 3, 'fn': 0, 'possible_fn': 2, 'errors': 0,
                     'penalty': '18181.818181818184', 'numPeaks': 38},
                    {'regions': 3, 'fp': 0, 'possible_fp': 3, 'fn': 0, 'possible_fn': 2, 'errors': 0,
                     'penalty': '26363.636363636364', 'numPeaks': 33},
                    {'regions': 3, 'fp': 0, 'possible_fp': 3, 'fn': 0, 'possible_fn': 2, 'errors': 0,
                     'penalty': '34545.454545454544', 'numPeaks': 32},
                    {'regions': 3, 'fp': 0, 'possible_fp': 3, 'fn': 0, 'possible_fn': 2, 'errors': 0,
                     'penalty': '42727.27272727273', 'numPeaks': 27},
                    {'regions': 3, 'fp': 0, 'possible_fp': 3, 'fn': 2, 'possible_fn': 2, 'errors': 2,
                     'penalty': '50909.09090909091', 'numPeaks': 22},
                    {'regions': 3, 'fp': 0, 'possible_fp': 3, 'fn': 2, 'possible_fn': 2, 'errors': 2,
                     'penalty': '59090.90909090909', 'numPeaks': 22},
                    {'regions': 3, 'fp': 0, 'possible_fp': 3, 'fn': 2, 'possible_fn': 2, 'errors': 2,
                     'penalty': '67272.72727272726', 'numPeaks': 22},
                    {'regions': 3, 'fp': 0, 'possible_fp': 3, 'fn': 2, 'possible_fn': 2, 'errors': 2,
                     'penalty': '75454.54545454546', 'numPeaks': 22},
                    {'regions': 3, 'fp': 0, 'possible_fp': 3, 'fn': 2, 'possible_fn': 2, 'errors': 2,
                     'penalty': '83636.36363636363', 'numPeaks': 20},
                    {'regions': 3, 'fp': 0, 'possible_fp': 3, 'fn': 2, 'possible_fn': 2, 'errors': 2,
                     'penalty': '91818.18181818182', 'numPeaks': 20},
                    {'regions': 3, 'fp': 0, 'possible_fp': 3, 'fn': 2, 'possible_fn': 2, 'errors': 2,
                     'penalty': '100000', 'numPeaks': 18},
                    {'regions': 0, 'fp': 0, 'possible_fp': 0, 'fn': 0, 'possible_fn': 0, 'errors': 0,
                     'penalty': '1000000', 'numPeaks': 0}]

        assert gridContig == expected

    def checkModelSumLoop(self, label, startTime, problem, numModels):
        while True:
            query = {'command': 'getModelSummary', 'args': label}
            request = self.testapp.post_json(self.modelsUrl, query)

            if not len(request.json) == 0:
                models = request.json
                gridContig = models[str(problem['chromStart'])]
                print('lenGridContig', len(gridContig))
                if len(gridContig) >= numModels:
                    return gridContig

            if (time.time() - startTime) > sleepTime:
                raise Exception

            time.sleep(5)