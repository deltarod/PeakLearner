import pandas as pd
from api.util import PLdb as db
from api.Handlers.Handler import Handler
from api.Handlers import Models
import logging

log = logging.getLogger(__name__)

statuses = ['New', 'Queued', 'Processing', 'Done', 'Error']


class JobHandler(Handler):
    """Handles Job Commands"""

    def do_POST(self, data):
        funcToRun = self.getCommands()[data['command']]
        args = {}
        if 'args' in data:
            args = data['args']

        return funcToRun(args)

    @classmethod
    def getCommands(cls):
        # TODO: Add update/delete/info
        return {'get': getJob,
                'getAll': getAllJobs,
                'update': updateTask,
                'resetJob': resetJob,
                'resetAll': resetAllJobs,
                'restartJob': restartJob,
                'restartAllJobs': restartAllJobs,
                'queueNextTask': queueNextTask,
                'processNextQueuedTask': processNextQueuedTask,
                'check': checkNewTask,
                'dlJobs': addDownloadJobs}


class JobType(type):
    """Just a simple way to keep track of different job types without needing to do it manually"""
    jobTypes = {}

    def __init__(cls, name, bases, dct):
        """Adds everything but the base job type to the jobTypes list"""
        if hasattr(cls, 'jobType'):
            cls.jobTypes[cls.jobType] = cls

    def fromStorable(cls, storable):
        """Create a job type from a storable object

        These storables are dicts which are stored using the Job type in api.util.PLdb
        """

        if 'jobType' not in storable:
            return None
        storableClass = cls.jobTypes[storable['jobType']]
        return storableClass.withStorable(storable)


class Job(metaclass=JobType):
    """Class for storing job information"""

    jobType = 'job'

    def __init__(self, user, hub, track, problem, trackUrl=None, tasks=None):
        self.user = user
        self.hub = hub
        self.track = track
        self.problem = problem

        if tasks is None:
            self.tasks = {}
        else:
            self.tasks = tasks

        # When adding labels during hub upload the URL hasn't been stored yet
        if trackUrl is None:
            txn = db.getTxn()
            hubInfo = db.HubInfo(user, hub).get(txn=txn)
            try:
                self.trackUrl = hubInfo['tracks'][track]['url']
            except TypeError:
                log.debug(hubInfo)
                log.debug(db.HubInfo.db_key_tuples())
                log.debug(user, track)
                txn.commit()
                raise Exception
            txn.commit()
        else:
            self.trackUrl = trackUrl
        self.status = 'New'

    @classmethod
    def withStorable(cls, storable):
        """Creates new job using the storable object"""
        # https://stackoverflow.com/questions/2168964/how-to-create-a-class-instance-without-calling-initializer
        self = cls.__new__(cls)
        self.user = storable['user']
        self.hub = storable['hub']
        self.track = storable['track']
        self.problem = storable['problem']
        self.trackUrl = storable['trackUrl']
        self.status = storable['status']
        self.tasks = storable['tasks']
        self.id = storable['id']
        self.iteration = storable['iteration']
        return self

    def putNewJob(self, checkExists=True):
        txn = db.getTxn()
        value = self.putNewJobWithTxn(txn, checkExists=checkExists)
        txn.commit()
        return value

    def putNewJobWithTxn(self, txn, checkExists=True):
        """puts Job into job list if the job doesn't exist"""
        if checkExists:
            if self.checkIfExists():
                return

        self.id = str(db.JobInfo('Id').incrementId(txn=txn))
        self.iteration = db.Iteration(self.user,
                                      self.hub,
                                      self.track,
                                      self.problem['chrom'],
                                      self.problem['chromStart']).increment(txn=txn)

        self.putWithDb(db.Job(self.id), txn=txn)

        return self.id

    def checkIfExists(self):
        """Check if the current job exists in the DB"""
        currentJobs = db.Job.all()
        for job in currentJobs:
            if self.equals(job):
                return True
        return False

    def equals(self, jobToCheck):
        """Check if current job is equal to the job to check"""
        if self.user != jobToCheck.user:
            return False

        if self.hub != jobToCheck.hub:
            return False

        if self.track != jobToCheck.track:
            return False

        if self.problem != jobToCheck.problem:
            return False

        if self.jobType != jobToCheck.jobType:
            return False

        return True

    # TODO: Time prediction for job
    def getNextNewTask(self):
        """Get's the next task which is new"""
        for key in self.tasks.keys():
            task = self.tasks[key]
            if task['status'].lower() == 'new':
                task.status = 'Queued'

    def updateTask(self, task):
        """Updates a task given a dict with keys to put/update"""
        taskToUpdate = None
        for key in self.tasks.keys():
            taskToUpdate = self.tasks[key]
            updateId = int(taskToUpdate['taskId'])
            taskId = int(task['taskId'])
            if updateId == taskId:
                break

        if taskToUpdate is None:
            raise Exception

        for key in task.keys():
            taskToUpdate[key] = task[key]

        self.updateJobStatus()

        return taskToUpdate

    def updateJobStatus(self):
        """Update current job status to the min of the task statuses"""
        # A status outside the bounds
        minStatusVal = len(statuses)
        minStatus = self.status
        for key in self.tasks.keys():
            taskToCheck = self.tasks[key]
            status = taskToCheck['status']
            if status == 'Error':
                self.status = status
                return
            statusVal = statuses.index(status)
            if statusVal < minStatusVal:
                minStatusVal = statusVal
                minStatus = status

        self.status = minStatus

        if self.status.lower() == 'done':
            times = []
            for task in self.tasks.values():
                times.append(float(task['totalTime']))

            self.time = str(sum(times))

    def getPriority(self):
        """Eventually will calculate the priority of the job"""

        # Priority based on number of labels
        labelsDb = db.Labels(self.user, self.hub, self.track, self.problem['chrom'])
        inProblem = labelsDb.getInBounds(self.problem['chrom'], self.problem['chromStart'], self.problem['chromEnd'])
        # Add one so the priority is always above predict jobs
        return len(inProblem.index) + 1

    def resetJob(self):
        self.status = 'New'

        for key in self.tasks.keys():
            task = self.tasks[key]
            task['status'] = 'New'

    def restartUnfinished(self):
        restarted = False
        for task in self.tasks.values():
            if task['status'].lower() != 'done':
                task['status'] = 'New'
                restarted = True

        self.updateJobStatus()
        return restarted

    def numTasks(self):
        return len(self.tasks.keys())

    def __dict__(self):
        output = {'user': self.user,
                  'hub': self.hub,
                  'track': self.track,
                  'problem': self.problem,
                  'jobType': self.jobType,
                  'status': self.status,
                  'id': self.id,
                  'iteration': self.iteration,
                  'tasks': self.tasks,
                  'trackUrl': self.trackUrl}

        if self.status.lower() == 'done':
            try:
                output['time'] = self.time
            except AttributeError:
                pass

        return output

    def putWithDb(self, jobDb, txn=None):
        toStore = self.__dict__()

        jobDb.put(toStore, txn=txn)

    def addJobInfoOnTask(self, task):
        task['user'] = self.user
        task['hub'] = self.hub
        task['track'] = self.track
        task['problem'] = self.problem
        task['iteration'] = self.iteration
        task['jobStatus'] = self.status
        task['id'] = self.id
        task['trackUrl'] = self.trackUrl

        return task


def createModelTask(taskId, penalty):
    output = {
        'status': 'New',
        'type': 'model',
        'taskId': str(taskId),
        'penalty': str(penalty)
    }

    return output


def createFeatureTask(taskId):
    output = {
        'status': 'New',
        'taskId': str(taskId),
        'type': 'feature'
    }

    return output


class PredictJob(Job):
    jobType = 'predict'

    def __init__(self, user, hub, track, problem):
        super().__init__(user, hub, track, problem, tasks={'0': createFeatureTask(0)})

    def updateJobStatus(self):
        keys = self.tasks.keys()

        # When initially created, these only have the predict job
        if len(keys) == 1:
            prediction = Models.doPrediction(self.__dict__(), self.problem)

            newTask = {'type': 'model'}

            # If no prediction, Set job as error
            if prediction is None or prediction is False:
                newTask['status'] = 'Error'
                newTask['penalty'] = 'Unknown'
            else:
                newTask['status'] = 'New'
                newTask['penalty'] = str(prediction)

            self.tasks['1'] = newTask

        Job.updateJobStatus(self)

    def getPriority(self):
        # Predict jobs should be low priority as they should only be ran when no other types of jobs are running
        return 0


class SingleModelJob(Job):
    jobType = 'model'

    def __init__(self, user, hub, track, problem, penalty):
        super().__init__(user, hub, track, problem)
        taskId = str(len(self.tasks.keys()))
        log.debug('Single Model Job created', penalty, type(penalty))
        self.tasks[taskId] = createModelTask(taskId, penalty)


class GridSearchJob(Job):
    """"Job type for performing a gridSearch on a region"""
    jobType = 'gridSearch'

    def __init__(self, user, hub, track, problem, penalties, trackUrl=None, tasks=None):
        if tasks is None:
            tasks = {}
        for penalty in penalties:
            taskId = str(len(tasks.keys()))
            tasks[taskId] = createModelTask(taskId, penalty)
        super().__init__(user, hub, track, problem, trackUrl=trackUrl, tasks=tasks)

    def equals(self, jobToCheck):
        if not Job.equals(self, jobToCheck):
            return False

        # The penalties are the keys
        if jobToCheck.tasks == self.tasks:
            return True

        return False


class PregenJob(GridSearchJob):
    """Grid Search but generate a feature vec"""
    jobType = 'pregen'

    def __init__(self, user, hub, track, problem, penalties, trackUrl=None, tasks=None):
        if tasks is None:
            tasks = {}

        tasks['0'] = createFeatureTask(0)

        super().__init__(user, hub, track, problem, penalties, trackUrl=trackUrl, tasks=tasks)


def updateTask(data):
    """Updates a task given the job/task id and stuff to update it with"""
    jobId = data['id']
    task = data['task']
    txn = db.getTxn()
    jobDb = db.Job(jobId)
    jobToUpdate = jobDb.get(txn=txn, write=True)
    task = jobToUpdate.updateTask(task)
    jobDb.put(jobToUpdate, txn=txn)
    txn.commit()

    task = jobToUpdate.addJobInfoOnTask(task)

    if jobToUpdate.status.lower() == 'done':
        checkForMoreJobs(task)
        if checkIfRunDownloadJobs():
            addDownloadJobs()
    return task


def addDownloadJobs(*args):
    # Check tracks for where a prediction needs to be made
    currentProblems = None

    for keys in db.HubInfo.db_key_tuples():
        user, hub = keys
        hubInfoDb = db.HubInfo(*keys)
        txn = db.getTxn()
        hubInfo = hubInfoDb.get(txn=txn, write=True)

        if 'complete' in hubInfo:
            txn.commit()
            continue

        problems = db.Problems(hubInfo['genome']).get()

        currentTrackProblems = pd.DataFrame()

        for track, value in hubInfo['tracks'].items():

            output = problems.apply(checkForModels, axis=1, args=(user, hub, track))

            # If there is a model for every region, don't consider this for download jobs
            if output['models'].all():
                continue

            output['track'] = track

            currentTrackProblems = currentTrackProblems.append(output, ignore_index=True)

        if currentTrackProblems.empty:
            txn.commit()
            continue

        noModels = currentTrackProblems[currentTrackProblems['models'] == False]

        # If no current problems, mark track as complete so it doesn't have to search the problems
        if len(noModels.index) == 0:
            hubInfo['complete'] = True
            hubInfoDb.put(hubInfo, txn=txn)
            txn.commit()
            continue

        txn.commit()

        numLabels = currentTrackProblems['numLabels'].sum()

        hubProblemsInfo = {'user': user,
                           'hub': hub,
                           'numLabels': numLabels,
                           'problems': currentTrackProblems}

        if currentProblems is None:
            currentProblems = hubProblemsInfo
            continue

        # Run download job on hub with most labels
        else:
            if currentProblems['numLabels'] < hubProblemsInfo['numLabels']:
                currentProblems = hubProblemsInfo

    if currentProblems is not None:
        submitDownloadJobs(currentProblems)


def submitDownloadJobs(problems):
    currentProblems = problems['problems']

    toCreateJobs = currentProblems[currentProblems['models'] == False]

    toCreateJobs.apply(submitPredictJobForDownload, axis=1, args=(problems['user'], problems['hub']))


def submitPredictJobForDownload(row, user, hub):
    problem = {'chrom': row['chrom'],
               'chromStart': row['chromStart'],
               'chromEnd': row['chromEnd']}

    track = row['track']

    job = PredictJob(user, hub, track, problem)

    job.putNewJob()


def checkForModels(row, user, hub, track):
    chrom = row['chrom']
    start = row['chromStart']
    end = row['chromEnd']

    ms = db.ModelSummaries(user, hub, track, chrom, start).get()

    row['models'] = not ms.empty

    labels = db.Labels(user, hub, track, chrom).getInBounds(chrom, start, end)

    row['numLabels'] = len(labels.index)

    return row


def checkForMoreJobs(task):
    txn = db.getTxn()
    problem = task['problem']
    modelSums = db.ModelSummaries(task['user'],
                                  task['hub'],
                                  task['track'],
                                  problem['chrom'],
                                  problem['chromStart']).get(txn=txn, write=True)
    Models.checkGenerateModels(modelSums, problem, task, txn=txn)
    txn.commit()


def checkIfRunDownloadJobs():
    for keys in db.Job.db_key_tuples():
        jobDb = db.Job(*keys)
        currentJob = jobDb.get()

        if currentJob.status.lower() != 'done':
            return False
    return True


def resetJob(data):
    """resets a job to a new state"""
    jobId = data['jobId']
    txn = db.getTxn()
    jobDb = db.Job(jobId)
    jobToReset = jobDb.get(txn=txn, write=True)
    jobToReset.resetJob()
    jobDb.put(jobToReset, txn=txn)
    txn.commit()
    return jobToReset.__dict__()


def resetAllJobs(data):
    """Resets all jobs"""
    for keys in db.Job.db_key_tuples():
        txn = db.getTxn()
        jobDb = db.Job(*keys)
        jobToReset = jobDb.get(txn=txn)
        jobToReset.resetJob()
        jobDb.put(jobToReset, txn=txn)
        txn.commit()


def restartJob(data):
    print(data)


def restartAllJobs(data):
    for key in db.Job.db_key_tuples():
        txn = db.getTxn()

        jobDb = db.Job(*key)

        job = jobDb.get(txn=txn, write=True)

        restarted = job.restartUnfinished()

        if restarted:
            jobDb.put(job, txn=txn)
        txn.commit()


def getJob(data):
    """Gets job by ID"""
    txn = db.getTxn()
    output = db.Job(data['id']).get().__dict__()
    txn.commit()
    return output


def processNextQueuedTask(data):
    # Get highest priority queued job
    job = getHighestPriorityQueuedJob()

    if job is None:
        return

    txn = db.getTxn()
    jobDb = db.Job(job.id)
    txnJob = jobDb.get(txn=txn, write=True)

    taskToProcess = None

    for key in txnJob.tasks.keys():
        task = txnJob.tasks[key]

        if task['status'].lower() == 'queued':
            taskToProcess = task
            break

    if taskToProcess is None:
        txn.commit()
        return

    taskToProcess['status'] = 'Processing'

    txnJob.updateJobStatus()

    jobDb.put(txnJob, txn=txn)

    txn.commit()

    task = txnJob.addJobInfoOnTask(taskToProcess)
    return task


def getHighestPriorityQueuedJob():
    jobWithTask = None

    jobs = db.Job.all()

    lowerStatus = [status.lower() for status in statuses]
    queuedIndex = lowerStatus.index('queued')

    if len(jobs) < 1:
        return

    for job in jobs:
        jobIndex = lowerStatus.index(job.status.lower())
        # If job is new or queued
        if jobIndex <= queuedIndex:
            hasQueue = False

            # Check to see that job has a queued task
            for key in job.tasks.keys():
                task = job.tasks[key]
                if task['status'].lower() == 'queued':
                    hasQueue = True

            if hasQueue:
                if jobWithTask is None:
                    jobWithTask = job

                elif jobWithTask.getPriority() < job.getPriority():
                    jobWithTask = job

    return jobWithTask


def queueNextTask(data):
    job = getJobWithHighestPriority()

    if job is None:
        return

    taskToRun, key = getNextTaskInJob(job)

    # Re get the job to obtain write lock
    txn = db.getTxn()
    jobDb = db.Job(job.id)
    txnJob = jobDb.get(txn=txn, write=True)
    try:
        if txnJob.tasks != job.tasks:
            txn.abort()
            raise Exception(txnJob.__dict__(), job.__dict__())
    except ValueError:
        print('txnJob', txnJob.tasks)
        print('job', job.tasks)
        print('txnDict', txnJob.__dict__())
        print('jobDict', job.__dict__())

    taskToUpdate = txnJob.tasks[key]

    taskToUpdate['status'] = 'Queued'

    txnJob.updateJobStatus()

    jobDb.put(txnJob, txn=txn)

    txn.commit()

    task = txnJob.addJobInfoOnTask(taskToUpdate)

    return task


def getJobWithHighestPriority():
    jobWithTask = None

    jobs = db.Job.all()

    if len(jobs) < 1:
        return

    for job in jobs:
        if job.status.lower() == 'new':
            if jobWithTask is None:
                jobWithTask = job

            elif jobWithTask.getPriority() < job.getPriority():
                jobWithTask = job

    return jobWithTask


def getNextTaskInJob(job):
    tasks = job.tasks
    for key in tasks.keys():
        task = tasks[key]

        if task['status'].lower() == 'new':
            return task, key

    return None, None


def checkNewTask(data):
    """Checks through all the jobs to see if any of them are new"""
    jobs = db.Job.all()
    for job in jobs:
        if job.status.lower() == 'new':
            return True
    return False


def getAllJobs(data):
    jobs = []

    for key in db.Job.db_key_tuples():
        value = db.Job(*key).get()
        if value is None:
            continue

        jobs.append(value.__dict__())

    return jobs


def stats():
    numJobs = newJobs = queuedJobs = processingJobs = doneJobs = 0

    times = []

    jobs = []
    for job in db.Job.all():
        numJobs = numJobs + 1
        job.user = str(job.user)
        jobs.append(job)
        status = job.status.lower()

        if status == 'new':
            newJobs = newJobs + 1
        elif status == 'queued':
            queuedJobs = queuedJobs + 1
        elif status == 'processing':
            processingJobs = processingJobs + 1
        elif status == 'done':
            doneJobs = doneJobs + 1

            for task in job.tasks.values():
                if task['status'].lower() == 'done':
                    times.append(float(task['totalTime']))

    if len(times) == 0:
        avgTime = 0
    else:
        avgTime = str(sum(times) / len(times))

    output = {'numJobs': numJobs,
              'newJobs': newJobs,
              'queuedJobs': queuedJobs,
              'processingJobs': processingJobs,
              'doneJobs': doneJobs,
              'jobs': jobs,
              'avgTime': avgTime}

    return output
