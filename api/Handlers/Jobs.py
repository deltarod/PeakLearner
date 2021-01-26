from api.util import PLdb as db
from api.Handlers.Handler import Handler


class JobHandler(Handler):
    """Handles Job Commands"""

    def do_POST(self, data):
        return self.getCommands()[data['command']](data['args'])

    @classmethod
    def getCommands(cls):
        # TODO: Add update/delete/info
        return {'get': getJob,
                'add': updateJob,
                'update': updateJob,
                'remove': removeJob,
                'getAll': getAllJobs}


# Adds new job to list for slurm server to process
def updateJob(data):
    print('updating job')
    updated, current = db.Job('jobs').add(data)

    return updated


# Will get job either by ID or next new job
def getJob(data):
    jobs = db.Job('jobs').get()

    jobOutput = {}

    for job in jobs:
        if job['id'] == data['id']:
            jobOutput = job
    return jobOutput


def removeJob(data):
    output = db.Job('jobs').remove(data)
    return output


def getAllJobs(data):
    output = db.Job('jobs').get()
    print('getting all jobs', output)
    if len(output) >= 1:
        return output
