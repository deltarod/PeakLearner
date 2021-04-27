from pyramid.view import view_config
from api.util import PLdb as db
from api.Handlers import Models, Labels, Jobs


@view_config(route_name='home', renderer='index.html')
def home(request):
    return {}


@view_config(route_name='about', renderer='about.html')
def about(request):
    return {}


@view_config(route_name='newHub', renderer='newHub.html')
def newHub(request):
    return {}


@view_config(route_name='tutorial', renderer='tutorial.html')
def tutorial(request):
    return {}


@view_config(route_name='backup', renderer='backup.html')
def backup(request):
    return {'last_backup': db.getLastBackup(),
            'backups': db.getAvailableBackups()}


@view_config(route_name='stats', renderer='stats.html')
def stats(request):
    numLabeledChroms, numLabels = Labels.stats()
    currentJobStats = Jobs.stats()
    return {'numModels': Models.numModels(),
            'numLabeledChroms': numLabeledChroms,
            'numLabels': numLabels,
            'numJobs': currentJobStats['numJobs'],
            'newJobs': currentJobStats['newJobs'],
            'queuedJobs': currentJobStats['queuedJobs'],
            'processingJobs': currentJobStats['processingJobs'],
            'doneJobs': currentJobStats['doneJobs'],
            'avgTime': currentJobStats['avgTime']}


# TODO: Maybe make these stats user specific?
@view_config(route_name='modelStats', renderer='stats/models.html')
def modelStats(request):
    return {'numModels': Models.numModels(),
            'correctModels': Models.numCorrectModels()}


@view_config(route_name='labelStats', renderer='stats/labels.html')
def labelStats(request):
    numLabeledChroms, numLabels = Labels.stats()

    return {'numLabeledChroms': numLabeledChroms,
            'numLabels': numLabels}


@view_config(route_name='jobsStats', renderer='stats/jobs.html')
def jobsStats(request):
    return Jobs.stats()


@view_config(route_name='jobStats', renderer='stats/job.html')
def jobStats(request):
    id = request.matchdict['id']

    job = db.Job(id).get()

    return job.__dict__()

