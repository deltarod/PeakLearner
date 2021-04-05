import os
import sys
import requests
import time
import tempfile

if __name__ == '__main__':
    import Tasks as tasks
    import SlurmConfig as cfg
else:
    import Slurm.Tasks as tasks
    import Slurm.SlurmConfig as cfg


def startNextTask():
    if not os.path.exists(cfg.dataPath):
        os.makedirs(cfg.dataPath)


        createSlurmTask()
    else:
        tasks.runTask()

    return True


def createSlurmTask():
    jobName = 'PeakLearner'
    jobString = '#!/bin/bash\n'
    jobString += '#SBATCH --job-name=%s\n' % jobName
    jobString += '#SBATCH --output=%s\n' % os.path.join(os.getcwd(), cfg.dataPath, jobName + '.txt')
    jobString += '#SBATCH --chdir=%s\n' % os.getcwd()
    jobString += '#SBATCH --open-mode=append'

    # TODO: Make resource allocation better
    jobString += '#SBATCH --time=%s:00\n' % cfg.maxJobLen

    if cfg.monsoon:
        jobString += '#SBATCH --mem=1024\n'
        jobString += 'module load anaconda3\n'
        jobString += 'module load R\n'
        jobString += 'conda activate %s\n' % cfg.condaVenvPath

    jobString += 'srun python3 Slurm/Tasks.py'

    with tempfile.NamedTemporaryFile(mode='w', suffix='.sh') as temp:
        temp.write(jobString)
        temp.flush()
        temp.seek(0)

        command = 'sbatch %s' % temp.name

        os.system(command)



def runMonsoon():
    if not checkNextTask():
        print('No new task')
        return

    queueNextTask()

    tasks.runTask()

    startNextSlurmJob()


def runTest():
    if not checkNextTask():
        print('No new task')
        return

    queueNextTask()

    tasks.runTask()


def checkNextTask():
    try:
        query = {'command': 'check'}
        r = requests.post(cfg.jobUrl, json=query, timeout=10)
    except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
        return False

    if not r.status_code == 200:
        return False

    # If no new jobs to process, return false and sleep
    if not r.json():
        return False

    return True


def queueNextTask():
    try:
        query = {'command': 'queueNextTask'}
        r = requests.post(cfg.jobUrl, json=query, timeout=10)
    except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
        return False

    if not r.status_code == 200:
        return False

    return True


def startNextSlurmJob():



configs = {'test': runTest(),
           'monsoon': runMonsoon()}


def getConfig():
    try:
        return configs[cfg.configuration]
    except KeyError:
        print('Invalid config, defaulting to test config')
        return runTest


if __name__ == '__main__':

    # For monsoon jobs, need to specify inputs
    args = len(sys.argv) - 1
    if args == 1:
        arg = sys.argv[1]
        if arg in configs.keys():
            cfg.configuration = arg

    getConfig()()
