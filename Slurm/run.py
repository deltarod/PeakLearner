import os
import sys
import time
import requests

if __name__ == '__main__':
    import Tasks as tasks
    import SlurmConfig as cfg
else:
    import Slurm.Tasks as tasks
    import Slurm.SlurmConfig as cfg


def runMonsoon():
    if not checkNextTask():
        print('No new task')
        return

    if queueNextTask():
        tasks.runTask()
    else:
        time.sleep(5)

    if not cfg.debug:
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
    scriptPath = os.path.join(os.getcwd(), cfg.slurmScript)
    os.system(scriptPath)


configs = {'test': runTest,
           'monsoon': runMonsoon}


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
