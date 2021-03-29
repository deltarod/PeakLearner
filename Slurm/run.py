import os
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
    try:
        query = {'command': 'check'}
        r = requests.post(cfg.jobUrl, json=query, timeout=5)
    except requests.exceptions.ConnectionError:
        return False

    if not r.status_code == 200:
        return False

    # If no new jobs to process, return false and sleep
    if not r.json():
        return False

    try:
        query = {'command': 'queueNextTask'}
        r = requests.post(cfg.jobUrl, json=query, timeout=5)
    except requests.exceptions.ConnectionError:
        return False

    if cfg.useSlurm:
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


if __name__ == '__main__':
    startTime = time.time()

    if cfg.useCron:
        timeDiff = lambda: time.time() - startTime

        while timeDiff() < cfg.timeToRun:
            while startNextTask():
                pass
            time.sleep(1)
    else:
        startNextTask()

        endTime = time.time()

        print("Start Time:", startTime, "End Time", endTime)
