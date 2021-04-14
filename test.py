import os
import time
import requests


remoteServer = "%s:%s/" % ('http://localhost', '8080')
jobUrl = '%sjobs/' % remoteServer


def checkNextTask():
    try:
        query = {'command': 'check'}
        r = requests.post(jobUrl, json=query, timeout=10)
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
        r = requests.post(jobUrl, json=query, timeout=10)
    except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
        return False

    if not r.status_code == 200:
        return False

    return r.json()


if __name__ == '__main__':
    checkNextTask()

    out = queueNextTask()

    print(out)
