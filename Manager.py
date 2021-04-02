from api.util import PLConfig as cfg
import time

shutdownServer = False


def managerLoop():

    lastRun = time.time()
    timeDiff = lambda: time.time() - lastRun

    while not shutdownServer:





def shutdown():
    global shutdownServer
    shutdownServer = True


if __name__ == '__main__':
    managerLoop()
