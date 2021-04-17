import configparser

minScale = 0.5
gridSearchSize = 10
stopScaling = 1
test = False
geneUrl = 'https://hgdownload.soe.ucsc.edu/goldenPath/'


configFile = 'PeakLearner.cfg'

config = configparser.ConfigParser()
config.read(configFile)

configSections = config.sections()

save = False

# Setup a default config if doesn't exist
if 'http' not in configSections:
    config.add_section('http')
    config['http']['path'] = 'jbrowse/jbrowse/'
    config['http']['backupPath'] = 'backup'

    save = True

if 'data' not in configSections:
    config.add_section('data')
    config['data']['path'] = 'data/'

    save = True

if 'learning' not in configSections:
    config.add_section('learning')
    config['learning']['doIdlePredictions'] = 'False'
    config['learning']['timeBetween'] = '600'
    config['learning']['numChanges'] = '10'
    config['learning']['minLabeledRegions'] = '20'

    save = True

# If a section was missing, save that to the config
if save:
    with open(configFile, 'w') as cfg:
        config.write(cfg)

# get ports from config
jbrowsePath = config['http']['path']

dataPath = config['data']['path']
timeBetween = int(config['learning']['timeBetween'])
numChanges = int(config['learning']['numChanges'])
minLabeledRegions = int(config['learning']['minLabeledRegions'])
backupPath = config['http']['backupPath']
doIdlePredictions = config['learning']['doIdlePredictions'].lower() == 'true'


def testing():
    global test, geneUrl
    test = True
    geneUrl = 'https://rcdata.nau.edu/genomic-ml/PeakLearner/files/'
