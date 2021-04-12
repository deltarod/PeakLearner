import configparser

configFile = 'PeakLearnerSlurm.cfg'

config = configparser.ConfigParser()
config.optionxform = str
config.read(configFile)

configSections = config.sections()

save = False

if 'general' not in configSections:
    config.add_section('general')
    config['general']['configuration'] = 'test'
    config['general']['debug'] = 'False'
    config['general']['slurm_script'] = 'PeakLearnerSlurmMonsoonRequeue.sh'

# Setup a default config if doesn't exist
if 'remoteServer' not in configSections:
    config.add_section('remoteServer')
    config['remoteServer']['url'] = 'http://localhost'
    config['remoteServer']['port'] = '8081'
    save = True

if 'slurm' not in configSections:
    config.add_section('slurm')
    config['slurm']['dataPath'] = 'slurmdata/'
    save = True

# If a section was missing, save that to the config
if save:
    with open(configFile, 'w') as cfg:
        config.write(cfg)

port = int(config['remoteServer']['port'])

if port == 80:
    remoteServer = "%s/" % config['remoteServer']['url']
else:
    remoteServer = "%s:%s/" % (config['remoteServer']['url'], config['remoteServer']['port'])

configuration = config['general']['configuration'].lower()
slurmScript = config['general']['slurm_script']
debug = config['general']['debug'].lower() == 'true'
dataPath = config['slurm']['dataPath']
jobUrl = '%sjobs/' % remoteServer
