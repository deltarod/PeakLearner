from dozer import Profiler

from pyramid.paster import get_app, setup_logging
ini_path = '/build/PeakLearner/production.ini'
setup_logging(ini_path)
application = Profiler(get_app(ini_path, 'main'), profile_path='testProfile/')
