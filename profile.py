from werkzeug.contrib.profiler import ProfilerMiddleware

from pyramid.paster import get_app, setup_logging
ini_path = '/build/PeakLearner/production.ini'
setup_logging(ini_path)
application = get_app(ini_path, 'main')

application.config['PROFILE'] = True
application.wsgi_app = ProfilerMiddleware(application.wsgi_app, restrictions=[30])
application.run(debug=True)
