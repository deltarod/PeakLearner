from pyramid.config import Configurator


def main(global_config, **settings):
    config = Configurator(settings=settings)
    # Front end page
    config.add_route('home', '')
    config.add_route('about', '/about/')
    config.add_route('jbrowse', '/jbrowse/')
    config.add_route('newHub', '/newHub/')
    config.add_route('tutorial', '/tutorial/')
    config.add_route('backup', '/backup/')
    config.add_route('stats', '/stats/')
    config.add_route('modelStats', '/stats/model/')
    config.add_route('labelStats', '/stats/label/')
    config.add_route('jobsStats', '/stats/job/')
    config.add_route('jobStats', '/stats/job/{id}/')
    config.add_static_view(name='tutorial/static', path='website:static/tutorial')
    config.add_route('uploadHubUrl', '/uploadHubUrl/')
    config.add_route('jobs', '/jobs/')
    config.add_route('jobInfo', '/jobs/info/')
    config.include('pyramid_jinja2')
    config.add_jinja2_renderer('.html')
    config.scan('website.views')

    config.add_route('api', '/api/')
    config.add_route('doBackup', '/doBackup/')
    config.add_route('doRestore', '/doRestore/')
    config.add_route('prediction', '/prediction/')
    config.add_route('hubInfo', '/{user}/{hub}/info/')
    config.add_route('hubData', '/{user}/{hub}/data/{handler}')
    config.add_route('trackData', '/{user}/{hub}/{track}/{handler}/')
    config.add_static_view(name='/{user}/{hub}', path='jbrowse:jbrowse')

    config.scan('api.views')

    return config.make_wsgi_app()
