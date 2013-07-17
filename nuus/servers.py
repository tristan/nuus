def run_tornado(app, urls=[], debug=False, address='0.0.0.0', port=5000):
    import logging
    import tornado.httpserver
    import tornado.ioloop
    import tornado.wsgi
    import tornado.web
    import tornado.options
    import tornado.autoreload
    import sys

    tornado.options.parse_command_line() # makes logger work

    # print out request/response log
    if debug:
        tornado.options.options.logging = 'debug'

    logger = logging.getLogger('server')
    flsk = tornado.wsgi.WSGIContainer(app)
    application = tornado.web.Application(urls + [(r".*", tornado.web.FallbackHandler, dict(fallback=flsk))])
    # TODO: if a port is already bound, starting the ioloop doesn't alert of a failure, see if I can change this
    application.listen(port, address=address)

    # TODO: http://www.tornadoweb.org/en/stable/autoreload.html says this should be set up
    # by tornado.web.Application(debug=True), but I haven't been able to find the equivalent
    # for the WSGIContainer setup yet.
    if debug:
        def fn():
            logger.info('Reloading...')
        tornado.autoreload.add_reload_hook(fn)
        tornado.autoreload.start()

    logger.info('Starting http server on http://%s:%s' % (address,port))
    if sys.platform == 'win32':
        logger.info('if CTRL+C fails to exit, use CTRL+BREAK')
    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        # only gets thrown when autoreload is set up
        logger.info('Stopping http server')
        tornado.ioloop.IOLoop.instance().stop()
