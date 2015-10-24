#!/usr/bin/env python

from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from app import app

import tornado.options
tornado.options.parse_command_line()

http_server = HTTPServer(WSGIContainer(app))
http_server.listen(80)
IOLoop.instance().start()

# flask alone
#app.run(host='0.0.0.0', port=80, debug = True)

#app.run(host='0.0.0.0', debug = True)
#app.run(host='0.0.0.0', port=80, debug = False)
