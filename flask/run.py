#!/usr/bin/env python

from app import app
app.run(host='0.0.0.0', port=80, debug = False)
#app.run(host='127.0.0.1', debug = True)