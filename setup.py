#!/usr/bin/env python

import os
from distutils.core import setup

def find_scripts(path = "scripts"):
    scripts = list()
    for item in os.listdir(path):
        p = os.path.join(path, item)
        if os.access(p, os.X_OK):
            scripts.append(p)
    return scripts


scripts = find_scripts()

setup(
    name = 'nzbmonkey',
    version = '0.0.1',
    description = 'Assemble nzb files from usenet articles',
    author = 'Michael van Bracht',
    author_email = 'michael@wontfix.org',
    url = 'http://wontfix.org/projects/nzbmonkey/',

    scripts = scripts,
    packages = ['nzbmonkey'],
    data_files = [
        ('/etc/', ['files/nzbmonkey.conf'])
    ] ,
)

