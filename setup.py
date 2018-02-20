#!/usr/bin/env python
#-*- coding: UTF-8 -*-

from distutils.core import setup

setup(name="castor",
      version="1.0",
      description="Python 2.x module to store metrics in cassandra (works like rrdtool but with a storage based on cassandra)",
      author="Pascal BERINGUIE",
      author_email="pascal.beringuie@orange.com",
      url="https://gforge-portail.orangeportails.net/projects/castor/",
      packages=["castor"],
     )
