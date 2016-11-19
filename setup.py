#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from snoPlowPy.version import __version__

setup(name='snoPlowPy',
      version=__version__,
      # description='',
      author='weng-lab',
      # author_email='',
      url='https://github.com/weng-lab/SnoPlowPy',
      license='MIT',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Science/Research',
          'Topic :: Scientific/Engineering :: Bio-Informatics',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 2.6',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
      ],
      # keywords='',
      packages=find_packages(),
      install_requires=[
          'future',
      ]
      )
