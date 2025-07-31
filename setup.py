#!/usr/bin/env python

from setuptools import setup, find_packages
import subprocess

setup(name="tap-intacct",
      version='1.1.0',
      description="Singer.io tap for extracting data from Intacct",
      author="Stitch",
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      url="http://singer.io",
      install_requires=[
          'singer-encodings==0.1.3',
          'singer-python==6.1.1',
          'boto3==1.39.17',
          'backoff==2.2.1',
      ],
      entry_points='''
          [console_scripts]
          tap-intacct=tap_intacct:main
      ''',
      packages=find_packages(),
)
