#!/usr/bin/env python

from setuptools import setup, find_packages
import subprocess

setup(name="tap-intacct",
      version='0.0.2',
      description="Singer.io tap for extracting data from Intacct",
      author="Stitch",
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      url="http://singer.io",
      install_requires=[
          'singer-encodings==0.0.2',
          'singer-python==5.1.5',
          'boto3==1.4.4'
      ],
      entry_points='''
          [console_scripts]
          tap-intacct=tap_intacct:main
      ''',
      packages=find_packages(),
)
