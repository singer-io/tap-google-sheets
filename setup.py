#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-google-sheets',
      version='1.0.4',
      description='Singer.io tap for extracting data from the Google Sheets v4 API',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_google_sheets'],
      install_requires=[
          'backoff==1.8.0',
          'singer-python==5.9.0',
          'google-api-python-client==1.12.5',
          'google-auth==1.23.0',
          'google-auth-oauthlib==0.4.2',
      ],
      extras_require={
          'dev': [
              'ipdb==0.11',
              'pylint',
              'nose'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-google-sheets=tap_google_sheets:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_google_sheets': [
              'schemas/*.json'
          ]
      })
