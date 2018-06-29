# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='yelp',
    version='0.1.0',
    description='Yelp API Python interface',
    long_description=readme,
    author='',
    author_email='',
    url='',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)

