#!/usr/bin/env python
from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'requirements.txt')) as f:
    requirements = f.read().splitlines()

setup(
    name='cosecha_colectiva',
    version='0.1.0',
    description='Project for testing cosecha colectiva back-end',
    author='Alvaro Luna',
    author_email='alvalunasan@gmail.com',
    packages=find_packages(exclude=[]),
    install_requires=requirements,
)
