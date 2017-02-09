#!/usr/bin/env python
# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
import re


try:
    from setuptools import setup
except:
    from distutils.core import setup


with open('README.md') as fh:
    long_description = fh.read()

with open('pyoozie/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')

setup(
    name='pyoozie',
    version=version,
    description='A Python client for querying and scheduling with Oozie',
    long_description=long_description,
    author='Shopify Data Acceleration',
    author_email='data-acceleration@shopify.com',
    url='https://github.com/Shopify/pyoozie',
    packages=['pyoozie'],
    install_requires=[
        'enum34>=0.9.23',
        'yattag>=1.7.2',
        'setuptools>=0.9',
    ],
    license="MIT",
    keywords=['oozie'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    test_suite='tests',
)
