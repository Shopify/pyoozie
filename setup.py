#!/usr/bin/env python
# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
import re


try:
    import setuptools as setuplib
except ImportError:
    import distutils.core as setuplib


def get_version():
    version = None
    with open('pyoozie/__init__.py', 'r') as fdesc:
        version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fdesc.read(), re.MULTILINE).group(1)
    if not version:
        raise RuntimeError('Cannot find version information')
    return version


setuplib.setup(
    name='pyoozie',
    version=get_version(),
    description='A Python client for querying and scheduling with Oozie',
    author='Shopify Data Acceleration',
    author_email='data-acceleration@shopify.com',
    url='https://github.com/Shopify/pyoozie',
    packages=['pyoozie'],
    install_requires=[
        'enum34>=0.9.23',
        'requests>=2.12.3',
        'six>=1.10.0',
        'typing',
        'untangle>=1.1.0',
        'yattag>=1.7.2',
    ],
    extras_require={
        'deploy': [
            'setuptools>=0.9'
        ],
        'test': [
            'autopep8',
            'flake8',
            'mock',
            'pycodestyle == 2.2.0',
            'pylint>=1.6.5,<1.7',
            'pytest-cov',
            'pytest-randomly',
            'pytest>=2.7',
            'requests-mock',
            'shopify_python==0.4.1',
            'xmltodict',
            'pywebhdfs>=0.4.1',
        ],
        'testpy3: python_version >= "3.3"': [
            'mypy',
        ]
    },
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
