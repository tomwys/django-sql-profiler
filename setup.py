#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

setup(
    name='django-sql-profiler',
    version='0.1.3',
    description="Sql profiler for PostgreSQL Django backend.",
    maintainer="Tomasz Wysocki",
    maintainer_email="tomasz@wysocki.info",
    packages=find_packages(),
    include_package_data=True,
)
