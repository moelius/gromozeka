import io
import os
import re
from os.path import join, dirname

from setuptools import setup, find_packages

PACKAGE = "gromozeka"
NAME = "gromozeka"
DESCRIPTION = "Distributed task queue."
AUTHOR = "Klimov Konstantin"
AUTHOR_EMAIL = "moelius1983@gmail.com"
URL = "https://github.com/moelius/gromozeka"

with io.open('gromozeka/__init__.py', 'rt', encoding='utf8') as f:
    version = re.search(r'__version__ = \'(.*?)\'', f.read()).group(1)


def _strip_comments(l):
    return l.split('#', 1)[0].strip()


def _pip_requirement(req):
    if req.startswith('-r '):
        _, path = req.split()
        return requirements_list(*path.split('/'))
    return [req]


def _requirements_list(*files):
    return [
        _pip_requirement(r) for r in
        (_strip_comments(l) for l in open(os.path.join(os.getcwd(), 'requirements', *files)).readlines()) if r]


def requirements_list(*files):
    return [req for sub in _requirements_list(*files) for req in sub]


setup(
    name=NAME,
    version=version,
    description=DESCRIPTION,
    long_description=open(join(dirname(__file__), 'README.rst')).read(),
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    keywords='task job queue distributed messaging pika rabbit rabbitmq pool worker',
    license="MIT",
    url=URL,
    packages=find_packages(exclude=["tests.*", "tests"]),
    install_requires=requirements_list('base.txt'),
    extras_require={
        'docs': requirements_list('docs.txt'),
        'visual': requirements_list('visual.txt'),
    },
    zip_safe=False,
)
