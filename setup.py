import io
import re
from os.path import join, dirname

from pip.req import parse_requirements
from setuptools import setup, find_packages

PACKAGE = "gromozeka"
NAME = "gromozeka"
DESCRIPTION = "Distributed task queue."
AUTHOR = "Klimov Konstantin"
AUTHOR_EMAIL = "moelius1983@gmail.com"
URL = "https://github.com/moelius/gromozeka"

with io.open('gromozeka/__init__.py', 'rt', encoding='utf8') as f:
    version = re.search(r'__version__ = \'(.*?)\'', f.read()).group(1)

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
    install_requires=[str(ir.req) for ir in parse_requirements('requirements.txt', session='session')],
    extras_require={
        'docs': [str(ir.req) for ir in parse_requirements('requirements/docs.txt', session='session')],
    },
    zip_safe=False,
)
