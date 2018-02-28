from os.path import join, dirname

from pip.req import parse_requirements
from setuptools import setup, find_packages

PACKAGE = "gromozeka"
NAME = "gromozeka"
DESCRIPTION = "Distributed task queue."
AUTHOR = "Klimov Konstantin"
AUTHOR_EMAIL = "moelius1983@gmail.com"
URL = "https://github.com/moelius/gromozeka"
VERSION = __import__(PACKAGE).__version__

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=open(join(dirname(__file__), 'README.rst')).read(),
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    license="MIT",
    url=URL,
    packages=find_packages(exclude=["tests.*", "tests"]),
    install_requires=[str(ir.req) for ir in parse_requirements('requirements.txt', session='session')],
    extras_require={
        'docs': [str(ir.req) for ir in parse_requirements('requirements/docs.txt', session='session')],
    },
    zip_safe=False,
)
