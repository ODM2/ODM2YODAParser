from __future__ import (absolute_import, division, print_function)

import codecs
import os

from setuptools import find_packages, setup

import versioneer


here = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    return codecs.open(os.path.join(here, *parts), 'r').read()


long_description = read('README.md')

# Dependencies.
with open('requirements.txt') as f:
    requirements = f.readlines()
install_requires = [t.strip() for t in requirements]


def walk_subpkg(name):
    data_files = []
    package_dir = 'yodatools'
    for parent, dirs, files in os.walk(os.path.join(package_dir, name)):
        # Remove package_dir from the path.
        sub_dir = os.sep.join(parent.split(os.sep)[1:])
        for f in files:
            data_files.append(os.path.join(sub_dir, f))
    return data_files


pkg_data = {
    '': walk_subpkg('dataloader/templates') + walk_subpkg('yodaparser')
}

setup(
    name='YODA-Tools',
    version=versioneer.get_version(),
    author='Stephanie Reeder',
    author_email='stephanie.reeder@usu.edu',
    description='Tools to validate and manage YODA files',
    long_description=long_description,
    packages=find_packages(),
    package_data=pkg_data,
    entry_points="""
        [console_scripts]
        yodatools=yodatools.dataloader.controller.Main:main
        """,
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    install_requires=install_requires,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    cmdclass=versioneer.get_cmdclass(),
)
