# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from setuptools import find_packages, setup

# gross, but git submodule is the simplest way to install these (there's no egg), so add them to our structure
neptune_python_utils_package_names = find_packages('amazon-neptune-tools/neptune-python-utils')
neptune_python_utils_package_directories = dict((name, f'amazon-neptune-tools/neptune-python-utils/{name}')
                                                for name in neptune_python_utils_package_names)

setup(
    name='amundsen-gremlin',
    version='0.0.7',
    description='Gremlin code library for Amundsen',
    url='https://github.com/amundsen-io/amundsengremlin',
    maintainer='Amundsen TSC',
    maintainer_email='amundsen-tsc@lists.lfai.foundation',
    packages=find_packages(exclude=['tests*']) + neptune_python_utils_package_names,
    package_dir=neptune_python_utils_package_directories,
    zip_safe=False,
    dependency_links=[],
    include_package_data=True,
    install_requires=[],
    python_requires=">=3.6",
    package_data={'amundsen_gremlin': ['py.typed']},
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)
