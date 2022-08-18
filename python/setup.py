import os
from setuptools import setup, find_packages

setup_py_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(setup_py_dir)

setup(
    name='debezium',
    entry_points={
        'console_scripts': [
            'debezium = debezium:main',
        ],
    },
    version='0.1.0',
    packages=find_packages(),
    author="Memiiso Organization",
    description='Debezium Server Python runner',
    url='https://debezium.io/',
    include_package_data=True,
    license="Apache License 2.0",
    test_suite='tests',
    install_requires=["pyjnius==1.4.0"],
    python_requires='>=3',
)
