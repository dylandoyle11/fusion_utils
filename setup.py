# setup.py

from setuptools import setup, find_packages

setup(
    name='fusion_utils',
    version='0.11',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'google-cloud-bigquery'
        # Add other dependencies here
    ],
    author='Dylan D',
    author_email='dylan.doyle@jdpa.com',
    description='A utility package Fusion 2.0',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/dylandoyle11/fusion_utils', 
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)


