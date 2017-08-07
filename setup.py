from setuptools import setup

VERSION = __import__('globomap_driver_acs').VERSION

setup(
    name='globomap-driver-acs',
    version=VERSION,
    description='Python library for globomap-driver to get data '
                'from Cloudstack',
    author='Victor Mendes Eduardo',
    author_email='victor.eduard@corp.globo.com',
    install_requires=[
        'pika==0.10.0',
        'python-dateutil==2.4.2',
    ],
    url='https://github.com/globocom/globomap-driver-acs',
    packages=['globomap_driver_acs'],
)
