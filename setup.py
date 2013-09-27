from setuptools import setup, find_packages

setup(
    name='beanstalk-cli',
    version='0.4.1',
    license='MIT',
    description='Interactive client for beanstalk',
    author='dccmx@dccmx.com',
    packages=find_packages('.'),
    install_requires=['pyyaml', 'beanstalkc'],
    entry_points={
        'console_scripts': [
            'beanstalk-cli = cli.main:main'
        ],
    }
)
