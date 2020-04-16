import os
from setuptools import setup
import sys

base_path = '%s/opt/newrelic-python-agent' % os.getenv('VIRTUAL_ENV', '')
data_files = dict()
data_files[base_path] = ['LICENSE',
                         'README.rst',
                         'etc/init.d/newrelic-python-agent.deb',
                         'etc/init.d/newrelic-python-agent.rhel',
                         'etc/newrelic/newrelic-python-agent.cfg',
                         'apc-nrp.php']

console_scripts = ['newrelic-python-agent=newrelic_python_agent.agent:main']
install_requires = ['helper>=2.2.2', 'requests>=2.0.0', 'six>=1.5']
tests_require = []
extras_require = {'mongodb': ['pymongo'],
                  'pgbouncer': ['psycopg2'],
                  'postgresql': ['psycopg2'],
                  'prometheus': ['prometheus_client']}

if sys.version_info < (2, 7, 0):
    install_requires.append('importlib')

setup(name='newrelic_python_agent',
      version='1.4.0',
      description='Python based agent for collecting metrics for NewRelic',
      url='https://github.com/NewRelic-Python-Plugins/newrelic-python-agent',
      packages=['newrelic_python_agent', 'newrelic_python_agent.plugins'],
      author='Gavin M. Roy',
      author_email='gavinmroy@gmail.com',
      license='BSD',
      entry_points={'console_scripts': console_scripts},
      data_files=[(key, data_files[key]) for key in data_files.keys()],
      install_requires=install_requires,
      extras_require=extras_require,
      tests_require=tests_require,
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: System Administrators',
          'License :: OSI Approved :: BSD License',
          'Operating System :: POSIX',
          'Operating System :: POSIX :: Linux',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 2.6',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 2 :: Only',
          'Topic :: System :: Monitoring'])
