"""
Multiple Plugin Agent for the New Relic Platform

"""
import helper
import importlib
import json
import logging
import os
import requests
import socket
import sys
import threading
import time
import gzip

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

from newrelic_python_agent import __version__
from newrelic_python_agent import plugins
import newrelic_python_agent.plugins.base as base

is_py2 = sys.version[0] == '2'
if is_py2:
    # Python 2.7 uses `Queue` (first letter upper case)
    # https://docs.python.org/2/library/queue.html
    import Queue as queue
else:
    # Python 3.x uses `queue` (first letter lower case)
    # https://docs.python.org/3.5/library/queue.html
    import queue as queue

LOGGER = logging.getLogger(__name__)


class NewRelicPythonAgent(helper.Controller):
    """The NewRelicPythonAgent class implements a agent that polls plugins
    every minute and reports the state to NewRelic.

    """

    IGNORE_KEYS = ['license_key', 'proxy', 'endpoint', 'verify_ssl_cert',
                   'poll_interval', 'wake_interval', 'newrelic_api_timeout', 'skip_newrelic_upload']

    MAX_SIZE_PER_REQUEST = 750*1024   # behavior is undetermined if POST is larger than 1MB, so max at 750KB
    MAX_METRICS_PER_REQUEST = 10000   # limit is 20,000 per POST
    PLATFORM_URL = 'https://platform-api.newrelic.com/platform/v1/metrics'
    WAKE_INTERVAL = 60

    def __init__(self, args, operating_system):
        """Initialize the NewRelicPythonAgent object.

        :param argparse.Namespace args: Command line arguments
        :param str operating_system: The operating_system name

        """
        super(NewRelicPythonAgent, self).__init__(args, operating_system)
        self.derive_last_interval = dict()
        self.config_last_result = dict()
        self.clean_values = False
        self.endpoint = self.PLATFORM_URL
        self.http_headers = {'Accept': 'application/json',
                             'Content-Encoding': 'gzip',
                             'Content-Type': 'application/json'}
        self.last_interval_start = None
        self.min_max_values = dict()
        self._wake_interval = (self.config.application.get('wake_interval') or
                               self.config.application.get('poll_interval') or
                               self.WAKE_INTERVAL)
        self.next_wake_interval = int(self._wake_interval)
        self.config_queue = queue.Queue()
        self.publish_queue = queue.Queue()
        self.threads = list()
        info = tuple([__version__] + list(self.system_platform))
        LOGGER.info('Agent v%s initialized, %s %s v%s', *info)

    def setup(self):
        """Setup the internal state for the controller class. This is invoked
        on Controller.run().

        Items requiring the configuration object should be assigned here due to
        startup order of operations.

        """
        if hasattr(self.config.application, 'endpoint'):
            self.endpoint = self.config.application.endpoint
        self.http_headers['X-License-Key'] = self.license_key
        self.last_interval_start = time.time()

    @property
    def agent_data(self):
        """Return the agent data section of the NewRelic Platform data payload

        :rtype: dict

        """
        return {'host': socket.gethostname(),
                'pid': os.getpid(),
                'version': __version__}

    @property
    def license_key(self):
        """Return the NewRelic license key from the configuration values.

        :rtype: str

        """
        licensekey = os.getenv('NEW_RELIC_LICENSE_KEY')
        if licensekey is None:
            licensekey = os.getenv('NEWRELIC_LICENSE_KEY')
            if licensekey is None:
                licensekey = self.config.application.license_key
        return licensekey

    def get_instance_name(self, plugin_name, instance):
        """
        Determine a unique instance name for a plugin.  This is done by combining the
        plugin block name + the name field + an instance number

        Example:
            plugin name = mysql[:desc]
            block name = name field in block or unnamed
            instance number = incremental number in case previous is not unique

        :param str plugin_name: The plugin block name as defined in the application config
        :param dict instance: The instance config block
        :rtype: str
        """
        # start with the base name of the plugin + name field
        name = "%s:%s" % (plugin_name, instance.get('name', 'unnamed'))
        i = 0
        instance_name = "%s:%i" % (name, i)
        while instance_name in self.thread_names:
            i = i + 1
            instance_name = "%s:%i" % (name, i)

        return instance_name

    def start_plugin(self, plugin_name, plugin, config):
        """Kick off a background thread to run the processing task.

        :param plugin: The plugin name as defined in the application config
        :param config: The set of instance configs for the plugin
        :type plugin: newrelic_python_agent.plugins.base.Plugin
        :type config: dict or list(dict)

        """

        if not isinstance(config, (list, tuple)):
            config = [config]

        LOGGER.debug("Plugin config: %s", config)

        # the instance names must be unique so we can store the results for each.
        # use the 'name' field, if specified.  If there are duplicate names, we
        # simply append a number so it is unique.  As long as the the order in the
        # config remains the same, then the instance number will remain the same.
        for instance in config:
            instance_name = self.get_instance_name(plugin_name, instance)

            if issubclass(plugin, base.ConfigPlugin):
                thread = threading.Thread(target=self.thread_config_process,
                                          kwargs={'config': instance,
                                                  'name': instance_name,
                                                  'plugin': plugin})
            else:
                thread = threading.Thread(target=self.thread_metric_process,
                                          kwargs={'config': instance,
                                                  'name': instance_name,
                                                  'plugin': plugin,
                                                  'poll_interval':
                                                      int(self._wake_interval)})
            LOGGER.info("Starting plugin instance %s as thread %s", instance_name, thread.getName())
            self.thread_names[instance_name] = thread.getName()
            thread.start()
            self.threads.append(thread)

    def clean_last_values(self):
        """Remove any saved value data for plugins that are no longer configured"""
        for key in self.derive_last_interval.keys():
            if key not in self.thread_names:
                LOGGER.info("Removing last interval data for unused %s", key)
                self.derive_last_interval.pop(key)
        for key in self.config_last_result.keys():
            if key not in self.thread_names:
                LOGGER.info("Removing last config result for unused %s", key)
                self.config_last_result.pop(key)
        self.clean_values = False

    def process(self):
        """This method is called after every sleep interval. If the intention
        is to use an IOLoop instead of sleep interval based daemon, override
        the run method.

        """
        start_time = time.time()
        self.start_plugins()

        # Sleep for a second while threads are running
        while self.threads_running:
            time.sleep(1)

        # threads are done, so empty the list
        self.threads = list()

        # send any collected metrics to newrelic
        self.send_data_to_newrelic()

        # perform any config updates
        self.process_config_plugins()

        if self.clean_values:
            self.clean_last_values()

        duration = time.time() - start_time
        self.next_wake_interval = self._wake_interval - duration
        if self.next_wake_interval < 1:
            LOGGER.warning('Poll interval took greater than %i seconds',
                           duration)
            self.next_wake_interval = int(self._wake_interval)
        LOGGER.info('Threads processed in %.2f seconds, next wake in %i seconds',
                    duration, self.next_wake_interval)

    def process_min_max_values(self, component):
        """Agent keeps track of previous values, so compute the differences for
        min/max values.

        :param dict component: The component to calc min/max values for

        """
        guid = component['guid']
        name = component['name']

        if guid not in self.min_max_values.keys():
            self.min_max_values[guid] = dict()

        if name not in self.min_max_values[guid].keys():
            self.min_max_values[guid][name] = dict()

        for metric in component['metrics']:
            min_val, max_val = self.min_max_values[guid][name].get(
                    metric,
                    (None, None)
            )
            value = component['metrics'][metric]['total']
            if min_val is not None and min_val > value:
                min_val = value

            if max_val is None or max_val < value:
                max_val = value

            if component['metrics'][metric]['min'] is None:
                component['metrics'][metric]['min'] = min_val or value

            if component['metrics'][metric]['max'] is None:
                component['metrics'][metric]['max'] = max_val

            self.min_max_values[guid][name][metric] = min_val, max_val

    @property
    def proxies(self):
        """Return the proxy used to access NewRelic.

        :rtype: dict

        """
        if 'proxy' in self.config.application:
            return {
                'http': self.config.application['proxy'],
                'https': self.config.application['proxy']
            }
        return None

    def configuration_reloaded(self):
        # if the configuration was reloaded, then flag it so we can
        # check if we need to purget any old data.
        self.clean_values = True

    def process_config_plugins(self):
        """Process the queue of config plugin results"""

        while self.config_queue.qsize():
            (name, data) = self.config_queue.get()
            if isinstance(data, dict) and data.get('application'):
                LOGGER.debug("%s results" % name, extra={"results": data.get('application')})

                # this is a success, so save this
                self.config_last_result[name] = data

                # process each result individually
                for plugin_name in data['application'].keys():
                    action = None
                    if data['application'][plugin_name]:
                        # config is not empty
                        if plugin_name in self.config.application \
                                and self.config.application[plugin_name] == data['application'][plugin_name]:
                            action = "unchanged"
                        else:
                            # update or add new block
                            self.config.application.update({plugin_name: data['application'][plugin_name]})
                            action = "updated"
                            self.clean_values = True
                    elif plugin_name in self.config.application:
                        # config is empty for an existing plugin_name, so remove it
                        self.config.application.pop(plugin_name)
                        action = "removed"
                        self.clean_values = True

                    if action:
                        LOGGER.info("Plugin instance %s result %s %s", name, plugin_name, action)

    def send_data_to_newrelic(self):
        """Process the queue of metric plugin results"""
        size = 0
        metrics = 0
        components = list()
        while self.publish_queue.qsize():
            (name, data, last_values) = self.publish_queue.get()
            self.derive_last_interval[name] = last_values
            if isinstance(data, dict):
                data = [data]
            if isinstance(data, list):
                for component in data:
                    self.process_min_max_values(component)
                    components.append(component)
                    # track a rough approximation of payload size
                    size += len(json.dumps(component, ensure_ascii=False))
                    metrics += len(component['metrics'].keys())
                    if metrics >= self.MAX_METRICS_PER_REQUEST or size >= self.MAX_SIZE_PER_REQUEST:
                        self.send_components(components, metrics)
                        components = list()
                        metrics = 0
                        size = 0

        if metrics > 0:
            LOGGER.debug('Done, will send remainder of %i metrics', metrics)
            self.send_components(components, metrics)

    def send_components(self, components, metrics):
        """Create the headers and payload to send to NewRelic platform as a
        JSON encoded POST body.

        """
        if not metrics:
            LOGGER.warning('No metrics to send to NewRelic this interval')
            return

        body = {'agent': self.agent_data, 'components': components}

        s = StringIO()
        g = gzip.GzipFile(fileobj=s, mode='w')
        g.write(json.dumps(body, ensure_ascii=False))
        g.close()
        gzipped_body = s.getvalue()
        request_body = gzipped_body

        LOGGER.info('%sSending %i metrics for %i components to NewRelic (%i bytes compressed to %i bytes)',
                    "NOT " if self.config.application.get('skip_newrelic_upload') else "",
                    metrics,
                    len(components),
                    len(json.dumps(body, ensure_ascii=False)),
                    len(request_body))
        LOGGER.debug(body)

        if self.config.application.get('skip_newrelic_upload'):
            return

        try:
            response = requests.post(self.endpoint,
                                     headers=self.http_headers,
                                     proxies=self.proxies,
                                     data=request_body,
                                     timeout=self.config.application.get('newrelic_api_timeout', 10),
                                     verify=self.config.application.get('verify_ssl_cert', True))

            LOGGER.info('Response: %s: %r',
                        response.status_code,
                        response.content.strip())
        except requests.ConnectionError as error:
            LOGGER.error('Error reporting stats: %s', error)
        except requests.Timeout as error:
            LOGGER.error('TimeoutError reporting stats: %s', error)

    @staticmethod
    def _get_plugin(plugin_path):
        """Given a qualified class name (eg. foo.bar.Foo), return the class

        :rtype: object

        """
        try:
            package, class_name = plugin_path.rsplit('.', 1)
        except ValueError:
            return None

        try:
            module_handle = importlib.import_module(package)
            class_handle = getattr(module_handle, class_name)
            return class_handle
        except ImportError:
            LOGGER.exception('Attempting to import %s', plugin_path)
            return None

    def start_plugins(self):
        """Iterate through each plugin and start the thread(s)."""

        # reset the configured instance names
        self.thread_names = dict()

        for plugin in [key for key in self.config.application.keys()
                       if key not in self.IGNORE_KEYS]:

            # ignore this if the config is empty
            if not self.config.application.get(plugin):
                continue

            LOGGER.info('Checking plugin config: %s', plugin)
            # support plugin:id format to allow multiple config blocks
            # for a single plugin
            plugin_name = plugin.split(":", 1)[0]
            plugin_class = None

            # If plugin is part of the core agent plugin list
            if plugin_name in plugins.available:
                plugin_class = self._get_plugin(plugins.available[plugin_name])

            # If plugin is in config and a qualified class name
            elif '.' in plugin_name:
                plugin_class = self._get_plugin(plugin_name)

            # If plugin class could not be imported
            if not plugin_class:
                LOGGER.error('Plugin %s not available', plugin_name)
                continue

            self.start_plugin(plugin, plugin_class,
                              self.config.application.get(plugin))

    @property
    def threads_running(self):
        """Return True if any of the child threads are alive

        :rtype: bool

        """
        for thread in self.threads:
            if thread.is_alive():
                return True
        return False

    def thread_config_process(self, name, plugin, config):
        """
        Create a thread process to return a dynamic config for a plugin.
        The result of this plugin is added to a Queue object which is used
        to maintain the stack of running config plugins.

        :param str name: The unique instance name of the plugin
        :param newrelic_python_agent.plugins.base.ConfigPlugin plugin: The plugin class
        :param dict config: The plugin configuration
        """
        previous_state = self.config_last_result.get(name)
        obj = plugin(config, previous_state)
        obj.start()
        self.config_queue.put((name, obj.results()))

    def thread_metric_process(self, name, plugin, config, poll_interval):
        """Created a thread process for the given name, plugin class,
        config and poll interval. Process is added to a Queue object which
        used to maintain the stack of running metrics plugins.

        :param str name: The unique instance name of the plugin
        :param newrelic_python_agent.plugins.base.Plugin plugin: The plugin class
        :param dict config: The plugin configuration
        :param int poll_interval: How often the plugin is invoked

        """
        obj = plugin(config, poll_interval,
                     self.derive_last_interval.get(name))
        obj.poll()
        self.publish_queue.put((name, obj.values(),
                                obj.derive_last_interval))

    @property
    def wake_interval(self):
        """Return the wake interval in seconds as the number of seconds
        until the next minute.

        :rtype: int

        """
        return self.next_wake_interval


def main():
    helper.parser.description('The NewRelic Plugin Agent polls various '
                              'services and sends the data to the NewRelic '
                              'Platform')
    helper.parser.name('newrelic_python_agent')
    argparse = helper.parser.get()
    argparse.add_argument('-C',
                          action='store_true',
                          dest='configure',
                          help='Run interactive configuration')
    args = helper.parser.parse()
    if args.configure:
        print('Configuration')
        sys.exit(0)
    helper.start(NewRelicPythonAgent)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
