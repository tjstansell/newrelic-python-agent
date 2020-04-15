"""
Generic prometheus Support

"""
from prometheus_client.parser import text_string_to_metric_families
from functools import reduce
import logging
import re

from newrelic_python_agent.plugins import base

LOGGER = logging.getLogger(__name__)

class Prometheus(base.HTTPStatsPlugin):

    DEFAULT_PATH = 'metrics'
    GUID = 'com.meetme.newrelic_prometheus_agent'
    INCLUDE_CONFIG_KEY = 'include'
    EXCLUDE_CONFIG_KEY = 'exclude'
    GAUGES_CONFIG_KEY = 'gauges'

    def __init__(self, config, poll_interval, last_interval_values=None):
        super(Prometheus, self).__init__(config, poll_interval, last_interval_values)


    def add_datapoints(self, raw_metrics):
        """Add all of the data points for a node

        :param str metrics: The metrics content

        """
        hasMetrics = False
        if not raw_metrics:
            return
        for family in text_string_to_metric_families(raw_metrics):
            for sample in family.samples:
                hasMetrics = True
                if (
                    not self.INCLUDE_CONFIG_KEY in self.config or
                    sample.name in self.config[self.INCLUDE_CONFIG_KEY]
                ):
                    if (
                        self.EXCLUDE_CONFIG_KEY in self.config and
                        sample.name in self.config[self.EXCLUDE_CONFIG_KEY]
                    ):
                        LOGGER.debug('Ignoring sample: %r', sample)
                    else:
                        name = reduce(
                            (lambda k, i: k + '/' + i[0] + '/' + i[1]),
                            sample.labels.iteritems(),
                            sample.name
                        )
                        if (
                            self.GAUGES_CONFIG_KEY in self.config and
                            sample.name in self.config[self.GAUGES_CONFIG_KEY]
                        ):
                            self.add_gauge_value(name,
                                sample.name,
                                sample.value)
                        else:
                            self.add_derive_value(name,
                                sample.name,
                                sample.value)
        if not hasMetrics:
            LOGGER.debug('Metrics output: %r', raw_metrics)
