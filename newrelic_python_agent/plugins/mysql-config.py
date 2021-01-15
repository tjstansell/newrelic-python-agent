"""
MySQLConfig Plugin

This is used to dynamically configure a MySQL plugin.  It's main purpose is to query
AWS to find all RDS instances in a particular region.  You can select which instances
to monitor by including and excluding based on the instance name or by tags.

All boto3 exceptions are fatal and will cause the config script to exit.  Therefore, if an credstash or cloudformation
should not be queried, make sure those settings are not set so it knows not to even try.

Some settings can be overridden by environment variables:

    RDS_REGIONS
        if defined, this will override any `regions` defined in the config.  Can be separated by
        comma, colon, semi-colon, or whitespace. (e.g. "us-west-1, us-west-2;us-east-1 : us-east-2")

    CLOUDFORMATION_HOSTED_ZONE_EXPORT_NAME
        overrides `cloudformation_hosted_zone_export_name` in all region-specific settings

    AWS_ACCOUNT_NAME
        overrides `aws_account_name` setting.

    AWS_ACCOUNT_ID
        overrides `aws_account_id` setting.

The following settings are supported:

    `name`: A descriptive name of this config block
    `newrelic_name_format`: A format string for generating the newrelic name.
                            {account_name} specifies the `aws_account_name` setting.
                            {account_id} specifies the `aws_account_id` setting.
                            {account} will use `{account_name}` if defined, otherwise `{account_id}`
                            {dbname} refers to the DBInstanceIdentifier property of the RDS instance.
                            {region} is the region where the instance resides.
        type: string
        default: '{dbname} ({account}:{region})'
    `target_plugin_name`: The application name that the resulting config block should be assigned to.
                          If you have a static 'mysql' block already configured, you might want the
                          dynamic one generated here assigned as 'mysql:RDS' instead.
        type: string
        default: 'mysql'
    `refresh_interval`: How often this module should run (seconds)
        type: integer
        default: 0 (run every time)
    `aws_account_name`: The name of the AWS account.  Used by `newrelic_name_format`.
        type: string
        env: `AWS_ACCOUNT_NAME` takes precedence.
        default: None
    `aws_account_id`: The AWS account ID.  Used by `newrelic_name_format`.
        type: string
        env: `AWS_ACCOUNT_ID` takes precedence.
        default: Query the EC2 metadata
    `regions`: The list of regions to query for RDS instances
        type: comma, space, colon, semi-colon separated string or list of region names
        env: `RDS_REGIONS` environment variable takes precedence over any config entries
        default: the current region if on an EC2 instance, or the current boto3 session region
    `settings`:
        A dictionary containing region-specific settings, keyed by region name or `default`.
        If a setting is defined in a region-specific sub-section, it's used, otherwise the
        setting from the `default` section is used.

        The following region-specific settings are supported:

            Used only by `targets` entries to create fully qualified domain names, if required:

                `domain`: The domain name for this region.
                `cloudformation_hosted_zone_export_name`: A cloudformation export name
                    If `domain` is not specified, then query cloudformation exports for an export by this name
                    and use that as the domain name.

            To populate the MySQL config parameters for each instance in a region:

                `user`: The username to use in the `user` field of the MySQL config.
                `password`: The password to use in the `password` field of the MySQL config.

            If either `user` or `password` is missing, and `credstash_table` is defined for the region, then we will
            attempt to look up the values from credstash using the following settings (note that each region queries
            credstash in that specific region):

                `credstash_table`: The table in credstash to query.
                `credstash_user_key`: The key in the credstash table to get the `user` value.
                `credstash_password_key`: The key in the credstash table to get the `password` value.

            The following settings allow you to filter which RDS instances to monitor:

                `include`: Only include RDS instances who match this set of tests
                `exclude`: Exclude any RDS intance that matches this set of tests

                Note: see `is_match()` for details on the supported format.

        Any other setting defined in a specific region or in the `default` section will be assumed to
        be a config parameter to pass along.  For instance, `metrics` can be specified to list which metrics
        to collect.  Or, `connect_timeout` can be used to override the default `connect_timeout` in the MySQL
        plugin.  These settings will be applied to each instance config related to the region, including the
        ones in `targets`.

    `targets`:
        A way to define custom targets that are not otherwise found by querying RDS.  This can be useful to reference an
        RDS by it's public name instead of instance name so as it moves or new instances are created, the historical
        data remains consistent.

        This is a list of targets, which can be in the form:

            - a string, which is treated as the `name`
            - a dictionary with the following attributes:
                `name`: The `name` in the generated mysql config with be this with ` (manual)` appended.
                    e.g. `db1` as a name will result in a config name of `db1 (manual)`
                `host`: The hostname to connect to.  If not defined, then use `name`.  If `name` is not a
                        fully-qualified domain name, then determined the domain from the `region` specified.
                `region`: The region the domain name and/or credstash values should come from.  If not
                          specified and we need these details, the first region in `regions` will be used.
                `user`: The username to connect to the db.
                `password`: The password to connect to the db.
                    Note: if either `user` or `password` are not specified, then the settings for the `region`
                    will be used.  This might include pulling the credstash values for that region.

                Note: Any other entries will be included in the generated config without modification.
                      (e.g. `metrics`, `connect_timeout`, `database`, etc.)  Settings defined here will be
                      applied after any region-specific raw config settings, so these will take precedence.

        Examples:

        ```
        regions:
          - us-east-1
          - us-west-2
        targets:
          - db1
        ```

        will produce a config of:

        ```
        - name: db1 (manual)
          host: db1.domain.com
          user: <username defined by first region (us-east-1)>
          password: <password defined by first region (us-east-1)>
        ```

        ```
        targets:
          - name: db1
            host: db1.my.domain.com
            region: us-west-1
            user: myuser
        ```

        will produce a config of:

        ```
        - name: db1 (manual)
          host: db1.my.domain.com
          user: myuser
          password: <password defined by `us-west-1`>
        ```


Example:

MySQLConfig:
  - name: RDS
    refresh_interval: 300
    target_plugin_name: mysql:RDS
    regions:
      - us-west-1
      - us-west-2
    settings:
        default:
            cloudformation_hosted_zone_export_name: Default-HostedZoneName
            credstash_table: newrelic_monitor
            credstash_user_key: newrelic_monitor_username
            credstash_password_key: newrelic_monitor_password
            exclude: test
            include:
              - tag: newrelic-monitor
                values: "yes"
        us-west-2:
            domain: mycompany.com
            user: monitor
            password: monitorpass
            include:
              - prod
              - tag: newrelic-monitor
                values:
                  - "yes"
                  - "true"


"""

import logging
import re
import os
import json
import credstash
import boto3
from botocore.exceptions import ClientError
from newrelic_python_agent.plugins import base
try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen

LOGGER = logging.getLogger(__name__)


class MySQLConfig(base.ConfigPlugin):

    # region settings that we handle.  Any other keys will be passed through directly.
    # these are under either the settings.$region or settings.default configs

    DEFAULT_NAME_FORMAT = '{dbname} ({account}:{region})'
    SETTINGS_KEYS = ['user', 'password', 'include', 'exclude', 'host', 'name', 'domain',
                     'cloudformation_hosted_zone_export_name', 'region', 'credstash_table',
                     'credstash_user_key', 'credstash_password_key']

    def initialize(self):
        """Initialize ourselves, preparing the required variables."""

        self.init_vars()
        self.init_from_env()
        self.init_verify_vars()
        self.init_defaults()

    def init_vars(self):
        self.rds_cache = dict()
        self.creds_cache = dict()
        self.exports_cache = dict()
        self.tags_cache = dict()

    def init_from_env(self):
        # This requires some manipulation, so handled separately.
        r = self.get_region_from_environment()
        if r:
            LOGGER.info('using regions from environment: %s' % r)
            self.config['regions'] = r

        # these are supported straight overrides
        for i in ['aws_account_id', 'aws_account_name']:
            r = os.getenv(i.upper())
            if r:
                LOGGER.info('using %s from environment: %s' % (i, r))
                self.config[i] = r

    def init_verify_vars(self):
        # we expect/want these to be lists, but support
        # a single string (with comma separated items) for convenience
        # or just a single dict (for targets)
        for prop in ['regions', 'targets']:
            if prop in self.config:
                if isinstance(self.config[prop], str):
                    self.config[prop] = self.string_to_list(self.config[prop])
                elif isinstance(self.config[prop], dict):
                    self.config[prop] = [self.config[prop]]

        if 'newrelic_name_format' in self.config:
            # make sure this is valid and doesn't cause an exception
            try:
                self.format_newrelic_name('testname', 'testregion')
            except KeyError as e:
                raise Exception("newrelic_name_format is invalid! invalid key %s specified!" % e)

    def init_defaults(self):
        # default to only the current region if none are specified.
        if 'regions' not in self.config:
            LOGGER.info('setting regions to default')
            self.config['regions'] = self.get_default_region()

        # default account name is the accountid in the EC2 data
        if 'aws_account_id' not in self.config:
            LOGGER.info('setting aws_account_id to default')
            self.config['aws_account_id'] = self.get_value_from_metadata('accountId')

        if 'newrelic_name_format' not in self.config:
            LOGGER.info('setting newrelic_name_format to default')
            self.config['newrelic_name_format'] = self.DEFAULT_NAME_FORMAT

    def get_default_region(self):
        """
        Auto-determine the local region, by either the EC2 metadata or the default session.

        :return: The AWS region name
        :rtype: str
        """
        return self.get_region_from_metadata() or self.get_region_from_session()

    def get_region_from_environment(self):
        """
        If `RDS_REGIONS` is defined in the environment, then use that as a list of regions to query.

            us-west1:us-west2

        :return: list of regions
        """
        r = os.getenv('RDS_REGIONS')
        if r:
            return self.string_to_list(r)

    def get_value_from_metadata(self, value):
        """
        Query the EC2 metadata for the local region.

        :param str value: The name of the value to return
        :return: The AWS region the EC2 instance is running.
        :rtype: str or None
        """
        LOGGER.info('Obtaining %s from EC2 metadata.' % value)
        try:
            url = 'http://169.254.169.254/latest/dynamic/instance-identity/document'
            document = json.loads(urlopen(url, timeout=3).read())
            return [document[value]]
        except Exception as e:
            LOGGER.warning("failed to query EC2 metadata: %s", e)
            pass

    def get_region_from_metadata(self):
        """
        Query the EC2 metadata for the local region.

        :return: The AWS region the EC2 instance is running.
        :rtype: str or None
        """
        return self.get_value_from_metadata('region')

    def get_region_from_session(self):
        """
        Query the region as defined by the default session.  Useful for testing and running by hand outside of EC2.

        :return: The AWS region of the default session.
        :rtype: str
        """
        LOGGER.info('Obtaining region from default session.')
        return [boto3.session.Session().region_name]

    def string_to_list(self, string):
        """
        Split a string on any combination of: comma, space, semi-colon, colon

        :param str string: The string to split
        :return: a list of strings
        """
        return re.split("\s*[;:, ]+\s*", string)

    def build_config(self):
        """
        Build a full config object for the plugin defined by the `target_plugin_name` config variable.
        This prepares the `state` which will be used as the return value.

        This is the entry point of a ConfigPlugin object.

        :return: None
        """

        plugin = self.config.get('target_plugin_name', 'mysql')
        if not plugin:
            LOGGER.error("must specify 'target_plugin_name' config value")
            return

        try:
            LOGGER.info("initializing '%s'", plugin)
            self.initialize()
        except Exception as e:
            LOGGER.error(e)
            return

        try:
            LOGGER.info("building config for '%s'", plugin)
            instances = self.get_all_rds_instances()
            instances.extend(self.get_manual_instances())
            LOGGER.info("found %s database instances to monitor", len(instances))
            self.add_config_block(plugin, instances)
        except ClientError as e:
            # all boto3 exceptions are simply logged here, but are fatal
            LOGGER.error(e)
        finally:
            LOGGER.info("Exiting with %d application results", len(self.state['application']))

    def cache_cloudformation_exports(self, region):
        """
        Query cloudformation exports for a region and cache them for subsequent queries

        :param str region: The name of the AWS region this applies to.
        :return: None
        """

        LOGGER.info("querying cloudformation exports for %s", region)
        c = boto3.client('cloudformation', region_name=region)
        more = True
        args = dict()
        self.exports_cache[region] = dict()
        while more:
            result = c.list_exports(**args)
            for e in result['Exports']:
                self.exports_cache[region][e['Name']] = e['Value']
            if 'NextToken' in result and result['NextToken'] is not None:
                args['NextToken'] = result['NextToken']
                more = True
            else:
                more = False

    def get_hosted_zonename(self, region):
        """
        Determine the hosted zone name from a cloudformation export name for a particular region

        :param str region: The name of the region to query.
        :return: The domain name of the region.
        :rtype: str
        """
        export_name = os.getenv('CLOUDFORMATION_HOSTED_ZONE_EXPORT_NAME',
                                self.get_region_setting(region, "cloudformation_hosted_zone_export_name"))
        if not region:
            region = self.config['regions'][0]
        if region not in self.exports_cache:
            self.cache_cloudformation_exports(region)
        return self.exports_cache[region].get(export_name)

    def get_fqdn(self, name, region):
        """
        Convert `name` to a fully-qualified domain name, as determined by the region specified.

        :param str name: The name to convert to a fully-qualified name.
        :param str region: The name of the AWS region this applies to.
        :return: The fully-qualified domain name.
        :rtype: str
        """
        if '.' in name:
            # assumed to already be an fqdn
            return name

        domain = self.get_region_setting(region, "domain") or self.get_hosted_zonename(region)
        if domain:
            return "%s.%s" % (name, domain)
        return name

    def get_credentials(self, region):
        """
        Determine the credentials to use to connect to an RDS instance in an AWS region.
        If credstash is configured for the region, then query that for the info.  Otherwise, use
        the user and password config fields.  The result is cached so we only have to query
        credstash once per instance run.

        :param str region: The name of the AWS region
        :return: user and password
        :rtype: list
        """
        # cache the credentials so we only query once
        if region not in self.creds_cache:
            LOGGER.info("querying credentials for region %s", region)
            # these will throw an exception if credstash is configured but cannot be queried,
            # otherwise, they will simply return None
            u = self.get_credstash_username(region)
            p = self.get_credstash_password(region)

            # if either of these are not found, try to pull them from the settings
            if not u:
                u = self.get_region_setting(region, "user")
            if not p:
                p = self.get_region_setting(region, "password")

            # cache the response for this region
            self.creds_cache[region] = [u, p]

        return self.creds_cache[region]

    def get_credstash_username(self, region):
        """
        Query credstash for the username to use to connect to the RDS instance.

        :param str region: The name of the AWS region to query credstash.
        :return: The username as specified in credstash
        :rtype: str
        """
        key = self.get_region_setting(region, "credstash_user_key")
        if key:
            return self.query_credstash(key, region)

    def get_credstash_password(self, region):
        """
        Query credstash for the password to use to connect to the RDS instance.

        :param str region: The name of the AWS region to query credstash.
        :return: The password as specified in credstash
        :rtype: str
        """
        key = self.get_region_setting(region, "credstash_password_key")
        if key:
            return self.query_credstash(key, region)

    def query_credstash(self, key, region):
        """
        Query credstash for a specific key in the credstash table as specified by
        the `credstash_table` config setting.  Returns None if no table is configured.

        :param str key: The key in the credstash table to query.
        :param str region: The name of the AWS region to query credstash.
        :return: The value of the credstash key.
        :rtype: str or None
        """
        table = self.get_region_setting(region, "credstash_table")
        if table:
            r = credstash.getSecret(key,
                                    region=region,
                                    table=table)
            return r
        return None

    def get_region_setting(self, region, name):
        """
        Convenience function to pull a config value from a region-specific section first, otherwise
        from the default section.

        :param str region: the name of the region
        :param str name: the name of the setting to look for
        """
        return self.get_config_value(["settings.%s.%s" % (region, name), "settings.default.%s" % name])

    def get_config_value(self, name, default=None):
        """
        Convenience function to pull a dot-separated name from the config
        without having to worry about if each level in the heirarchy exists.

        :param str name: a dot-separated config variable
        :param list name: a list of dot-separated config variables to search for.  return first match
        :param str default: the default result to return if no matches are found.
        """

        # support a list of config values to search for, returning the first match
        if isinstance(name, list):
            for n in name:
                r = self.get_config_value(n)
                if r is not None:
                    return r
            return default

        v = self.config
        for i in name.split('.'):
            if i not in v:
                return default
            v = v[i]
        return v

    def get_all_rds_instances(self):
        """
        Get all RDS instances to monitor in all regions defined by the `regions` setting.

        :return: A list of instance configs
        :rtype: list
        """
        instances = list()
        for region in self.config['regions']:
            instances.extend(self.get_rds_region_instances(region))
        return instances

    def check_instance_tags(self, client, arn, test):
        """
        Compare the tags on an RDS instance with the name/values in our test.

        :param botocore.client.RDS client: The RDS boto3 client instance to use for the query.
        :param str arn: The ARN of the instance to query.
        :param dict test: A tag to look for, specified as a dictionary with 'tag' as the tag name
                          and 'values' as a list of possible values (any match will succeed).
        :return: True if a tag key/value pair finds a match on the RDS instance, False otherwise.
        :rtype: bool
        """

        LOGGER.debug("checking tags for '%s'", arn)

        # check for an invalid test format
        if not (isinstance(test, dict) and 'tag' in test and 'values' in test):
            return False

        # cache this arn's tags for future tests
        if arn not in self.tags_cache:
            self.tags_cache[arn] = client.list_tags_for_resource(ResourceName=arn)

        res = self.tags_cache.get(arn)
        # LOGGER.debug("result: %s", json.dumps(res))
        if res and 'TagList' in res:
            # TagList: [{'Key': key 'Value': value}]
            for tag in res['TagList']:
                if tag['Key'] == test['tag'] and tag['Value'] in test['values']:
                    return True
        return False

    def get_rds_region_instances(self, region):
        """
        Query a region for a list of instances.  The result will be in the order as returned by
        botocore.client.RDS, filtered as defined by the `include` and `exclude`
        settings for the particular region.

        :param str region: The name of the AWS region.
        :return: A list of instance configs
        :rtype: list
        """
        instances = []
        more = True
        args = {}

        username, password = self.get_credentials(region)
        if not username or not password:
            # if either of these don't exist, we can't continue
            LOGGER.warning("no db credentials defined for '%s' region. not probing RDS instances." % region)
            return instances

        # see if we should be doing any include/exclude regex matches
        include = self.get_region_setting(region, 'include')
        exclude = self.get_region_setting(region, 'exclude')

        c = boto3.client('rds', region_name=region)
        while more:
            LOGGER.debug("querying for db instances in %s with args: %s", region, args)
            result = c.describe_db_instances(**args)
            for instance in result['DBInstances']:
                if instance['Engine'] == "mysql":
                    # include by default
                    good = True
                    endpoint = instance['Endpoint']['Address']

                    if include and not self.is_match(c, instance, include):
                        LOGGER.debug("excluding '%s' because it did not match include pattern of '%s'",
                                     endpoint, self.format_pattern(include))
                        good = False

                    if good and exclude and self.is_match(c, instance, exclude):
                        LOGGER.debug("excluding '%s' because it matches exclude pattern of '%s'",
                                     endpoint, self.format_pattern(exclude))
                        good = False

                    if good:
                        # create a stub instance
                        i = {
                            'name': self.format_newrelic_name(instance['DBInstanceIdentifier'], region),
                            'host': endpoint,
                        }

                        # include these if they are defined
                        if username:
                            i['user'] = username
                        if password:
                            i['password'] = password

                        # include any passthrough settings
                        i.update(self.get_passthrough_settings(region=region))

                        # now append this to the list of instances
                        LOGGER.debug("adding '%s' as monitored instance",
                                     instance['Endpoint']['Address'])
                        instances.append(i)
                else:
                    LOGGER.debug("skipping '%s' with unsupported '%s' engine type",
                                 instance['Endpoint']['Address'],
                                 instance['Engine'])

            # If there are over MaxRecords (default 100), Marker will tell us
            if 'Marker' in result:
                # there are more results, so pull the next set
                args['Marker'] = result['Marker']
                more = True
            else:
                more = False
        return instances

    def get_passthrough_settings(self, region=None, target=None):
        result = dict()

        settings = ['settings.default']
        if region:
            settings.append("settings.%s" % region)

        # include any extra settings defined at the region level
        exclude = set(self.SETTINGS_KEYS)
        for s in settings:
            vals = self.get_config_value(s)
            if vals:
                for k in (set(vals) - exclude):
                    result[k] = vals[k]

        # if a target config is passed in, see if there are any settings here too
        if target and isinstance(target, dict):
            for k in (set(target) - exclude):
                result[k] = target[k]

        return result

    def is_match(self, client, instance, tests, all=False):
        """
        Compare an instance with a set of tests.  The tests are a list of regex or tag comparisons
        to check.  A set of comparisons are grouped as ORs at the top level and as ANDs at a second level.

        A test can be a string, which represents a regular expression to match against the DBInstanceIdentifier
        or Endpoint.Address properties of the instance.

        A test can be a dictionary, with a key of `tag` (specifying the tag name to compare) and `values`
        with a list of possible matching values for that tag (treated as OR).

        [
            test1
            OR [test2 AND test3]
            OR test4
        ]

        :param client: a boto3.client.RDS instance to query RDS
        :param dict instance: An instance dictionary result from boto3.client.RDS.describe_rds_instances()
        :param list tests: A list of tests to check.
        :result: True if the tests pass, False otherwise
        """
        # a single entity (string or dict) is put into list form
        if not isinstance(tests, list):
            tests = [tests]

        # The first list of tests is treated as ORs
        for test in tests:
            match = False
            if isinstance(test, list) \
                    and self.is_match(client, instance, test, all=True):
                match = True
            elif isinstance(test, str) \
                    and (re.search(test, instance['DBInstanceIdentifier']) or
                         re.search(test, instance['Endpoint']['Address'])):
                match = True
            elif isinstance(test, dict) \
                    and self.check_instance_tags(client, instance['DBInstanceArn'], test):
                match = True

            # if we are matching any and there's a match, short circuit
            if not all and match:
                return True

            # if we are matching all and there's not a match, short circuit
            if all and not match:
                return False

        # return True if we were matching all (and didn't short-circuit earlier)
        # return False if we were matching any (and didn't short-circuit earlier)
        return all

    def format_pattern(self, pattern):
        """
        Convert the include/exclude pattern to a human readable form that shows the logic more clearly.
            - a
            - - b
              - c

            is converted to:

            a OR (b AND c)

        :result: a human readable representation of the test patterns
        """
        if not isinstance(pattern, list):
            pattern = [pattern]

        ors = []
        for p in pattern:
            if isinstance(p, list):
                ands = [str(i) for i in p]
                p = "(%s)" % " AND ".join(ands)
            ors.append(str(p))
        return " OR ".join(ors)

    def format_newrelic_name(self, name, region):
        f = self.get_config_value('newrelic_name_format')
        account_id = self.get_config_value('aws_account_id') or ''
        account_name = self.get_config_value('aws_account_name') or ''
        account = account_name or account_id
        desc = f.format(dbname=name,
                        account_id=account_id,
                        account_name=account_name,
                        account=account,
                        region=region)
        return desc

    def get_manual_instances(self):
        """
        Build a list of instance configs from manually-specified names.  This
        might be useful to reference a common name rather than an instance-specific one
        so that the metric data follows the name as instances change.

        Supported formats for targets:

        targets:
          - db1                         (if just a string, then equivalent to specifying `name`)
          - name: db2                   (uses default domain as defined by settings for first region)
          - name: db3
            host: db3.foo.com
            region: us-west-2           (use credentials as defined by this region)
          - name: db4
            user: newrelic
            password: somepassword

        :return: A list of MySQL instance configs
        :rtype: list
        """
        instances = []
        targets = self.config.get('targets', [])
        for target in targets:
            if isinstance(target, str):
                target = {'name': target}
            if isinstance(target, dict):
                # required setting
                name = target['name']

                # optional settings
                user = target.get('user')
                password = target.get('password')
                # the region to use when determining the credentials and domain name
                # if those are not already specified
                region = target.get('region', self.config['regions'][0])

                # use the region credentials if not specified directly
                if not user or not password:
                    u, p = self.get_credentials(region)
                    if not user:
                        user = u
                    if not password:
                        password = p

                # now build the instance config
                cf = {
                    'name': self.format_newrelic_name("%s (manual)" % name, ''),
                    'host': target.get('host', self.get_fqdn(name, region)),
                }
                # only include these if they are defined
                if user:
                    cf['user'] = user
                if password:
                    cf['password'] = password

                # now include any "extra" fields given in region or this target
                cf.update(self.get_passthrough_settings(region=region, target=target))

                instances.append(cf)
        return instances
