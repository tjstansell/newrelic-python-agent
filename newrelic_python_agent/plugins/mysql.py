"""MySQL Plugin

This is designed to be a drop-in replacement for the newrelic java mysql plugin found at
https://github.com/newrelic-platform/newrelic_mysql_java_plugin

Like all other plugins, the config can be a dict, which will represent the config for a single instance.
It can also be a list of instances, each with it's own set of settings.  The possible settings are:

    name: A descriptive name for this instance, which is used as the name reported to newrelic.
        type: string
        default: None
    metrics: The set of metric categories to collect
        type: string or list(string)
        default: "status,newrelic"
        allowed values:
            - "all" -- this will select all categories
            - a comma-separated string of categories (e.g. status,newrelic,master,innodb_metrics)
            - list of categories

The following settings are passed to sql.connect():

    host: The hostname or IP to connect to.
        type: string
        default: localhost
    port: The port to connect to the database.
        type: integer
        default: 3306
    user: The username to connect to mysql as.
        type: string
        default: newrelic
    password: The password to connect to mysql as.
        type: string
        default: f63c225f4abe9e13
    database: Database to use, None to not use a particular one.
        type: string
        default: None
    connect_timeout: The timeout when connecting to the database (seconds).
        type: integer
        default: 5

Any other settings specified will be passed directly to connect() (from pymsql or mysql.connect)

Example:
    mysql:
      - name: localhost
        host: localhost
        user: monitor
        password: monitor
        metrics: status,newrelic,master,innodb_status
      - name: db1 (master)
        host: db1
        port: 5432
        metrics: all
      - name: db1 (slave)
        host: db1-slave
        metrics:
          - status
          - newrelic
          - slave
          - master
          - innodb_status
          - innodb_mutex
          - innodb_metrics

"""

import six
import re
import time
import logging

# Support both PyMySQL and mysql-connector-python interfaces
try:
    import pymysql as sql

    def _errno(err):
        """Extract error code from pymysql.Error"""
        return err.args[0]
    from pymysql.constants.ER import ACCESS_DENIED_ERROR as ER_ACCESS_DENIED_ERROR, BAD_DB_ERROR as ER_BAD_DB_ERROR
except ImportError:
    import mysql.connector as sql

    def _errno(err):
        """Extract error code from mysql.connector.Error"""
        return err.errno
    from mysql.connector.errorcode import ER_ACCESS_DENIED_ERROR, ER_BAD_DB_ERROR

from newrelic_python_agent.plugins import base

LOGGER = logging.getLogger(__name__)

DEFAULT_UNIT = "Operations"
DEFAULT_TYPE = "gauge"
DEFAULT_METRICS = ['status', 'newrelic']
DEFAULT_CONNECT_ARGS = {
    "port": 3306,
    "database": None,
    "host": "localhost",
    "user": "newrelic",
    "password": "f63c225f4abe9e13",
    "connect_timeout": 5
}

#
# These are the available categories that can be queried, as specified
# in the `metrics` config setting.
#
CATEGORIES = {
    "newrelic": {
        # this is derived from other metrics, so there is no SQL
    },
    "status": {
        "SQL": "SHOW GLOBAL STATUS",
        "parser": "set",
    },
    "slave": {
        "SQL": "SHOW SLAVE STATUS",
        "parser": "row",
    },
    "master": {
        "SQL": "SHOW MASTER STATUS",
        "parser": "row",
    },
    "buffer_pool_stats": {
        "SQL": "SELECT * FROM information_schema.innodb_buffer_pool_stats",
        "parser": "row",
        "comment": "MySQL 5.5 or later",
    },
    "innodb_metrics": {
        "SQL": "SELECT name, count FROM information_schema.innodb_metrics",
        "parser": "set",
        "comment": "MySQL 5.6 or later",
    },
    "innodb_mutex": {
        "SQL": "SHOW ENGINE INNODB MUTEX",
        "parser": "innodb_mutex",
    },
    "innodb_status": {
        "SQL": "SHOW ENGINE INNODB STATUS",
        "parser": "innodb_status",
    }
}
#
# META defines the type and category for each metric, optionally overriding the units
#
META = {
    "gauge": {
        "status": [
            "innodb_page_size",
            "innodb_data_pending_fsyncs",
            "max_used_connections",
            "open_files",
            "open_streams",
            "open_table_definitions",
            "open_tables",
            ["qcache_free_blocks", "Blocks"],
            ["qcache_free_memory", "Bytes"],
            ["qcache_queries_in_cache", "Queries"],
            ["qcache_total_blocks", "Blocks"],
            "threads_cached",
            "threads_connected",
            "threads_created",
            "threads_running",
            "uptime"
        ],
        "slave": [
            "read_master_log_pos",
            "slave_io_running",
            "slave_sql_running",
            "exec_master_log_pos",
            "relay_log_pos",
            "relay_log_size",
            "seconds_behind_master",
            "last_errno"
        ],
        "newrelic": [
            ["connections_connected", "Connections"],
            ["connections_running", "Connections"],
            ["connections_cached", "Connections"],
            ["replication_lag", "Seconds"],
            ["replication_status", "State"],
            ["pct_connection_utilization", "Percent"],
            ["pct_innodb_buffer_pool_hit_ratio", "Percent"],
            ["pct_query_cache_hit_utilization", "Percent"],
            ["pct_query_cache_memory_in_use", "Percent"],
            ["pct_tmp_tables_written_to_disk", "Percent"],
            ["innodb_buffer_pool_pages_clean", "Pages"],
            ["innodb_buffer_pool_pages_dirty", "Pages"],
            ["innodb_buffer_pool_pages_misc", "Pages"],
            ["innodb_buffer_pool_pages_free", "Pages"],
            ["innodb_buffer_pool_pages_unassigned", "Pages"]
        ],
        "innodb_status": [
            ["history_list_length", "Pages"],
            ["queries_inside_innodb", "Queries"],
            ["queries_in_queue", "Queries"],
            ["checkpoint_age", "Bytes"],
        ]
    },
    "counter": {
        "status": [
            ["aborted_clients", "Connections"],
            ["aborted_connects", "Connections"],
            ["bytes_received", "Bytes"],
            ["bytes_sent", "Bytes"],
            "com_admin_commands",
            "com_assign_to_keycache",
            "com_alter_db",
            "com_alter_db_upgrade",
            "com_alter_event",
            "com_alter_function",
            "com_alter_procedure",
            "com_alter_server",
            "com_alter_table",
            "com_alter_tablespace",
            "com_alter_user",
            "com_analyze",
            "com_begin",
            "com_binlog",
            "com_call_procedure",
            "com_change_db",
            "com_change_master",
            "com_check",
            "com_checksum",
            "com_commit",
            "com_create_db",
            "com_create_event",
            "com_create_function",
            "com_create_index",
            "com_create_procedure",
            "com_create_server",
            "com_create_table",
            "com_create_trigger",
            "com_create_udf",
            "com_create_user",
            "com_create_view",
            "com_dealloc_sql",
            ["com_delete", "Deletes"],
            ["com_delete_multi", "Deletes"],
            "com_do",
            "com_drop_db",
            "com_drop_event",
            "com_drop_function",
            "com_drop_index",
            "com_drop_procedure",
            "com_drop_server",
            "com_drop_table",
            "com_drop_trigger",
            "com_drop_user",
            "com_drop_view",
            "com_empty_query",
            "com_execute_sql",
            "com_flush",
            "com_grant",
            "com_ha_close",
            "com_ha_open",
            "com_ha_read",
            "com_help",
            ["com_insert", "Inserts"],
            ["com_insert_select", "Inserts"],
            "com_install_plugin",
            "com_kill",
            "com_load",
            "com_lock_tables",
            "com_optimize",
            "com_preload_keys",
            "com_prepare_sql",
            "com_purge",
            "com_purge_before_date",
            "com_release_savepoint",
            "com_rename_table",
            "com_rename_user",
            "com_repair",
            ["com_replace", "Replaces"],
            ["com_replace_select", "Replaces"],
            "com_reset",
            "com_resignal",
            "com_revoke",
            "com_revoke_all",
            "com_rollback",
            "com_rollback_to_savepoint",
            "com_savepoint",
            ["com_select", "Selects"],
            "com_set_option",
            "com_signal",
            "com_show_authors",
            "com_show_binlog_events",
            "com_show_binlogs",
            "com_show_charsets",
            "com_show_collations",
            "com_show_contributors",
            "com_show_create_db",
            "com_show_create_event",
            "com_show_create_func",
            "com_show_create_proc",
            "com_show_create_table",
            "com_show_create_trigger",
            "com_show_databases",
            "com_show_engine_logs",
            "com_show_engine_mutex",
            "com_show_engine_status",
            "com_show_events",
            "com_show_errors",
            "com_show_fields",
            "com_show_function_status",
            "com_show_grants",
            "com_show_keys",
            "com_show_master_status",
            "com_show_open_tables",
            "com_show_plugins",
            "com_show_privileges",
            "com_show_procedure_status",
            "com_show_processlist",
            "com_show_profile",
            "com_show_profiles",
            "com_show_relaylog_events",
            "com_show_slave_hosts",
            "com_show_slave_status",
            "com_show_status",
            "com_show_storage_engines",
            "com_show_table_status",
            "com_show_tables",
            "com_show_triggers",
            "com_show_variables",
            "com_show_warnings",
            "com_slave_start",
            "com_slave_stop",
            "com_stmt_close",
            "com_stmt_execute",
            "com_stmt_fetch",
            "com_stmt_prepare",
            "com_stmt_reprepare",
            "com_stmt_reset",
            "com_stmt_send_long_data",
            "com_truncate",
            "com_uninstall_plugin",
            "com_unlock_tables",
            ["com_update", "Updates"],
            ["com_update_multi", "Updates"],
            "com_xa_commit",
            "com_xa_end",
            "com_xa_prepare",
            "com_xa_recover",
            "com_xa_rollback",
            "com_xa_start",
            ["created_tmp_disk_tables", "Creates"],
            ["created_tmp_files", "Creates"],
            ["created_tmp_tables", "Creates"],
            ["innodb_buffer_pool_pages_flushed", "Pages"],
            "innodb_buffer_pool_read_ahead_rnd",
            "innodb_buffer_pool_read_ahead",
            "innodb_buffer_pool_read_ahead_evicted",
            "innodb_buffer_pool_read_requests",
            "innodb_buffer_pool_reads",
            "innodb_buffer_pool_wait_free",
            "innodb_buffer_pool_write_requests",
            ["innodb_data_fsyncs", "Fsyncs"],
            "innodb_data_read",
            "innodb_data_reads",
            "innodb_data_writes",
            "innodb_data_written",
            ["innodb_os_log_fsyncs", "Fsyncs"],
            ["innodb_os_log_written", "Bytes"],
            "innodb_pages_created",
            "innodb_pages_read",
            "innodb_pages_written",
            "innodb_rows_deleted",
            "innodb_rows_inserted",
            "innodb_rows_read",
            "innodb_rows_updated",
            "max_used_connections",
            "opened_files",
            "opened_table_definitions",
            "opened_tables",
            ["qcache_hits", "Queries"],
            ["qcache_inserts", "Queries"],
            ["qcache_lowmem_prunes", "Queries"],
            ["qcache_not_cached", "Queries"],
            "select_full_join",
            "select_full_range_join",
            "select_range",
            "select_range_check",
            "select_scan",
            ["slow_queries", "Queries"],
            "sort_merge_passes",
            "sort_range",
            "sort_rows",
            "sort_scan",
            "table_locks_immediate",
            "table_locks_waited",
            "threads_cached",
            "threads_connected",
            "threads_created",
            "threads_running"
        ],
        "master": [
            ["position", "Bytes"]
        ],
        "slave": [
            ["relay_log_pos", "Bytes"]
        ],
        "newrelic": [
            ["bytes_reads", "Bytes"],
            ["bytes_writes", "Bytes"],
            ["innodb_bp_pages_created", "Pages"],
            ["innodb_bp_pages_read", "Pages"],
            ["innodb_bp_pages_written", "Pages"],
            ["innodb_fsyncs_data", "Fsyncs"],
            ["innodb_fsyncs_os_log", "Fsyncs"],
            ["master_log_lag_bytes", "Bytes"],
            ["query_cache_hits", "Queries"],
            ["query_cache_misses", "Queries"],
            ["query_cache_not_cached", "Queries"],
            ["slave_relay_log_bytes", "Bytes"],
            ["volume_reads", "Queries"],
            ["volume_writes", "Queries"]
        ]
    }
}


class MySQL(base.Plugin):

    # pretend to be the official mysql plugin
    GUID = 'com.newrelic.plugins.mysql.instance'

    is_true = re.compile("^(on|yes|true)$", re.I)
    is_false = re.compile("^(off|no|false)$", re.I)
    is_null = re.compile("^null$", re.I)
    has_slave_data = False
    raw_metrics = dict()

    #
    # Track the server uuid just to verify who we are talking to.
    #
    # This was used to help debug some threading issues where connections were going
    # to the wrong database instance.  Left it here, but unused, because it seemed useful.
    #
    def verify_uuid(self, cursor):
        cursor.execute("show variables where variable_name like 'server%'")
        server = self.parse_set_stats(cursor)
        if 'server_uuid' in self.derive_last_interval.keys():
            if server['server_uuid'] == self.derive_last_interval['server_uuid']:
                self.logger.debug("server_uuid matches %s", server['server_uuid'])
            else:
                raise ValueError("server_uuid (%s) does not match previous value of %s" %
                                 (server['server_uuid'], self.derive_last_interval['server_uuid']))
        else:
            self.derive_last_interval['server_uuid'] = server['server_uuid']
            self.logger.info("server_uuid is %s", server['server_uuid'])

    def collect_stats(self, cursor):
        """
        Loop through each of the requested metrics and collect the data.

        :param sql.connection.cursor cursor: The SQL cursor to query the db
        :return: None
        """
        metrics = self.config.get('metrics', DEFAULT_METRICS)
        if isinstance(metrics, str):
            if metrics == "all":
                # puffer_pool_status is only for 5.5, so we ignore that by default
                metrics = CATEGORIES.keys()
                metrics.remove('buffer_pool_stats')
            else:
                # support comma-separated list
                metrics = re.split(r"\s*,\s*", metrics)

        self.logger.debug("metrics to collect: %s" % ", ".join(metrics))
        for cat in metrics:
            if cat in CATEGORIES:
                self.add_category_stats(cat, cursor)
            else:
                self.logger.warning("%s is not a valid metric category" % cat)

        if 'newrelic' in metrics:
            self.derive_newrelic_stats()

    def add_category_stats(self, category, cursor):
        """
        Collect all of the stats for this metric based on it's SQL query.
        This updates the raw_metrics with any results.

        :param str category: The name of the metric category.
        :param sql.connection.cursor cursor: The SQL cursor to perform the query.
        :return: Nothing
        """

        conf = CATEGORIES[category]
        if 'SQL' not in conf:
            return

        self.logger.debug("Collecting stats for %s" % category)
        cursor.execute(conf['SQL'])

        # call the self.parse_"parser"_stats" function for each one to get the raw key/value pairs
        results = getattr(self, "parse_%s_stats" % conf['parser'])(cursor)

        # now filter the results to only the things we care about
        for key in results:
            var = key.lower()
            metric_name = "/".join((category, var))

            val = self.parse_metric_value(results.get(key))

            # special case for slave/seconds_behind_master
            if not self.is_number(val) and var == "seconds_behind_master":
                val = -1.0

            self.update_metric(metric_name, val)

        if category == "slave":
            # log that we've processed slave data, if we have, so we know if
            # we should expect values in derive_newrelic_slaves()
            self.has_slave_data = True

    def add_stats(self):
        """
        Walk the raw_metrics and pass them over to the agent as a counter or gauge
        type and with the correct units defined
        """
        units = self.get_unit_map()
        for metric in self.raw_metrics:
            unit, metric_type = units.get(metric, (DEFAULT_UNIT, DEFAULT_TYPE))
            if metric_type == "counter":
                # Unit/Second
                unit = "/".join((unit, "Second"))
                self.add_derive_value(metric, unit, self.raw_metrics[metric], rate=True)
            else:
                self.add_gauge_value(metric, unit, self.raw_metrics[metric])

    def get_unit_map(self):
        """
        Walk the META dict and build a category/metric => [unit, type] map

        :return: The dict mapping "category/metric" names to their unit and type
        """
        units = dict()
        for t in META:
            for c in META[t]:
                for i in META[t][c]:
                    unit = DEFAULT_UNIT
                    if (isinstance(i, (tuple, list))):
                        val, unit = i
                    else:
                        val = i
                    # category/metric
                    n = "/".join((c, val))
                    units[n] = (unit, t)
        return units

    def get_values(self, names):
        """
        Given a list of names, return the values collected for those names as a list.
        If any are missing, then return None.

        :param list names: List of collected metrics to fetch
        :return: list of collected values or None
        """
        r = []
        for n in names:
            if n in self.raw_metrics:
                r.append(self.raw_metrics[n])
            else:
                return None
        return r

    def sum_of(self, names):
        """
        Given a list of metric names, return the sum of their values if all of
        them exist in the raw metrics, otherwise return None.

        :param list names: A list of metric names
        :return: the sum of the values (a + b + c + d ...)
        """
        vals = self.get_values(names)
        if vals is None:
            return None
        return sum(vals)

    def diff_of(self, names):
        """
        Given a list of metric names, return the result of the first subtracted by
        all others (a - b - c - d ...).  If any metric names do not exist, return None.

        :param list names: A list of metric names
        :return: the resulting value (a - b - c - d ...)
        """
        vals = self.get_values(names)
        if vals is None:
            return None
        return vals[0] - (sum(vals[1:]))

    def update_metric(self, metric, value):
        """
        Update the raw metrics for a particular metric name if the value is a number.

        :param str metric: The name of the metric
        :param float value: The value of the metric
        """
        if self.is_number(value):
            self.logger.debug("Collected raw metric: %s = %s" % (metric, value))
            self.raw_metrics[metric] = value

    def derive_newrelic_stats(self):
        """
        Derive all of the custom newrelic metric data from what we've collected.
        """
        self.logger.debug("Collecting stats for newrelic")
        self.derive_newrelic_volume()
        self.derive_newrelic_throughput()
        self.derive_newrelic_innodb()
        self.derive_newrelic_qcache()
        self.derive_newrelic_slaves()

    def derive_newrelic_volume(self):
        """
        Derive the newrelic read/write volume metrics
        """
        # read and write volume
        self.update_metric("newrelic/volume_reads", self.sum_of(["status/com_select", "status/qcache_hits"]))
        self.update_metric("newrelic/volume_writes", self.sum_of(["status/com_insert", "status/com_insert_select",
                                                                  "status/com_update", "status/com_update_multi",
                                                                  "status/com_delete", "status/com_delete_multi",
                                                                  "status/com_replace", "status/com_replace_select"]))

    def derive_newrelic_throughput(self):
        """
        Derive the newrelic throughput metrics
        """
        # read and write throughput
        self.update_metric("newrelic/bytes_reads", self.sum_of(["status/bytes_sent"]))
        self.update_metric("newrelic/bytes_writes", self.sum_of(["status/bytes_received"]))

        # Connection management
        vals = self.get_values(["status/threads_connected", "status/threads_running", "status/threads_cached"])
        if vals:
            connected, running, cached = vals
            self.update_metric("newrelic/connections_connected", connected)
            self.update_metric("newrelic/connections_running", running)
            self.update_metric("newrelic/connections_cached", cached)
            pct_connection_utilization = 0.0
            if vals[0] > 0:
                pct_connection_utilization = (running / connected) * 100.0
            self.update_metric("newrelic/pct_connection_utilization", pct_connection_utilization)

    def derive_newrelic_innodb(self):
        """
        Derive the newrelic innodb metrics
        """
        # InnoDB Metrics
        vals = self.get_values(["status/innodb_pages_created", "status/innodb_pages_read",
                                "status/innodb_pages_written", "status/innodb_buffer_pool_read_requests",
                                "status/innodb_buffer_pool_reads", "status/innodb_data_fsyncs",
                                "status/innodb_os_log_fsyncs"])
        if vals:
            created, read, written, bp_read_requests, bp_reads, data_fsync, log_fsync = vals
            self.update_metric("newrelic/innodb_bp_pages_created", created)
            self.update_metric("newrelic/innodb_bp_pages_read", read)
            self.update_metric("newrelic/innodb_bp_pages_written", written)

            hit_ratio = 0.0
            if (bp_read_requests + bp_reads) > 0:
                hit_ratio = (bp_read_requests / (bp_read_requests + bp_reads)) * 100.0

            self.update_metric("newrelic/pct_innodb_buffer_pool_hit_ratio", hit_ratio)
            self.update_metric("newrelic/innodb_fsyncs_data", data_fsync)
            self.update_metric("newrelic/innodb_fsyncs_os_log", log_fsync)

        # InnoDB Buffer Metrics
        vals = self.get_values(["status/innodb_buffer_pool_pages_total", "status/innodb_buffer_pool_pages_data",
                                "status/innodb_buffer_pool_pages_misc", "status/innodb_buffer_pool_pages_dirty",
                                "status/innodb_buffer_pool_pages_free"])
        if vals:
            pages_total, pages_data, pages_misc, pages_dirty, pages_free = vals
            unassigned = pages_total - pages_data - pages_free - pages_misc

            self.update_metric("newrelic/innodb_buffer_pool_pages_clean", pages_data - pages_dirty)
            self.update_metric("newrelic/innodb_buffer_pool_pages_dirty", pages_dirty)
            self.update_metric("newrelic/innodb_buffer_pool_pages_misc", pages_misc)
            self.update_metric("newrelic/innodb_buffer_pool_pages_free", pages_free)
            self.update_metric("newrelic/innodb_buffer_pool_pages_unassigned", unassigned)

    def derive_newrelic_qcache(self):
        """
        Derive the newrelic qcache metrics
        """
        # Query Cache
        vals = self.get_values(["status/qcache_hits", "status/com_select", "status/qcache_free_blocks",
                                "status/qcache_total_blocks", "status/qcache_inserts", "status/qcache_not_cached"])
        if vals:
            qc_hits, reads, free, total, inserts, not_cached = vals

            self.update_metric("newrelic/query_cache_hits", qc_hits)
            self.update_metric("newrelic/query_cache_misses", inserts)
            self.update_metric("newrelic/query_cache_not_cached", not_cached)

            pct_query_cache_hit_utilization = 0.0
            if (qc_hits + reads) > 0:
                pct_query_cache_hit_utilization = (qc_hits / (qc_hits + reads)) * 100.0

            self.update_metric("newrelic/pct_query_cache_hit_utilization", pct_query_cache_hit_utilization)

            pct_query_cache_memory_in_use = 0.0
            if total > 0:
                pct_query_cache_memory_in_use = 100.0 - ((free / total) * 100.0)

            self.update_metric("newrelic/pct_query_cache_memory_in_use", pct_query_cache_memory_in_use)

        # Temp Table
        vals = self.get_values(["status/created_tmp_tables", "status/created_tmp_disk_tables"])
        if vals:
            tmp_tables, tmp_tables_disk = vals

            pct_tmp_tables_written_to_disk = 0.0
            if tmp_tables > 0:
                pct_tmp_tables_written_to_disk = (tmp_tables_disk / tmp_tables) * 100.0

            self.update_metric("newrelic/pct_tmp_tables_written_to_disk", pct_tmp_tables_written_to_disk)

    def derive_newrelic_slaves(self):
        """
        Derive newrelic status metrics about slaves
        """
        if self.has_slave_data is True:
            self.update_metric("newrelic/replication_lag", self.sum_of(["slave/seconds_behind_master"]))

            # both need to be YES, which is 1
            running = self.sum_of(["slave/slave_io_running", "slave/slave_sql_running"])
            if running is not None:
                replication_status = 1.0
                if running == 2:
                    replication_status = 0.0
                self.update_metric("newrelic/replication_status", replication_status)
            self.update_metric("newrelic/slave_relay_log_bytes", self.sum_of(["slave/relay_log_pos"]))
            self.update_metric("newrelic/master_log_lag_bytes", self.diff_of(["slave/read_master_log_pos",
                                                                             "slave/exec_master_log_pos"]))
        else:  # This is a hack because the NR UI can't handle it missing for graphs
            self.update_metric("newrelic/replication_lag", 0.0)
            self.update_metric("newrelic/replication_status", 0.0)
            self.update_metric("newrelic/slave_relay_log_bytes", 0.0)
            self.update_metric("newrelic/master_log_lag_bytes", 0.0)

    def parse_metric_value(self, value):
        """
        Parse the values from mysql, converting them to floats when necessary
            on|yes|true => 1
            off|no|false => 0
            null => -1

        :param str value: The value from mysql
        :return: the parsed value
        :rtype: float or None
        """
        if isinstance(value, str):
            if value == "":
                return None

            # yes|true|on
            if self.is_true.match(value):
                return 1
            # no|false|off
            if self.is_false.match(value):
                return 0
            if self.is_null.match(value):
                return -1

            # anything else, try to convert it to a float
            try:
                r = float(value)
                return r
            except Exception:
                pass

            return None

        return value

    def is_number(self, value):
        """
        Check if something is a number.

        :param mixed value: The value to check
        :result: True if the value is a number type, False otherwise
        :rtype: bool
        """
        if isinstance(value, (six.integer_types + (int, float, complex))):
            return True
        return False

    def parse_row_stats(self, cursor):
        """
        Parse the SQL results with a single row of values, keyed by their column name.

        :param cursor: The sql cursor to use for the SQL queries
        :return: A dict of name/values metrics
        :rtype: dict
        """
        rows = list(cursor)
        if len(rows) > 0:
            column_names = [desc[0] for desc in cursor.description]
            # assumed to be a single row returned
            # convert the column names to lowercase
            return dict(zip(column_names, rows[0]))
        return dict()

    def parse_set_stats(self, cursor):
        """
        Parse a set of SQL results where the first column is the name and the second column is the value.

        :param cursor: The sql cursor to use for the SQL queries
        :return: A dict of name/values metrics
        :rtype: dict

        """
        rows = list(cursor)
        if len(rows) > 0 and len(rows[0]) == 2:
            return dict(rows)
        return dict()

    def parse_innodb_status_stats(self, cursor):
        """
        Parse the innodb status results and pull interesting metrics from it.

        :param cursor: The sql cursor to use for the SQL queries
        :return: A dict of name/values metrics
        :rtype: dict

        """
        rows = list(cursor)
        metrics = {
            "history_list_length": r"^History list length\s+(\d+)",
            "log_sequence_number": r"^Log sequence number\s+(\d+)",
            "last_checkpoint": r"^Last checkpoint at\s+(\d+)",
            "queries_inside_innodb": r"^(\d+)\s+queries inside InnoDB",
            "queries_in_queue": r"queries inside InnoDB,\s+(\d+)\s+queries in queue",
        }
        result = {
            'log_sequence_number': 0.0,
            'last_checkpoint': 0.0
        }
        if len(rows) > 0:
            text = rows[0][-1]
            for m in metrics:
                match = re.search(metrics[m], text, re.MULTILINE)
                if match is not None:
                    result[m] = match.group(1)

        result['checkpoint_age_metric'] = (float(result.get('log_sequence_number', 0.0)) -
                                           float(result.get('last_checkpoint', 0.0)))

        return result

    def parse_innodb_mutex_stats(self, cursor):
        """
        Parse innodb mutex output:
            1. toss out first column (type always set to 'InnoDB')
            2. string conversion of names (arrow "->" to underscore "_")
            3. extraction of value (yes/no/null => 1/0/-1, etc)
            4. aggregation of repeating names

        :param cursor: The sql cursor to use for the SQL queries
        :return: A dict of name/values metrics
        :rtype: dict

        """
        rows = list(cursor)
        result = dict()
        for row in rows:
            # columns = type, name, status
            # 1. ignore type (always "InnoDB")
            # 2. strip &, [, and ] chars, then convert "->" to "_"
            name = re.sub(r'[&\[\]]', "", row[1]).replace("->", "_")
            # 3. parse value from status column in "name=value" format
            value = self.parse_metric_value(row[2].split("=")[-1])
            if name in result:
                # 4. aggregate repeating names
                result[name] += value
            else:
                result[name] = value
        return result

    def connect(self):
        """Connect to MySQL, returning the connection object.

        :rtype: sql.connect

        """

        self.logger.debug("creating DB connection")
        conn = sql.connect(**self.connection_arguments)
        self.logger.debug("DB connection ready: %r", conn.get_host_info())
        return conn.cursor()

    @property
    def connection_arguments(self):
        """Create connection parameter dictionary for mysql.connect

        :return dict: The dictionary to be passed to mysql.connect
            via double-splat
        """
        filtered_args = ['name', 'metrics']

        # make sure we make a copy of this global so it is thread-safe
        args = dict(DEFAULT_CONNECT_ARGS)

        for key in set(self.config) - set(filtered_args):
            if key == 'dbname':
                args['database'] = self.config[key]
            else:
                args[key] = self.config[key]
        return args

    def poll(self):
        # initialize a custom logger to always add these fields
        self.logger = base.PluginLogger(LOGGER, dict(target_name=self.config['name'],
                                                     hostname=self.config['host']))
        self.initialize()
        self.raw_metrics = dict()
        try:
            # open a new connection
            with self.connect() as cursor:
                # self.verify_uuid(cursor)
                # self.logger.debug("done verifying uuid")
                self.collect_stats(cursor)
                self.logger.debug("done collecting data")
            # build stats
            self.add_stats()
        except ValueError as err:
            self.logger.exception(err)
        except sql.Error as err:
            if _errno(err) == ER_ACCESS_DENIED_ERROR:
                self.logger.error("Something is wrong with your user name or password")
            elif _errno(err) == ER_BAD_DB_ERROR:
                self.logger.error("Database does not exist")
            else:
                self.logger.error('Could not connect to %s, skipping stats run: %s' %
                                  (self.__class__.__name__, err))
        finally:
            self.finish()

    def finish(self):
        """Note the end of the stat collection run and let the user know of any
        errors.

        """
        sev = 'info'
        desc = 'successful'
        col = 0
        if not self.derive_values and not self.gauge_values:
            sev = 'error'
            desc = 'unsuccessful'
        else:
            col = len(self.derive_values) + len(self.gauge_values)

        dur = time.time() - self.poll_start_time
        getattr(self.logger, sev)('%s poll %s' % (self.__class__.__name__, desc),
                                  extra={"duration": "%.3f" % dur, "collected": col})
