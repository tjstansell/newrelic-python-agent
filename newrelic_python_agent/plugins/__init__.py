"""
Plugins are responsible for fetching and parsing the stats from the service
being profiled.

"""
available = {
    'apache_httpd': 'newrelic_python_agent.plugins.apache_httpd.ApacheHTTPD',
    'couchdb': 'newrelic_python_agent.plugins.couchdb.CouchDB',
    'edgecast': 'newrelic_python_agent.plugins.edgecast.Edgecast',
    'elasticsearch':
        'newrelic_python_agent.plugins.elasticsearch.ElasticSearch',
    'haproxy': 'newrelic_python_agent.plugins.haproxy.HAProxy',
    'memcached': 'newrelic_python_agent.plugins.memcached.Memcached',
    'mongodb': 'newrelic_python_agent.plugins.mongodb.MongoDB',
    'mysql': 'newrelic_python_agent.plugins.mysql.MySQL',
    'mysql-config': 'newrelic_python_agent.plugins.mysql-config.MySQLConfig',
    'nginx': 'newrelic_python_agent.plugins.nginx.Nginx',
    'pgbouncer': 'newrelic_python_agent.plugins.pgbouncer.PgBouncer',
    'php_apc': 'newrelic_python_agent.plugins.php_apc.APC',
    'php_fpm': 'newrelic_python_agent.plugins.php_fpm.FPM',
    'postgresql': 'newrelic_python_agent.plugins.postgresql.PostgreSQL',
    'prometheus': 'newrelic_python_agent.plugins.prometheus.Prometheus',
    'rabbitmq': 'newrelic_python_agent.plugins.rabbitmq.RabbitMQ',
    'redis': 'newrelic_python_agent.plugins.redis.Redis',
    'riak': 'newrelic_python_agent.plugins.riak.Riak',
    'uwsgi': 'newrelic_python_agent.plugins.uwsgi.uWSGI'}
