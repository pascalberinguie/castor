#!/usr/bin/python
# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import InvalidRequest
from cassandra import ConsistencyLevel
from cassandra.policies import TokenAwarePolicy,DCAwareRoundRobinPolicy,RoundRobinPolicy
import re

class CastorConfiguration:

    class ConfigurationMissingElement(Exception):
        pass

    item = {} 
    item['CASSANDRA_KEYSPACE'] = 'netstat'

    def __init__(self, conf_file):
        try:
            lines = [line.rstrip('\n') for line in open(conf_file)]
            for line in lines:
                if len(line.split('#')) > 1:
                    line = line.split('#')[0]
                if not re.match('^\s*$', line):
                    try:
                        elems = line.split('=',1)
                        key = elems[0]
                        value = elems[1]
                        key = elems[0].rstrip(' ')
                        value = elems[1].rstrip(' ')
                        CastorConfiguration.item[key] = value
                    except Exception as e:
                        raise Exception("Error reading line: %s"%(line))
        except IOError as e:
            print "Error %s doesn't exist"%conf_file
            raise e
        try:
            cassandraCluster = CastorConfiguration.item['CASSANDRA_CLUSTER']
            CastorConfiguration.item['cassandra_cluster'] = cassandraCluster.split(',')
        except KeyError as e:
            raise CastorConfiguration.ConfigurationMissingElement("Missing value in configuration: %s"%e)

    def __getitem__(self, key):
        return self.item[key]

    
    def has_param(self, key):
        return self.item.has_key(key)


class CassandraKeyspace:

    consistencies = {
                'ANY': ConsistencyLevel.ANY,
                'ONE': ConsistencyLevel.ONE,
                'TWO': ConsistencyLevel.TWO,
                'THREE': ConsistencyLevel.THREE,
                'QUORUM': ConsistencyLevel.QUORUM,
                'ALL': ConsistencyLevel.ALL,
                'LOCAL_QUORUM': ConsistencyLevel.LOCAL_QUORUM,
                'EACH_QUORUM': ConsistencyLevel.EACH_QUORUM,
                'SERIAL': ConsistencyLevel.SERIAL,
                'LOCAL_SERIAL': ConsistencyLevel.LOCAL_SERIAL,
                'LOCAL_ONE': ConsistencyLevel.LOCAL_ONE,
            }

    def __init__(self, config):
        policy = RoundRobinPolicy()
        if config.has_param('CASSANDRA_LB_POLICY') and config['CASSANDRA_LB_POLICY'] == 'DC_AWARE_POLICY':
            if config.has_param('CASSANDRA_LOCAL_DC'):
                policy = DCAwareRoundRobinPolicy(config['CASSANDRA_LOCAL_DC'])
            else:
                raise Exception("CASSANDRA_LOCAL_DC param is mandatory with policy DC_AWARE")
        if config.has_param('CASSANDRA_TOKEN_AWARE_POLICY') and config['CASSANDRA_TOKEN_AWARE_POLICY'] == 1:
            policy = TokenAwarePolicy(policy)

        cluster = Cluster(contact_points = config['cassandra_cluster'],
                          auth_provider = PlainTextAuthProvider(
                                           username=config['CASSANDRA_KEYSPACE'],
                                           password=config['CASSANDRA_PASSWORD']),
                          load_balancing_policy=policy)
        
        try:
            self.session = cluster.connect(config['CASSANDRA_KEYSPACE'])
        except InvalidRequest:
            raise Exception("Keyspace %s doesn't exists, please create it"%config['CASSANDRA_KEYSPACE'])
        
        self.read_consistency = ConsistencyLevel.ONE
        self.write_consistency = ConsistencyLevel.ONE
        
        if config.has_param('CASSANDRA_READ_CONSISTENCY') and CassandraKeyspace.consistencies.has_key(config['CASSANDRA_READ_CONSISTENCY']):
            self.read_consistency = CassandraKeyspace.consistencies[config['CASSANDRA_READ_CONSISTENCY']]
        if config.has_param('CASSANDRA_WRITE_CONSISTENCY') and CassandraKeyspace.consistencies.has_key(config['CASSANDRA_WRITE_CONSISTENCY']):
            self.write_consistency = CassandraKeyspace.consistencies[config['CASSANDRA_WRITE_CONSISTENCY']]

