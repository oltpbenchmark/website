'''
Created on Jul 8, 2017

@author: dvanaken
'''

import json
import re
from abc import ABCMeta, abstractmethod
from collections import OrderedDict

import logging
LOG = logging.getLogger(__name__)

from .types import DBMSType, MetricType


class JSONUtil(object):

    @staticmethod
    def loads(config_str):
        return json.loads(config_str, encoding="UTF-8", object_pairs_hook=OrderedDict)

    @staticmethod
    def dumps(config, pprint=False):
        indent = 4 if pprint else None
        return json.dumps(OrderedDict(sorted(config.items())), encoding="UTF-8", indent=indent)


class DBMSUtilImpl(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def parse_version_string(self, version_string):
        pass

    @staticmethod
    def extract_valid_keys(idict, keys):
        lowercase_dict = {p.lower():v for p,v in idict.iteritems()}
        valid_dict = {}
        if len(idict) != len(keys):
            LOG.warn("TODO: handle extra/missing keys!")
        for key in keys:
            if key.lower() not in lowercase_dict:
                LOG.warn("TODO: handle unknown keys!")
                continue
            valid_dict[key] = lowercase_dict[key.lower()]
        return valid_dict


    def parse_dbms_config(self, config, official_config):
        config_names = [c.name for c in official_config]
        return DBMSUtilImpl.extract_valid_keys(config, config_names)

    def parse_dbms_metrics(self, metrics, official_metrics):
        metric_names = [m.name for m in official_metrics]
        return DBMSUtilImpl.extract_valid_keys(metrics, metric_names)

class PostgresUtilImpl(DBMSUtilImpl):

    def parse_version_string(self, version_string):
        dbms_version = version_string.split(',')[0]
        return re.search("\d+\.\d+(?=\.\d+)", dbms_version).group(0)

    def parse_dbms_metrics(self, metrics, official_metrics):
        # Postgres measures stats at different scopes (e.g. indexes,
        # tables, database) so for now we just combine them
        valid_metrics = {}
        for view_name, entries in metrics.iteritems():
            for entry in entries:
                for mname, mvalue in entry.iteritems():
                    key = '{}.{}'.format(view_name, mname)
                    if key not in valid_metrics:
                        valid_metrics[key] = []
                    valid_metrics[key].append(mvalue)

        # Extract all valid metrics
        official_metric_names = [m.name for m in official_metrics]
        valid_metrics = DBMSUtilImpl.extract_valid_keys(valid_metrics, official_metric_names)

        # Combine values
        for metric in official_metrics:
            mname = metric.name
            mvalues = valid_metrics[mname]
            if metric.metric_type == MetricType.INFO or len(mvalues) == 1:
                valid_metrics[mname] = mvalues[0]
            elif metric.metric_type == MetricType.COUNTER:
                mvalues = [int(v) for v in mvalues if v is not None]
                if len(mvalues) == 0:
                    valid_metrics[mname] = 0
                else:
                    valid_metrics[mname] = str(sum(mvalues))
            else:
                raise Exception('Invalid metric type: {}'.format(metric.metric_type))
        return valid_metrics

class DBMSUtil(object):

    __DBMS_UTILS_IMPLS = {
        DBMSType.POSTGRES: PostgresUtilImpl()
    }

    @staticmethod
    def __utils(dbms_type):
        try:
            return DBMSUtil.__DBMS_UTILS_IMPLS[dbms_type]
        except KeyError:
            raise NotImplementedError('Implement me! ({})'.format(dbms_type))

    @staticmethod
    def parse_version_string(dbms_type, version_string):
        return DBMSUtil.__utils(dbms_type).parse_version_string(version_string)

    @staticmethod
    def parse_dbms_config(dbms_type, config, official_config):
        return DBMSUtil.__utils(dbms_type).parse_dbms_config(config, official_config)

    @staticmethod
    def parse_dbms_metrics(dbms_type, metrics, official_metrics):
        return DBMSUtil.__utils(dbms_type).parse_dbms_metrics(metrics, official_metrics)

