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

from .types import BooleanType, DBMSType, MetricType, VarType, KnobUnitType


class JSONUtil(object):

    @staticmethod
    def loads(config_str):
        return json.loads(config_str, encoding="UTF-8", object_pairs_hook=OrderedDict)

    @staticmethod
    def dumps(config, pprint=False, sort=False):
        indent = 4 if pprint == True else None
        if sort == True:
            if isinstance(config, dict):
                config = OrderedDict(sorted(config.items()))
            else:
                config = sorted(config)

        return json.dumps(config, encoding="UTF-8", indent=indent)


class ConversionUtil(object):

    @staticmethod
    def get_raw_size(value, system):
        for factor, suffix in system:
            if value.endswith(suffix):
                if len(value) == len(suffix):
                    amount = 1
                else:
                    amount = int(value[:-len(suffix)])
                return amount * factor
        return None

    @staticmethod
    def get_human_readable(value, system):
        from hurry.filesize import size
        return size(value, system=system)


class DBMSUtilImpl(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def parse_version_string(self, version_string):
        pass

    def preprocess_bool(self, bool_value, param_info):
        return BooleanType.TRUE if bool_value.lower() == 'on' else BooleanType.FALSE

    def preprocess_enum(self, enum_value, param_info):
        enumvals = param_info.enumvals.split(',')
        try:
            return enumvals.index(enum_value)
        except ValueError:
            raise Exception('Invalid enum value for param {} ({})'.format(param_info.name, enum_value))

    def preprocess_integer(self, int_value, param_info):
        try:
            return int(int_value)
        except ValueError:
            return int(float(int_value))

    def preprocess_real(self, real_value, param_info):
        return float(real_value)

    @abstractmethod
    def preprocess_string(self, string_value, param_info):
        pass

    def preprocess_timestamp(self, timestamp_value, param_info):
        raise NotImplementedError('Implement me!')

    def preprocess_dbms_params(self, tunable_params, tunable_param_catalog):
        param_data = {} #OrderedDict()
        for pinfo in tunable_param_catalog:
            # These tunable_params should all be tunable
            assert pinfo.tunable == True, "All tunable_params should be tunable ({} is not)".format(pinfo.name)
            pvalue = tunable_params[pinfo.name]
            prep_value = None
            if pinfo.vartype == VarType.BOOL:
                prep_value = self.preprocess_bool(pvalue, pinfo)
            elif pinfo.vartype == VarType.ENUM:
                prep_value = self.preprocess_enum(pvalue, pinfo)
            elif pinfo.vartype == VarType.INTEGER:
                prep_value = self.preprocess_integer(pvalue, pinfo)
            elif pinfo.vartype == VarType.REAL:
                prep_value = self.preprocess_real(pvalue, pinfo)
            elif pinfo.vartype == VarType.STRING:
                prep_value = self.preprocess_string(pvalue, pinfo)
            elif pinfo.vartype == VarType.TIMESTAMP:
                prep_value = self.preprocess_timestamp(pvalue, pinfo)
            else:
                raise Exception('Unknown variable type: {}'.format(pinfo.vartype))

            if prep_value is None:
                raise Exception('Param value for {} cannot be null'.format(pinfo.name))
            param_data[pinfo.name] = prep_value
        return param_data

    def preprocess_dbms_metrics(self, numeric_metrics, numeric_metric_catalog, execution_time):
        metric_data = {} #OrderedDict()
        for minfo in numeric_metric_catalog:
            assert minfo.metric_type != MetricType.INFO
            mvalue = numeric_metrics[minfo.name]
            if minfo.metric_type == MetricType.COUNTER:
                converted = self.preprocess_integer(mvalue, minfo)
                metric_data[minfo.name] = float(converted) / execution_time
            else:
                raise Exception('Unknown metric type: {}'.format(minfo.metric_type))
        return metric_data

    @staticmethod
    def extract_valid_keys(idict, keys):
        valid_dict = {}
        diffs = []
        lowercase_dict = {k.lower():k for k in keys}
        for k, v in idict.iteritems():
            lower_k2 = k.lower()
            if lower_k2 in lowercase_dict:
                true_k = lowercase_dict[lower_k2]
                if k != true_k:
                    diffs.append(('miscapitalized_key', true_k, k, v))
                valid_dict[true_k] = v
            else:
                diffs.append('extra_key', None, k, v)
        if len(idict) < len(keys):
            assert len(diffs) > 0
        elif len(idict) > len(keys):
            lowercase_idict = {k.lower(): v for k, v in idict.iteritems()}
            for k in keys:
                if k.lower() not in lowercase_idict:
                    diffs.append('missing_key', k, None, None)
        return valid_dict, diffs


    def parse_dbms_config(self, config, official_config):
        config_names = [c.name for c in official_config]
        return DBMSUtilImpl.extract_valid_keys(config, config_names)

    def parse_dbms_metrics(self, metrics, official_metrics):
        metric_names = [m.name for m in official_metrics]
        return DBMSUtilImpl.extract_valid_keys(metrics, metric_names)


class PostgresUtilImpl(DBMSUtilImpl):

    POSTGRES_BYTES_SYSTEM = [
        (1024 ** 5, 'PB'),
        (1024 ** 4, 'TB'),
        (1024 ** 3, 'GB'),
        (1024 ** 2, 'MB'),
        (1024 ** 1, 'kB'),
        (1024 ** 0, 'B'),
    ]

    POSTGRES_TIME_SYSTEM = [
        (1000 * 60 * 60 * 24, 'd'),
        (1000 * 60 * 60, 'h'),
        (1000 * 60, 'min'),
        (1, 'ms'),
        (1000, 's'),
    ]

    def preprocess_string(self, enum_value, param_info):
        raise Exception('Implement me!')

    def preprocess_integer(self, int_value, param_info):
        converted = None
        try:
            converted = super(PostgresUtilImpl, self).preprocess_integer(int_value, param_info)
        except ValueError:
            if param_info.unit == KnobUnitType.BYTES:
                converted = ConversionUtil.get_raw_size(int_value, system=self.POSTGRES_BYTES_SYSTEM)
            elif param_info.unit == KnobUnitType.MILLISECONDS:
                converted = ConversionUtil.get_raw_size(int_value, system=self.POSTGRES_TIME_SYSTEM)
            else:
                raise Exception('Unknown unit type: {}'.format(param_info.unit))
        if converted is None:
            raise Exception('Invalid integer format for param {} ({})'.format(param_info.name, int_value))
        return converted

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
        valid_metrics, diffs = DBMSUtilImpl.extract_valid_keys(valid_metrics, official_metric_names)

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
        return valid_metrics, diffs

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
    def preprocess_dbms_params(dbms_type, tunable_params, tunable_param_catalog):
        return DBMSUtil.__utils(dbms_type).preprocess_dbms_params(tunable_params, tunable_param_catalog)

    @staticmethod
    def preprocess_dbms_metrics(dbms_type, numeric_metrics, numeric_metric_catalog, execution_time):
        return DBMSUtil.__utils(dbms_type).preprocess_dbms_metrics(numeric_metrics, numeric_metric_catalog, execution_time)

    @staticmethod
    def parse_dbms_config(dbms_type, config, official_config):
        return DBMSUtil.__utils(dbms_type).parse_dbms_config(config, official_config)

    @staticmethod
    def parse_dbms_metrics(dbms_type, metrics, official_metrics):
        return DBMSUtil.__utils(dbms_type).parse_dbms_metrics(metrics, official_metrics)

