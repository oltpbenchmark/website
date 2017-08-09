'''
Created on Jul 8, 2017

@author: dvanaken
'''

import json
import logging
import numpy as np
import os.path
import re
import string
from abc import ABCMeta, abstractmethod, abstractproperty
from collections import OrderedDict
from random import choice

import mimetypes
from django.http import StreamingHttpResponse
from wsgiref.util import FileWrapper

from .settings import CONFIG_DIR, UPLOAD_DIR
from .types import BooleanType, DBMSType, MetricType, VarType, KnobUnitType

LOG = logging.getLogger(__name__)


class JSONUtil(object):

    @staticmethod
    def loads(config_str):
        return json.loads(config_str,
                          encoding="UTF-8",
                          object_pairs_hook=OrderedDict)

    @staticmethod
    def dumps(config, pprint=False, sort=False):
        indent = 4 if pprint is True else None
        if sort is True:
            if isinstance(config, dict):
                config = OrderedDict(sorted(config.items()))
            else:
                config = sorted(config)

        return json.dumps(config,
                          encoding="UTF-8",
                          indent=indent)


class MediaUtil(object):

    @staticmethod
    def get_result_data_path(result_id):
        result_path = os.path.join(UPLOAD_DIR, str(result_id % 100))
        try:
            os.makedirs(result_path)
        except OSError as e:
            if e.errno == 17:
                pass
        return os.path.join(result_path, str(int(result_id) / 100l))

    @staticmethod
    def upload_code_generator(size=6,
                              chars=string.ascii_uppercase + string.digits):
        new_upload_code = ''.join(choice(chars) for _ in range(size))
        return new_upload_code

    @staticmethod
    def download_file(filepath, chunk_size=8192):
        filename = os.path.basename(filepath)
        response = StreamingHttpResponse(FileWrapper(open(filepath, 'rb'), chunk_size),
                                         content_type=mimetypes.guess_type(filepath)[0])
        response['Content-Length'] = os.path.getsize(filepath)
        response['Content-Disposition'] = "attachment; filename=%s" % filename
        return response


class DataUtil(object):

    @staticmethod
    def aggregate_data(results, knob_labels, metric_labels):
        X_matrix_shape = (len(results), len(knob_labels))
        y_matrix_shape = (len(results), len(metric_labels))
        X_matrix = np.empty(X_matrix_shape, dtype=float)
        y_matrix = np.empty(y_matrix_shape, dtype=float)
        rowlabels = np.empty(X_matrix_shape[0], dtype=int)

        for i, result in enumerate(results):
            param_data = JSONUtil.loads(result.param_data)
            if len(param_data) != len(knob_labels):
                raise Exception(
                    ("Incorrect number of knobs "
                     "(expected={}, actual={})").format(len(knob_labels),
                                                        len(param_data)))
            metric_data = JSONUtil.loads(result.metric_data)
            if len(metric_data) != len(metric_labels):
                raise Exception(
                    ("Incorrect number of metrics "
                     "(expected={}, actual={})").format(len(metric_labels),
                                                        len(metric_data)))
            X_matrix[i, :] = [param_data[l] for l in knob_labels]
            y_matrix[i, :] = [metric_data[l] for l in metric_labels]
            rowlabels[i] = result.pk
        return {
            'X_matrix': X_matrix,
            'y_matrix': y_matrix,
            'rowlabels': rowlabels,
            'X_columnlabels': knob_labels,
            'y_columnlabels': metric_labels,
        }

    @staticmethod
    def combine_duplicate_rows(X_matrix, y_matrix, rowlabels):
        X_unique, idxs, invs, cts = np.unique(X_matrix,
                                              return_index=True,
                                              return_inverse=True,
                                              return_counts=True,
                                              axis=0)
        num_unique = X_unique.shape[0]
        if num_unique == X_matrix.shape[0]:
            # No duplicate rows
            return X_matrix, y_matrix, rowlabels

        # Combine duplicate rows
        y_unique = np.empty((num_unique, y_matrix.shape[1]))
        rowlabels_unique = np.empty(num_unique, dtype=tuple)
        ix = np.arange(X_matrix.shape[0])
        for i, count in enumerate(cts):
            if count == 1:
                y_unique[i, :] = y_matrix[idxs[i], :]
                rowlabels_unique[i] = (rowlabels[idxs[i]],)
            else:
                dup_idxs = ix[invs == i]
                y_unique[i, :] = np.median(y_matrix[dup_idxs, :], axis=0)
                rowlabels_unique[i] = tuple(rowlabels[dup_idxs])
        return X_unique, y_unique, rowlabels_unique


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

    @abstractproperty
    def base_configuration_settings(self):
        return {}

    @abstractproperty
    def configuration_filename(self):
        pass

    @abstractmethod
    def parse_version_string(self, version_string):
        pass

    def preprocess_bool(self, bool_value, param_info):
        return BooleanType.TRUE if \
                bool_value.lower() == 'on' else BooleanType.FALSE

    def preprocess_enum(self, enum_value, param_info):
        enumvals = param_info.enumvals.split(',')
        try:
            return enumvals.index(enum_value)
        except ValueError:
            raise Exception('Invalid enum value for param {} ({})'.format(
                param_info.name, enum_value))

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
        param_data = {}
        for pinfo in tunable_param_catalog:
            # These tunable_params should all be tunable
            assert pinfo.tunable is True, \
                   "All tunable_params should be tunable ({} is not)".format(
                        pinfo.name)
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
                raise Exception(
                    'Unknown variable type: {}'.format(pinfo.vartype))

            if prep_value is None:
                raise Exception(
                    'Param value for {} cannot be null'.format(pinfo.name))
            param_data[pinfo.name] = prep_value
        return param_data

    def preprocess_dbms_metrics(self, numeric_metrics, numeric_metric_catalog,
                                external_metrics, execution_time):
        if len(numeric_metrics) != len(numeric_metric_catalog):
            raise Exception('The number of metrics should be equal!')
        metric_data = {}
        for minfo in numeric_metric_catalog:
            assert minfo.metric_type != MetricType.INFO
            mvalue = numeric_metrics[minfo.name]
            if minfo.metric_type == MetricType.COUNTER:
                converted = self.preprocess_integer(mvalue, minfo)
                metric_data[minfo.name] = float(converted) / execution_time
            else:
                raise Exception(
                    'Unknown metric type: {}'.format(minfo.metric_type))
        metric_data.update({k: float(v)
                            for k, v in external_metrics.iteritems()})
        return metric_data

    @staticmethod
    def extract_valid_keys(idict, official_config, default=None):
        valid_dict = {}
        diffs = []
        lowercase_dict = {k.name.lower(): k for k in official_config}
        for k, v in idict.iteritems():
            lower_k2 = k.lower()
            if lower_k2 in lowercase_dict:
                true_k = lowercase_dict[lower_k2].name
                if k != true_k:
                    diffs.append(('miscapitalized_key', true_k, k, v))
                valid_dict[true_k] = v
            else:
                diffs.append(('extra_key', None, k, v))
        if len(idict) > len(lowercase_dict):
            assert len(diffs) > 0
        elif len(idict) < len(lowercase_dict):
            lowercase_idict = {k.lower(): v for k, v in idict.iteritems()}
            for k, v in lowercase_dict.iteritems():
                if k not in lowercase_idict:
                    # Set missing keys to a default value
                    diffs.append(('missing_key', v.name, None, None))
                    valid_dict[
                        v.name] = default if default is not None else v.default
        assert len(valid_dict) == len(official_config)
        return valid_dict, diffs

    def parse_dbms_config(self, config, official_config):
        return DBMSUtilImpl.extract_valid_keys(config, official_config)

    def parse_dbms_metrics(self, metrics, official_metrics):
        return DBMSUtilImpl.extract_valid_keys(metrics,
                                               official_metrics,
                                               default='0')

    def create_configuration(self, tuning_params, custom_params,
                             official_catalog):
        config_params = self.base_configuration_settings
        config_params.update(custom_params)

        categories = {}
        for pinfo in official_catalog:
            pname = pinfo.name
            if pname not in config_params:
                continue
            category = pinfo.category
            if category not in categories:
                categories[category] = []
            categories[category].append((pname, config_params[pname]))
        categories = OrderedDict(sorted(categories.iteritems()))

        config_path = os.path.join(CONFIG_DIR, self.configuration_filename)
        with open(config_path, 'r') as f:
            config = f.read()

        header_fmt = ('#' + ('-' * 78) + '\n# {cat1}\n#' +
                      ('-' * 78) + '\n\n').format
        subheader_fmt = '# - {cat2} -\n\n'.format
        for category, params in categories.iteritems():
            parts = [p.strip() for p in category.upper().split(' / ')]
            config += header_fmt(cat1=parts[0])
            if len(parts) == 2:
                config += subheader_fmt(cat2=parts[1])
            for pname, pval in sorted(params):
                config += '{} = \'{}\'\n'.format(pname, pval)
            config += '\n'
        config += header_fmt(cat1='TUNING PARAMETERS')
        for pname, pval in sorted(tuning_params.iteritems()):
            config += '{} = \'{}\'\n'.format(pname, pval)
        return config#, self.configuration_filename

    def get_nondefault_settings(self, config, official_config):
        nondefault_settings = OrderedDict()
        for pinfo in official_config:
            if pinfo.tunable is True:
                continue
            pname = pinfo.name
            pvalue = config[pname]
            if pvalue != pinfo.default:
                nondefault_settings[pname] = pvalue
        return nondefault_settings


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

    POSTGRES_BASE_PARAMS = {
        'data_directory': None,
        'hba_file': None,
        'ident_file': None,
        'external_pid_file': None,
        'listen_addresses': None,
        'port': None,
        'max_connections': None,
        'unix_socket_directories': None,
        'log_line_prefix': '%t [%p-%l] %q%u@%d ',
        'track_counts': 'on',
        'track_io_timing': 'on',
        'autovacuum': 'on',
        'default_text_search_config': 'pg_catalog.english',
    }

    @property
    def base_configuration_settings(self):
        return dict(self.POSTGRES_BASE_PARAMS)

    @property
    def configuration_filename(self):
        return 'postgresql.conf'

    def preprocess_string(self, enum_value, param_info):
        raise Exception('Implement me!')

    def preprocess_integer(self, int_value, param_info):
        converted = None
        try:
            converted = super(PostgresUtilImpl, self).preprocess_integer(
                int_value, param_info)
        except ValueError:
            if param_info.unit == KnobUnitType.BYTES:
                converted = ConversionUtil.get_raw_size(
                    int_value, system=self.POSTGRES_BYTES_SYSTEM)
            elif param_info.unit == KnobUnitType.MILLISECONDS:
                converted = ConversionUtil.get_raw_size(
                    int_value, system=self.POSTGRES_TIME_SYSTEM)
            else:
                raise Exception(
                    'Unknown unit type: {}'.format(param_info.unit))
        if converted is None:
            raise Exception('Invalid integer format for param {} ({})'.format(
                param_info.name, int_value))
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
        official_metric_map = {m.name: m for m in official_metrics}
        valid_metrics, diffs = DBMSUtilImpl.extract_valid_keys(
            valid_metrics, official_metrics, default='0')

        # Combine values
        for mname, mvalues in valid_metrics.iteritems():
            metric = official_metric_map[mname]
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
                raise Exception(
                    'Invalid metric type: {}'.format(metric.metric_type))
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
    def preprocess_dbms_params(dbms_type,
                               tunable_params,
                               tunable_param_catalog):
        return DBMSUtil.__utils(dbms_type).preprocess_dbms_params(
                tunable_params, tunable_param_catalog)

    @staticmethod
    def preprocess_dbms_metrics(dbms_type,
                                numeric_metrics,
                                numeric_metric_catalog,
                                external_metrics,
                                execution_time):
        return DBMSUtil.__utils(dbms_type).preprocess_dbms_metrics(
                numeric_metrics, numeric_metric_catalog,
                external_metrics, execution_time)

    @staticmethod
    def parse_dbms_config(dbms_type, config, official_config):
        return DBMSUtil.__utils(dbms_type).parse_dbms_config(
                config, official_config)

    @staticmethod
    def parse_dbms_metrics(dbms_type, metrics, official_metrics):
        return DBMSUtil.__utils(dbms_type).parse_dbms_metrics(
                metrics, official_metrics)

    @staticmethod
    def get_nondefault_settings(dbms_type, config, official_config):
        return DBMSUtil.__utils(dbms_type).get_nondefault_settings(
            config, official_config)

    @staticmethod
    def create_configuration(dbms_type, tuning_params, custom_params,
                             official_catalog):
        return DBMSUtil.__utils(dbms_type).create_configuration(
            tuning_params, custom_params, official_catalog)

    @staticmethod
    def get_configuration_filename(dbms_type):
        return DBMSUtil.__utils(dbms_type).configuration_filename

