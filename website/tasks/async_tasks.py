import itertools
import numpy as np
import os.path
import time
from collections import OrderedDict

from celery.task import task, Task
from django.utils.timezone import now
from sklearn.preprocessing import StandardScaler

from website.models import DBMSCatalog, Hardware, Result, ResultData, WorkloadCluster, PipelineResult
from website.models import Task as TaskModel
from website.settings import PIPELINE_DIR
from website.types import PipelineTaskType
from website.utils import DataUtil, JSONUtil


class UpdateTask(Task):

    def __call__(self, *args, **kwargs):
        self.rate_limit = '50/m'
        self.max_retries = 3
        self.default_retry_delay = 60
        
        # Update start time for this task
        task = TaskModel.objects.get(taskmeta_id=self.request.id)
        task.start_time = now()
        task.save()
        return super(UpdateTask, self).__call__(*args, **kwargs)
    
#     def after_return(self, status, retval, task_id, args, kwargs, einfo):
#         super(UpdateTask, self).after_return(status, retval, task_id, args, kwargs, einfo)
#         print "RETURNED!! (task_id={}, rl={}, mr={}, drt={})".format(task_id, self.rate_limit, self.max_retries, self.default_retry_delay)
#     
#     def on_failure(self, exc, task_id, args, kwargs, einfo):
#         super(UpdateTask, self).on_failure(exc, task_id, args, kwargs, einfo)
#         print "FAILURE!! {} (task_id={})".format(exc, task_id)
#     
#     def on_success(self, retval, task_id, args, kwargs):
#         super(UpdateTask, self).on_success(retval, task_id, args, kwargs)
#         print "SUCCESS!! result={} (task_id={})".format(retval, task_id)
#     
#     def on_retry(self, exc, task_id, args, kwargs, einfo):
#         super(UpdateTask, self).on_retry(exc, task_id, args, kwargs, einfo)
#         print "RETRY!! {} (task_id={})".format(exc, task_id)

@task(base=UpdateTask, name='preprocess')
def preprocess(a, b):
    print "PREPROCESSING ({}, {})".format(a, b)
    time.sleep(2)
    return a + b

@task(base=UpdateTask, name='run_wm')
def run_wm(q, r):
    print "RUNNING WM: ({}, {})".format(q, r)
    time.sleep(3)
    return q + r

@task(base=UpdateTask, name='run_gpr')
def run_gpr(x, y):
    print "RUNNING GP ({}, {})".format(x, y)
    time.sleep(4)
    return x + y

@task(name='aggregate_target_results')
def aggregate_target_results(result_id):
    newest_result = Result.objects.get(pk=result_id)
    target_results = Result.objects.filter(application=newest_result.application, dbms=newest_result.dbms)
    if len(target_results) == 0:
        raise Exception('Cannot find any results for app_id={}, dbms_id={}'
                        .format(newest_result.application, newest_result.dbms))
    target_result_datas = [ResultData.objects.get(result=tres) for tres in target_results]

    knob_labels = np.asarray(sorted(JSONUtil.loads(target_result_datas[0].param_data).keys()))
    metric_labels = np.asarray(sorted(JSONUtil.loads(target_result_datas[0].metric_data).keys()))
    agg_data = DataUtil.aggregate_data(target_result_datas, knob_labels, metric_labels)
    agg_data['newest_result_id'] = result_id

@task(name='map_workload')
def map_workload(target_data):
    from cmudbottertune.analysis.gp_tf import GPR
    from cmudbottertune.analysis.preprocessing import bin_by_decile

    newest_result = Result.objects.get(pk=target_data['newest_result_id'])
    dbms = newest_result.dbms.pk
    hardware = newest_result.application.hardware.pk
    workload_data = PipelineResult.get_latest(dbms, hardware, PipelineTaskType.WORKLOAD_MAPPING_DATA)
    if workload_data is None:
        return None

    data_values = JSONUtil.loads(workload_data.value)
    X_scaler = np.load(data_values['X_scaler'])
    y_scaler = np.load(data_values['y_scaler'])
    y_deciles = np.load(data_values['y_deciles'])['deciles']
    X_columnlabels = data_values['X_columnlabels']
    y_columnlabels = data_values['y_columnlabels']

    X_idxs = [i for i in range(target_data['X_matrix'].shape[1]) if target_data['X_columnlabels'][i] in X_columnlabels]
    y_idxs = [i for i in range(target_data['y_matrix'].shape[1]) if target_data['y_columnlabels'][i] in y_columnlabels]
    X_target = target_data['X_matrix'][:,X_idxs]
    y_target = target_data['y_matrix'][:,y_idxs]
    X_target = (X_target - X_scaler['mean']) / X_scaler['scale']
    y_target = (y_target - y_scaler['mean']) / y_scaler['scale']
    y_binned = np.empty_like(y_target)
    for i in range(y_target.shape[1]):
        y_binned[:,i] = bin_by_decile(y_target[:,i], y_deciles[i])

    scores = {}
    for wkld_id, wkld_entry_path in data_values['data'].iteritems():
        wkld_entry = np.load(wkld_entry_path)
        preds = np.empty_like(y_target)
        X_wkld = wkld_entry['X_matrix']
        for j in range(y_target.shape[1]):
            y_col = wkld_entry['y_matrix'][:,j].reshape(X_wkld.shape[0], 1)
            model = GPR()
            model.fit(X_wkld, y_col, ridge=0.01)
            preds[:,j] = bin_by_decile(model.predict(X_target).ypreds.ravel(), y_deciles[j])
        dists = np.sqrt(np.sum(np.square(np.subtract(preds, y_target)), axis=1))
        scores[wkld_id] = np.mean(dists)
    target_data['scores'] = scores
    return target_data

@task(name='aggregate_results')
def aggregate_results():
    unique_clusters = WorkloadCluster.objects.all()
    unique_clusters = filter(lambda x: x.isdefault() == False, unique_clusters)
    all_data = {}
    all_labels = {}
    for cluster in unique_clusters:
        results = ResultData.objects.filter(cluster=cluster)
        if len(results) < 2:
            continue
        if cluster.dbms.pk not in all_labels:
            knob_labels = np.asarray(sorted(JSONUtil.loads(results[0].param_data).keys()))
            metric_labels = np.asarray(sorted(JSONUtil.loads(results[0].metric_data).keys()))
            all_labels[cluster.dbms.pk] = (knob_labels, metric_labels)
        else:
            knob_labels, metric_labels = all_labels[cluster.dbms.pk]
        entry = DataUtil.aggregate_data(results, knob_labels, metric_labels)
        key = (cluster.dbms.pk, cluster.hardware.pk)
        if key not in all_data:
            all_data[key] = {}
        all_data[key][cluster.pk] = entry
    
    ts = now()
    tsf = ts.strftime("%Y%m%d-%H%M%S")
    for (dbkey, hwkey), cluster_data in all_data.iteritems():
        task_name = PipelineTaskType.TYPE_NAMES[PipelineTaskType.AGGREGATED_DATA].replace(' ', '').upper()
        savepaths = {}
        for clusterkey, entry in cluster_data.iteritems():
            fname = '{}_{}_{}_{}_{}.npz'.format(task_name, dbkey, hwkey, clusterkey, tsf)
            savepath = os.path.join(PIPELINE_DIR, fname)
            savepaths[clusterkey] = savepath
            np.savez_compressed(savepath, **entry)

        value = {
            'data': savepaths
        }

        new_res = PipelineResult()
        new_res.dbms = DBMSCatalog.objects.get(pk=dbkey)
        new_res.hardware = Hardware.objects.get(pk=hwkey)
        new_res.creation_timestamp = ts
        new_res.task_type = PipelineTaskType.AGGREGATED_DATA
        new_res.value = JSONUtil.dumps(value)
        new_res.save()

@task(name='create_workload_mapping_data')
def create_workload_mapping_data():
    from cmudbottertune.analysis.preprocessing import Bin

    agg_datas = PipelineResult.objects.filter(task_type=PipelineTaskType.AGGREGATED_DATA)
    dbmss = set([ad.dbms.pk for ad in agg_datas])
    hardwares = set([ad.hardware.pk for ad in agg_datas])

    for dbms_id, hw_id in itertools.product(dbmss, hardwares):
        data = PipelineResult.get_latest(dbms_id, hw_id, PipelineTaskType.AGGREGATED_DATA)
        file_info = JSONUtil.loads(data.value)
        cluster_data = OrderedDict()
        for cluster, path in file_info['data'].iteritems():
            compressed_data = np.load(path)
            X_matrix = compressed_data['X_matrix']
            y_matrix = compressed_data['y_matrix']
            X_columnlabels = compressed_data['X_columnlabels']
            y_columnlabels = compressed_data['y_columnlabels']
            rowlabels = compressed_data['rowlabels']

            # Filter metrics and knobs
            ranked_knobs = JSONUtil.loads(PipelineResult.get_latest(dbms_id, hw_id, PipelineTaskType.RANKED_KNOBS).value)[:10] # FIXME
            pruned_metrics = JSONUtil.loads(PipelineResult.get_latest(dbms_id, hw_id, PipelineTaskType.PRUNED_METRICS).value)
            knob_idxs = [i for i in range(X_matrix.shape[1]) if X_columnlabels[i] in ranked_knobs]
            metric_idxs = [i for i in range(y_matrix.shape[1]) if y_columnlabels[i] in pruned_metrics]
            X_matrix = X_matrix[:,knob_idxs]
            X_columnlabels = X_columnlabels[knob_idxs]
            y_matrix = y_matrix[:,metric_idxs]
            y_columnlabels = y_columnlabels[metric_idxs]

            # Combine duplicate rows
            X_unique, idxs, invs, cts = np.unique(X_matrix, return_index=True, return_inverse=True, return_counts=True, axis=0)
            num_unique = X_unique.shape[0]
            if num_unique < X_matrix.shape[0]:
                y_unique = np.empty((num_unique, y_matrix.shape[1]))
                rowlabels_unique = np.empty(num_unique, dtype=tuple)
                ix = np.arange(X_matrix.shape[0])
                for i, count in enumerate(cts):
                    if count == 1:
                        y_unique[i,:] = y_matrix[idxs[i],:]
                        rowlabels_unique[i] = (rowlabels[idxs[i]],)
                    else:
                        dup_idxs = ix[invs == i]
                        y_unique[i,:] = np.median(y_matrix[dup_idxs,:], axis=0)
                        rowlabels_unique[i] = tuple(rowlabels[dup_idxs])
                X_matrix = X_unique
                y_matrix = y_unique
                rowlabels = rowlabels_unique
            cluster_data[cluster] = {
                'X_matrix': X_matrix,
                'y_matrix': y_matrix,
                'X_columnlabels': X_columnlabels,
                'y_columnlabels': y_columnlabels,
                'rowlabels': rowlabels,
            }

        Xs = np.vstack([entry['X_matrix'] for entry in cluster_data.values()])
        ys = np.vstack([entry['y_matrix'] for entry in cluster_data.values()])

        X_scaler = StandardScaler(copy=False)
        X_scaler.fit(Xs)
        y_scaler = StandardScaler(copy=False)
        y_scaler.fit_transform(ys)
        y_binner = Bin(axis=0)
        y_binner.fit(ys)
        del Xs
        del ys

        task_name = PipelineTaskType.TYPE_NAMES[PipelineTaskType.WORKLOAD_MAPPING_DATA].replace(' ', '').upper()
        timestamp = data.creation_timestamp
        tsf = timestamp.strftime("%Y%m%d-%H%M%S")
        savepaths = {}
        for cluster, entry in cluster_data.iteritems():
            X_scaler.transform(entry['X_matrix'])
            y_scaler.transform(entry['y_matrix'])
            fname = '{}_{}_{}_{}_{}.npz'.format(task_name, dbms_id, hw_id, cluster, tsf)
            savepath = os.path.join(PIPELINE_DIR, fname)
            savepaths[cluster] = savepath
            np.savez_compressed(savepath, **entry)

        X_scaler_path = os.path.join(PIPELINE_DIR, '{}_XSCALER_{}_{}_{}.npz'.format(task_name, dbms_id, hw_id, tsf))
        np.savez_compressed(X_scaler_path, mean=X_scaler.mean_, scale=X_scaler.scale_)
        y_scaler_path = os.path.join(PIPELINE_DIR, '{}_YSCALER_{}_{}_{}.npz'.format(task_name, dbms_id, hw_id, tsf))
        np.savez_compressed(y_scaler_path, mean=y_scaler.mean_, scale=y_scaler.scale_)
        y_deciles_path = os.path.join(PIPELINE_DIR, '{}_YDECILES_{}_{}_{}.npz'.format(task_name, dbms_id, hw_id, tsf))
        np.savez_compressed(y_deciles_path, deciles=y_binner.deciles_)

        value = {
            'data': savepaths,
            'X_scaler': X_scaler_path,
            'y_scaler': y_scaler_path,
            'y_deciles': y_deciles_path,
            'X_columnlabels': cluster_data.values()[0]['X_columnlabels'].tolist(),
            'y_columnlabels': cluster_data.values()[0]['y_columnlabels'].tolist(),
        }

        new_res = PipelineResult()
        new_res.dbms = DBMSCatalog.objects.get(pk=dbms_id)
        new_res.hardware = Hardware.objects.get(pk=hw_id)
        new_res.creation_timestamp = timestamp
        new_res.task_type = PipelineTaskType.WORKLOAD_MAPPING_DATA
        new_res.value = JSONUtil.dumps(value, pprint=True)
        new_res.save()

