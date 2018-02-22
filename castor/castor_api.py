#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals
from castor.config import CassandraKeyspace
from castor.operations import *
from castor.storage import *
import syslog
import time
import md5


class ApiError(Exception):
    
    def __init__(self, code, error_str, request_str):
        Exception.__init__(self, error_str)
        syslog.syslog(syslog.LOG_INFO, "An error occured for request %s => %s (code:%s)"%(request_str, error_str, code))
        self.error_str = error_str
        self.code = code 





class CastorApi:

    def __init__(self, storage, logger):
        self.storage = storage
        self.logger = logger


    def __print_log__(self, log):
        if self.logger:
            self.logger.debug(log)
        else:
            pass


class DataPointsApi(CastorApi):

    def __init__(self, storage, logger = None):
        CastorApi.__init__(self, storage, logger)
    
                 

    def post(self, json_in_entry, key=None):
         # One value
        self.__print_log__(json_in_entry)
        for mandatory_param in ('cdef_expr','stime','etime'):
            if not json_in_entry.has_key(mandatory_param):
                raise ApiError(406, "Parameter %s is mandatory"%mandatory_param, '')
        cdef_expr = json_in_entry['cdef_expr']
        stime = json_in_entry['stime']
        etime = json_in_entry['etime']
        raw_data_allowed = True;

        if json_in_entry.has_key('raw_data_allowed') and json_in_entry['raw_data_allowed'] == False:
            raw_data_allowed = False
        log_query = "cdef:%s stime:%s etime:%s"%(cdef_expr, stime, etime)
        if key is not None:
            if not json_in_entry.has_key('hash'):
                raise ApiError(401,"Parameter hash is required", log_query)
            hash_sig = json_in_entry['hash']
            """
            hash_sig must be equal to md5hex("cdef_expr",key)
            """
            m = md5.new(",".join([cdef_expr,key]))
            if hash_sig != m.hexdigest():
                raise ApiError(401,"Bad value for parameter hash", log_query)

        if json_in_entry.has_key('func') and json_in_entry['func'] == 'MAX':
            consol_func = 'MAX'
        else:
            consol_func = 'AVG'        

        if json_in_entry.has_key('step'):
            step = json_in_entry['step']
        else:
            step = self.storage.get_best_step(stime, etime)
        ev_cdef = {}
        try:
            step = int(step)
            ev_cdef = self.storage.eval_cdef(cdef_expr, stime, etime, step, None, None, raw_data_allowed)
        except Operation.StackError as e:
            raise ApiError(412,"StackError %s"%e, log_query) 
        except Operation.BadRPNException as e:
            raise ApiError(412,"BadRPN %s"%e, log_query)
        except MetaDataStorage.NoSuchMetaData as e:
            raise ApiError(412,"UnknownDatasourceError %s"%e, log_query)
        except Exception as e:
            raise ApiError(500,"Internal error %s"%e, log_query)
        finally:
            res = []
            for e in sorted(ev_cdef.keys()):
                res.append([e * 1000, ev_cdef[e][consol_func]])

            ar = {
                    "cdef": cdef_expr,
                    "stime": stime,
                    "etime": etime,
                    consol_func: res
            }
        return ar
 


class DataSourcesApi(CastorApi):

    def __init__(self, storage, logger = None):
        CastorApi.__init__(self, storage, logger)


    def get(self, ds_name):
        try:
            metatadata = self.storage.get_metadata_by_name(ds_name)
            return metatadata.to_array()
        except MetaDataStorage.NoSuchMetaData as e:
            raise ApiError(404,"Metadata not found","ds_name=%s"%ds_name)


    def post(self, ds_name, json_in_entry):
        json_params = ['first_raw','last_agregated','raw_retention','computed_retention','ds_infos','last_inserted_ts']
        json_in_entry['ds_name'] = ds_name
        for jp in json_params:
            if not json_in_entry.has_key(jp):
                json_in_entry[jp] = None

        metadata = self.storage.update_or_create_metadata(str(json_in_entry['ds_name']), json_in_entry['first_raw'], json_in_entry['last_agregated'], json_in_entry['raw_retention'],
        json_in_entry['computed_retention'], str(json_in_entry['ds_infos']), json_in_entry['last_inserted_ts'])

        if json_in_entry.has_key('values'):
                #by default we suppose that values to insert are gauges
                values_type = 'g'
                if json_in_entry.has_key('values_type'): 
                    if json_in_entry['values_type'] == 'gauge' or json_in_entry['values_type'] == 'g':
                        values_type = 'g'
                    elif json_in_entry['values_type'] == 'counter' or json_in_entry['values_type'] == 'c':
                        values_type = 'c'

                values = {}
                for k in json_in_entry['values'].keys():
                    try:
                        timestamp = int(k)
                        value = float(json_in_entry['values'][k])
                        values[timestamp] = value
                    except ValueError:
                         raise ApiError(412,"BadValue or timestamp given ts:%s value: %s an int is required for ts, a float is required for value"%(k, json_in_entry['values'][k]),"ds_name=%s"%json_in_entry['ds_name'])
                try:
                        self.storage.insert_collected_values(str(json_in_entry['ds_name']), values_type, values, use_batch=False, update_last_inserted_value=True)
                except Exception as e:
                     raise ApiError(500,"Error inserting values %s "%e,"ds_name=%s"%json_in_entry['ds_name'])





    
