#!/usr/bin/python
# -*- coding: utf-8 -*-
from castor.config import CassandraKeyspace
from cassandra.cluster import BatchStatement,ConsistencyLevel
from cassandra import InvalidRequest,WriteTimeout
import time
import re

class Dataset:

    def __init__(self, result, start, end, wanted_step, tag):
        self.data = None
        self.rows = 0 
        self.last = None
        self.first = None
        self.first_raw = None
        self.step = None
        self.tags = {}
        self.cassandra_data = result
        self.__discretize__(start, end, wanted_step, tag)


    def __discretize__(self, start, end, wanted_step, tag):
        #timestamp demande = variable initialise a start
        #on prend les valeurs entre timestamp demande -1/2 step et timestamp demande + 1/2step
        #on maintient 2 valeurs somme et count
        #des qu'on a une valeur superieure a timestamp demande + 1/2step on calcule la moyenne,
        #on reinitialise somme et compteur et on prend en compte la valeur et on augmente timestamp demande du step
        #si count = 0 ==> None
        #on garde le dernier counter et si gauge n'est pas defini pour le raw en cours on le calcule du compteur
        required_ts = start
        last_counter = None
        value_sum = 0.0
        value_count = 0
        value_max = None
        discretized_data = {}
        self.valid_rows = 0
        self.step = wanted_step
        all_rows_selected = []
        for row in self.cassandra_data:
            all_rows_selected.append(row)
            self.rows+=1
            timestamp = row.timestamp
            value = None
            if self.first_raw is None:
                self.first_raw = timestamp
            if timestamp > end:
                break
            if tag:
                self.tags[tag] = 1
                if row.tag != tag:
                    continue 
            #counters
            if row.__dict__.has_key('counter') and row.counter is not None:
                value = None
                max_value = None
                if last_counter and row.counter>= last_counter:
                    value = (row.counter - last_counter)*1.0 / (timestamp-last_ts)
                    max_value = value
                last_counter = row.counter
                last_ts = timestamp
            elif row.__dict__.has_key('gauge') and row.gauge is not None:
                value = row.gauge
                max_value = row.gauge
            elif row.__dict__.has_key('avg') and row.avg is not None:
                value = row.avg
                max_value = row.max
            else:
                continue

            if not wanted_step:
                #no discretization
                self.valid_rows += 1
                self.last = timestamp
                if self.first is None:
                    self.first = timestamp
                discretized_data[timestamp] = {}
                discretized_data[timestamp]['AVG'] = value
                discretized_data[timestamp]['MAX'] = max_value
            elif row.timestamp > (required_ts + wanted_step/2):
                self.valid_rows += 1
                self.last = required_ts
                if self.first is None:
                    self.first = required_ts
                discretized_data[required_ts] = {}
                discretized_data[required_ts]['AVG'] = None
                if value_count > 0:
                    discretized_data[required_ts]['AVG'] = value_sum / value_count
                discretized_data[required_ts]['MAX'] = value_max
                
                #on reinitialise pour le prochain calcul
                while (required_ts + wanted_step/2) < row.timestamp:
                    required_ts += wanted_step
                    self.last = timestamp
                    discretized_data[required_ts] = {}
                    discretized_data[required_ts]['AVG'] = None
                    discretized_data[required_ts]['MAX'] = None
                value_sum = 0 
                value_count = 0
                if value is not None:
                    value_count = 1
                    value_sum = value
                    value_max = max_value

            elif value is not None and row.timestamp >= (required_ts - wanted_step/2):
                value_sum += value
                value_count += 1
                if value_max is None or value_max < value:
                    value_max = value
        if wanted_step:
            self.last = required_ts
            discretized_data[required_ts] = {}
            discretized_data[required_ts]['AVG'] = None
            if value_count > 0:
                if self.first is None:
                    self.first = required_ts
                discretized_data[required_ts] = {}
                self.valid_rows += 1
                discretized_data[required_ts]['AVG'] = value_sum / value_count
            discretized_data[required_ts]['MAX'] = value_max
        self.cassandra_data = all_rows_selected
        #print discretized_data
        self.data = discretized_data

    def get_valid_rows(self):
        return self.valid_rows


    def get_rows(self):
        return self.rows


    def get_raw_mean_step(self):
        """
        Mean step met in raw datas
        """
        if self.rows > 0:
            return (self.last - self.first_raw) / self.rows
        else:
            return None
    
    def __getitem__(self,i):
        return self.data[i]
    
    def get_data(self):
        return self.data

    def get_tags(self):
        """
        Returns list of available tags
        """
        return self.tags.keys()

    
    def get_step(self):
        return self.step

    def get_first_ts(self):
        return self.first


    def get_last_ts(self):
        return self.last


    def interpolate(self,required_step):
        """
        if no mean_stepvalue is present at ts[required] we interpolate it with values present in an interval smaller than
        required - mean_step ---> required + mean_step
        """
        mean_step = self.get_raw_mean_step()
        last_valid_value = dict()
        last_ts_with_valid_value = None
        all_ts_to_compute = [] 
        if self.first is None:
            return   
        for ts in range(self.first,self.last+1,self.step):
            none_value = True
            for consol_func in self.data[ts].keys():
                """
                value for this ts is defined, we store it in last_valid_value
                """
                if self.data[ts][consol_func] is not None:
                    none_value = False

            if none_value:
                if (ts - self.first)%required_step == 0:
                    all_ts_to_compute.append(ts)
            else:
                """
                we compute the previous values to compute
                """
                while len(all_ts_to_compute) > 0:
                    ts_to_compute = all_ts_to_compute.pop()
                    if last_ts_with_valid_value is not None:
                        if ts_to_compute - last_ts_with_valid_value <= mean_step and ts - ts_to_compute <= mean_step:
                            """
                            interval smaller than required - mean_step ---> required + mean_step
                            """
                            for consol_func in self.data[ts_to_compute].keys():
                                xa = last_ts_with_valid_value
                                ya = float(self.data[xa][consol_func])
                                xb = ts
                                yb = float(self.data[xb][consol_func])
                                x = ts_to_compute
                                """ 
                                Taylor young formula to make linear interpolation
                                """
                                y = ya + (x - xa) * (yb-ya)/(xb-xa)
                                self.data[ts_to_compute][consol_func] = y
                                
                #reinitialisation for next value
                last_valid_value = self.data[ts]
                last_ts_with_valid_value = ts
        




class RRDBaseStorage(CassandraKeyspace):
    
    max_pending_requests = 100

    class TooLongPeriodException(Exception):
        pass

    def __init__(self, config, column_family, keys, stored_values):
        
        CassandraKeyspace.__init__(self, config)
        self.column_family = column_family
        self.keys = keys
        self.values = stored_values
        self.step = None
        self.indexation_macro_step = 86400 #number of seconds in a day
        self.stored_raw_results = {}
        self.curr_pending_requests = 0
        self.batch = BatchStatement(consistency_level=self.write_consistency)
        try:
            self.__prepare_requests__()
        except InvalidRequest as e:
            self.session.execute(self.create_cf)
            self.__prepare_requests__()


    def __prepare_requests__(self):
        key_variables_creation_str = ','.join(map(lambda x: "%s %s"%(x,self.keys[x]), self.keys.keys()))
        stored_values_creation_str = ','.join(map(lambda x: "%s %s"%(x,self.values[x]), self.values.keys()))
        key_list = ','.join(self.keys.keys())
        create_cf = "CREATE TABLE %s ( %s,period int, timestamp int, tag ascii, %s,"%(self.column_family, key_variables_creation_str, stored_values_creation_str)
        create_cf += "PRIMARY key((%s,period),timestamp, tag))"%key_list
        create_cf += " With compression = { 'sstable_compression' : 'DeflateCompressor', 'chunk_length_kb' : 64 }"
        self.create_cf = self.session.prepare(create_cf)
        affect_keys = map(lambda x: "%s = ?"%x, self.keys.keys())
        self.get_rows_req = self.session.prepare("SELECT timestamp,tag,%s FROM %s WHERE %s AND period=? \
                                                AND timestamp >= ? AND timestamp <= ? \
                                                order by timestamp"%(','.join(self.values.keys()),self.column_family, " AND ".join(affect_keys)))
        self.get_rows_req.consistency_level = self.read_consistency
        self.count_rows_for_period_req = self.session.prepare("SELECT count(*) FROM %s WHERE %s AND period=? \
                                                AND timestamp >= ? AND timestamp <= ?"%(self.column_family, " AND ".join(affect_keys)))
        self.count_rows_for_period_req.consistency_level = self.read_consistency

    def clear_cached_results(self):
        """
        Delete all cached results stored
        Must be called when you use function
        get_rows with store_results option
        """
        self.stored_raw_results = {}

    def get_rows(self, keys_values, start, end, wanted_step , tag, store_results = False ):
        """
        Getting all rows on period start, end for given tag (can be none)
        If store_results is True datas getting from cassandra will be stored in the cache
        Dont forget to call function clear_cached_results when you can, to avoid memory explosion
        """
        key_binded = map(lambda x: keys_values[x], self.keys.keys())
        period_start = start / self.indexation_macro_step
        if end - start > self.indexation_macro_step:
            raise RRDBaseStorage.TooLongPeriodException("Period requested in get_rows can't exceed indexation_macro_step %s for this storage"%self.indexation_macro_step)
        key_binded.extend([period_start,start,end])
        extraction_key = '_'.join(map(lambda x: str(x),key_binded))
        result = None
        if self.stored_raw_results.has_key(extraction_key):
            #getting result in cache
            dataset = self.stored_raw_results[extraction_key]
            dataset.__discretize__(start, end, wanted_step, tag )
            return dataset
        else:
            #requesting cassandra
            binded = self.get_rows_req.bind(tuple(key_binded))
            result = self.session.execute(binded)
            dataset = Dataset(result,start, end, wanted_step, tag) 
            if store_results:
                self.stored_raw_results[extraction_key] = dataset 
            return dataset



    def count_rows_for_period(self, ds_name, ts_start, ts_end):
        keys_values = {'ds_name':str(ds_name)}
        period = ts_start / self.indexation_macro_step
        if (ts_end / self.indexation_macro_step) != period:
            raise RRDBaseStorage.TooLongPeriodException("Period requested in get_rows can't exceed indexation_macro_step %s for this storage"%self.indexation_macro_step)
        key_binded = map(lambda x: keys_values[x], self.keys.keys())
        key_binded.extend([period,ts_start,ts_end])
        binded = self.count_rows_for_period_req.bind(tuple(key_binded))
        result = self.session.execute(binded).current_rows
        return result[0][0]

    def execute_write_request(self, request, bind, use_batch):
        if use_batch:
            #consistency already set in batch
            self.batch.add(request, bind)
            self.curr_pending_requests += 1
            if self.curr_pending_requests > RRDBaseStorage.max_pending_requests:
                self.finish_pending_requests()
        else:
            request.consistency_level = self.write_consistency
            binded = request.bind(bind)
            self.session.execute(binded)


    def finish_pending_requests(self):
        action_pending = True
        errors = 0
        if self.curr_pending_requests == 0:
            return
        #old_time = time.time()
        while action_pending and errors<5:
            try:
                self.session.execute(self.batch)
                action_pending = False
                self.batch = BatchStatement(consistency_level=self.write_consistency)
                self.curr_pending_requests = 0
            except WriteTimeout as wt:
                action_pending = True
                errors += 1
                time.sleep(10)

    def get_storage_step(self):
        return self.step

    def get_indexation_macro_step(self):
        return self.indexation_macro_step


class RRDRawStorage(RRDBaseStorage):

    def __init__(self, config):
        RRDBaseStorage.__init__(self, config, 'raw_data', {'ds_name':'ascii'}, {'gauge': 'float', 'counter': 'bigint'})


    def __prepare_requests__(self):
        RRDBaseStorage.__prepare_requests__(self)
        key_list = ','.join(self.keys.keys())
        self.req_insert_gauge_tag = self.session.prepare("INSERT INTO raw_data(ds_name, period, tag, timestamp, gauge) VALUES(?,?,?,?,?) using TTL ?")
        self.req_insert_counter_tag = self.session.prepare("INSERT INTO raw_data(ds_name,  period, tag, timestamp, counter) VALUES(?,?,?,?,?) using TTL ?")
    
    def insert_gauge(self, ds_name, data_points, retention, tag='',  use_batch=False):
        for dp in data_points.keys():
            self.execute_write_request(self.req_insert_gauge_tag,\
                                          (str(ds_name), int(dp / 86400), tag, int(dp), float(data_points[dp]), retention), use_batch)
                    
    

    def insert_counter(self, ds_name, data_points, retention, tag='',  use_batch=False):
        for dp in data_points.keys():
            self.execute_write_request(self.req_insert_counter_tag, \
                                         (str(ds_name), int(dp / 86400), tag, int(dp), int(data_points[dp]), retention), use_batch)



    def get_rows(self, ds_name, start, end, wanted_step = None, tag = None, store_results = False):
        return RRDBaseStorage.get_rows(self, {'ds_name':str(ds_name)},\
                                              start, end, wanted_step, tag, store_results)


class RRDAgregatedStorage(RRDBaseStorage):

    def __init__(self, config, table_name, step, index_macro_step):
        RRDBaseStorage.__init__(self, config, table_name, {'ds_name':'ascii'}, {'avg': 'float', 'max': 'float'})
        self.step = step
        self.indexation_macro_step = index_macro_step

    def __prepare_requests__(self):
        RRDBaseStorage.__prepare_requests__(self)
        key_list = ','.join(self.keys.keys())
        self.req_insert_agreg_value_tag = self.session.prepare("INSERT INTO %s(ds_name, period, tag, timestamp, avg, max) VALUES(?,?,?,?,?,?) using TTL ?"%(self.column_family))
        self.req_delete_agreg_value_tag = self.session.prepare("DELETE FROM %s WHERE ds_name=? AND period=? "%(self.column_family))
 
    
    def delete_agregated_values(self, ds_name, period):
        self.execute_write_request(self.req_delete_agreg_value_tag,
                                               (str(ds_name), period), False)   
 
    def insert_agregated_values(self, ds_name, dataset, retention, tag='', use_batch=False):
        for ts in dataset.get_data().keys():
            dsentry = dataset[ts]
            if dsentry['MAX'] or dsentry['AVG']:
                self.execute_write_request(self.req_insert_agreg_value_tag, \
                                              (str(ds_name), int(ts / self.indexation_macro_step), tag , int(ts),\
                                              dsentry['AVG'], dsentry['MAX'], retention), use_batch)

    def get_rows(self, ds_name, start, end, wanted_step, tag = None, store_results = False):
        return RRDBaseStorage.get_rows(self, {'ds_name':str(ds_name)},\
                                       start, end, wanted_step, tag, store_results)

