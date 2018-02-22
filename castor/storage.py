#!/usr/bin/python
# -*- coding: utf-8 -*-

from castor.config import CastorConfiguration
from castor.metadata import MetaDataStorage
from castor.operations import Operation
from castor.rrd import RRDRawStorage, RRDAgregatedStorage

 
import time


class CastorEngine:

    raw_values_can_be_displayed = 300 #if step required is less than 5 minutes, we can display raw values

    def __init__(self, conf_file):
        config = CastorConfiguration(conf_file)
        self.storages = [] 
        self.storages.append(RRDRawStorage(config)) #raw
        #self.storages.append(RRDAgregatedStorage(config, 'consolidated_data', 900, 86400))
        self.storages.append(RRDAgregatedStorage(config, 'daily_data', 300, 86400)) #weekly
        self.storages.append(RRDAgregatedStorage(config, 'weekly_data', 3000, 7 * 86400)) #weekly
        self.storages.append(RRDAgregatedStorage(config, 'monthly_data', 12000, 30 * 86400)) #monthly
        self.storages.append(RRDAgregatedStorage(config, 'yearly_data', 144000, 366 * 86400)) #yearly
        self.metadatas = MetaDataStorage(config)


        
    def insert_collected_values(self, ds_name, vtype, values, tag='', use_batch=False, update_last_inserted_value=False):
        if len(values.keys()) == 0:
            return
        metadata = None
        try:
            metadata = self.metadatas.get_metadata(ds_name, True)
        except MetaDataStorage.NoSuchMetaData as e:
            metadata = self.metadatas.create_metadata(ds_name)
        except Exception as e:
            raise e
        first_ts = metadata.first_raw
        last_inserted_ts = metadata.last_inserted_ts
        for ts in values.keys():
            if first_ts is None or ts < first_ts:
                first_ts = ts
            if last_inserted_ts is None or ts > last_inserted_ts:
                last_inserted_ts = ts
        if not metadata.raw_retention:
            metadata.set_default_retentions()


        if first_ts != metadata.first_raw:
            #we get metadata from database before updating it
            #because first_raw may have change
            metadata = self.metadatas.get_metadata(ds_name, False)
            if first_ts < metadata.first_raw:
                metadata.set_first_raw(first_ts)

        if update_last_inserted_value and last_inserted_ts > metadata.last_inserted_ts:
            #we get metadata from database before updating it
            #because last_raw may have change
            metadata = self.metadatas.get_metadata(ds_name, False)
            if last_inserted_ts > metadata.last_inserted_ts:
                metadata.set_last_inserted_ts(last_inserted_ts)


        if vtype == 'g':
            self.storages[0].insert_gauge(ds_name, values, metadata.raw_retention, tag, use_batch)
        elif vtype == 'c':
            self.storages[0].insert_counter(ds_name, values, metadata.raw_retention, tag,  use_batch)


    def make_pending_actions(self):
        """
        Must be called at he end of your script if you have made writes using option "use_batch"
        """
        for storage in self.storages:
            storage.finish_pending_requests()

    
    def update_or_create_metadata(self, ds_name, first_raw=None, last_agregated=None, raw_retention=None, computed_retention=None, ds_infos=None, last_inserted_ts=None):
        """
        Update or create a metadata for a ds_name
        Will be called for example when you migrate datas and you want to set new date for first_raw
        you don't now if metadata exists or not
        """
        metadata = None   
        try:
            metadata = self.metadatas.get_metadata(ds_name)
            if first_raw:
                metadata.set_first_raw(first_raw)
            if last_agregated:
                metadata.set_last_agregated(last_agregated)
            if raw_retention:
                metadata.set_raw_retention(raw_retention)
            if computed_retention:
                metadata.set_computed_retention(computed_retention)
            if ds_infos:
                metadata.set_ds_infos(ds_infos)
            if last_inserted_ts:
                metadata.set_last_inserted_ts(last_inserted_ts)

        except MetaDataStorage.NoSuchMetaData as e:
            if raw_retention and computed_retention:
                metadata = self.metadatas.create_metadata(ds_name, first_raw, last_agregated, raw_retention, computed_retention)
            elif raw_retention:
                metadata = self.metadatas.create_metadata(ds_name, first_raw, last_agregated, raw_retention)
            else:
                metadata = self.metadatas.create_metadata(ds_name, first_raw, last_agregated)
            if ds_infos:
                metadata.set_ds_infos(ds_infos)
            if last_inserted_ts:
                metadata.set_last_inserted_ts(last_inserted_ts)
        return metadata



    def get_metadata_by_name(self, ds_name):
        return self.metadatas.get_metadata(ds_name)


    def eval_cdef(self, expr_cdef, starttime, endtime, step, consolidation=None, tag=None, raw_data_allowed = True):
        #searching the less precise storage whose step is more precise than needed step
        consolidation = consolidation or ['AVG','MAX']
        best_storage_index = len(self.storages) - 1 #the less precise
        while best_storage_index > 0:
            if self.storages[best_storage_index].get_storage_step() <= step:
                break
            else:
                best_storage_index -= 1
        
        result = {}
        operation = Operation(expr_cdef) 
        date = time.time()
        if endtime > date:
            #we can't request values in the future
            endtime = date

        """
        we compute the ts of first_raw_data, first_agregated_data
        and last_agregated_data
        they will be used for the loop start
        normaly :first_agregated_data < first_raw_data < last_agregated_data < now
        in most of cases first_agregated_data =~ now - computed_retention
        first_raw_data =~ now - raw_retention
        last_agregated_data =~ this day at 0:00 if datasource has been properly archived this morning
        here we compute the oldest value for all the datasources of the operation:oldest_value
        the most recent (between all datasources) first raw data: newest_fraw_data
        and the oldest last agregated_value: oldest_last_agregated
        between oldest_value and newest_fraw_data we take only computed datas because raw data are not present
        between newest_fraw_data and oldest_last_agregated we take agregated_data if precision is upper than required 
        between oldest_last_agregated and now we take raw_datas
        """
        oldest_value = date    
        newest_fraw_data = 0
        oldest_last_agregated = date

        for variable in operation.get_variables():
            dataset_metadatas = self.metadatas.get_metadata(variable)
            if dataset_metadatas.first_raw < oldest_value:
                oldest_value = dataset_metadatas.first_raw

            first_raw_data = max(dataset_metadatas.first_raw,date-dataset_metadatas.raw_retention)
            if first_raw_data > newest_fraw_data:
                newest_fraw_data = first_raw_data
            
            last_agregated = dataset_metadatas.last_agregated
            if last_agregated is None:
                print "For %s No value for last_agregated, it will be very slow"%(variable)
                last_agregated = first_raw_data
            if last_agregated < oldest_last_agregated:
                oldest_last_agregated = last_agregated           
       
        #print "oldest_value: %s"%oldest_value
        #print "newest_fraw_data: %s"%newest_fraw_data
        #print "oldest_last_agregated: %s"%oldest_last_agregated

        #giving good range to the result
        result[starttime] = {}
        result[endtime] = {}
        for func in consolidation:
            result[starttime][func] = None
            result[endtime][func] = None
        
        start = max(starttime, oldest_value)
        while start < endtime:
            """
            Boucle sur toute la periode
            A chaque iteration on requete la periode equivalent a la valeur du indexation_step
            1 jour pour raw_data
            """
            if best_storage_index == 0 and step and step <= CastorEngine.raw_values_can_be_displayed \
                and len(operation.get_variables()) == 1 and raw_data_allowed:
                #on a besoin d'un step inferieur a 5 minutes, on a une seule variable a substituer dans l'operation
                #autant donc fournir les valeurs brutes
                step = None

            storage_index = best_storage_index

            if start >= oldest_last_agregated:
                #no agregated data, we will use raw data.
                storage_index = 0
                indexation_step = self.storages[storage_index].get_indexation_macro_step()
                end = min((int((start + indexation_step) / indexation_step) * indexation_step), endtime)
            elif start <= newest_fraw_data and storage_index == 0:
                storage_index = 1
                indexation_step = self.storages[storage_index].get_indexation_macro_step()
                end = min((int((start + indexation_step) / indexation_step) * indexation_step), endtime, oldest_last_agregated, newest_fraw_data)
            else:
                indexation_step = self.storages[storage_index].get_indexation_macro_step()       
                end = min((int((start + indexation_step) / indexation_step) * indexation_step), endtime, oldest_last_agregated)
          

            for variable in operation.get_variables():

                print "-->getting dataset for %s start:%s end:%s diff:%s storage:%s" % (variable,start,end, end-start, storage_index)
                dataset = self.storages[storage_index].get_rows(variable, start, end, step , tag )
                operation.set_dataset(variable, dataset)
            for ts in operation.get_available_ts():
                values_computed = 0
                for func in consolidation:
                    value = operation.evaluate(ts, func)
                    if not value is None:
                        if not result.has_key(ts):
                            result[ts] = {}
                        result[ts][func] = value
            start = end 

        

        return result




    def archive_ds(self, ds_name, full_archive=False, end_date=None):
        """
        Fill agregated storage for a particular datasource
        To increase speed when you consult data
        By default start at the timestamp of the latest value computed
        the latest time this function has be launched
        But you can at the beginning of datas containing in archive using full_archive= True
    
        We always start at a date that is a multiple of a day
        1st january 1970 0:00 + (n * 86400)
        It's very important to be sure that we will inject the same points in a future
        programm iteration 
        """
        
        date = time.time()
        dataset_metadatas = self.metadatas.get_metadata(ds_name)
        if not dataset_metadatas.computed_retention:
            metadata.set_default_retentions()

        starttime = dataset_metadatas.last_agregated
        origin_storage_step = self.storages[0].get_indexation_macro_step()
        if end_date is None:
            end_date = int(date  / origin_storage_step) * origin_storage_step #===> this day at midnight   
        self.storages[0].clear_cached_results()

        if dataset_metadatas.last_agregated is None or full_archive:
            if dataset_metadatas.first_raw:
                starttime = int(dataset_metadatas.first_raw / origin_storage_step) * origin_storage_step
            else:
                #bogus situation because metadata is initialized at first iteration but we can repair -5 years
                starttime = end_date - 86400 * 365 * 5
        else:
            #nous ne faisons pas confiance au champ last_agregated present en base et nous nous assurons qu'il s'agit
            #bien d'un nombre entier de jours
            starttime = int(dataset_metadatas.last_agregated / origin_storage_step) * origin_storage_step
    
        
        for storage in self.storages:
            if storage.get_storage_step() is None:
                #raw dataset nothing to inject :)
                continue

            start = starttime
            needed_step = storage.get_storage_step()
            agreg_data_deleted = dict()
            while start < end_date:
                end = start + origin_storage_step
                untagged_values_to_inject = self.storages[0].get_rows(ds_name, start, end, needed_step, None, True)
                tags = untagged_values_to_inject.get_tags()
               
                if full_archive:
                    #we delete existing agregated data
                    for ts in untagged_values_to_inject.get_data().keys():
                        #we compute period in dst storage corresponding to this ts) to delete each period 
                        #one single time (very important to not delete previously inserted values
                        dst_period = int(ts / storage.get_indexation_macro_step())
                        if not agreg_data_deleted.has_key(dst_period):
                            agreg_data_deleted[dst_period] = True
                            storage.delete_agregated_values(ds_name, dst_period)

                for tag in tags:
                    untagged_values_to_inject = None
                    tagged_values = self.storages[0].get_rows(ds_name, start, end, needed_step, tag, True)
                    storage.insert_agregated_values(ds_name, tagged_values, dataset_metadatas.computed_retention, tag, True)
                if untagged_values_to_inject:
                    #no tag for this datasource
                    storage.insert_agregated_values(ds_name, untagged_values_to_inject, dataset_metadatas.computed_retention,'', True)

                start = end
            storage.finish_pending_requests()
        dataset_metadatas.set_last_agregated(start)
        self.storages[0].clear_cached_results()

    def get_best_step(self, start, end, needed_points=250):
        """
        give the best step to call eval_cdef method
        the required number of point won't be stricty respected to optimize computing by choosing
        precomputed steps in available storages
        """
        best_step = abs(int((end - start) / needed_points))
        best_step = best_step > 30 and best_step or 30 #steps < 30
        for storage in self.storages:
            step = storage.get_storage_step()
            if step and abs(best_step - step)*100 / best_step < 30:
                return step
        return best_step



    def get_report_on_agregated_data(self, ds_name, ts):
        day_start = int(ts/(86400))*86400
        day_end = day_start + 86400 - 1
        for i in range(1, len(self.storages)):
            storage_name = self.storages[i].column_family
            storage_step = self.storages[i].step
            storage_macro_step = self.storages[i].indexation_macro_step
            print "%s (%s - %s) step=%s period=%s"%(storage_name, day_start, day_end, storage_step,int(day_start/storage_macro_step))
            expected_values = (day_end-day_start)/storage_step
            nb_values = self.storages[i].count_rows_for_period(ds_name,day_start,day_end)
	    print "%s  values stored %s expected"%(nb_values,expected_values) 


if __name__ == "__main__":
       
    storage = CastorEngine('/etc/hebex/netstat/netstat.conf')
    # print storage.eval_cdef(15891, 172532, 1450865918, 1450866518, 30, ['AVG'])
    storage.insert_collected_values('01:00', 'g', {10 : 5.0, 20: 10.0, 30: 20.5, 40: 16}, use_batch=False)
    storage.insert_collected_values('01:10', 'g', {10 : 100, 20: 15.0, 30: 22, 40:1 }, use_batch=False)
    print storage.eval_cdef(0, '01:00,01:10,+,10,*', 0, 50, 10)
    storage.get_report_on_agregated_data('01:10',1479579180)
    #id_host=14926&id_cdef=291998&stime=1479579180&etime=1495526424
    #storage.archive_ds("18129:4204")
