#!/usr/bin/python
# -*- coding: utf-8 -*-

from castor.config import CassandraKeyspace
from cassandra import InvalidRequest
import re

DEFAULT_RAW_RETENTION = 13 * 31 *86400 #13 Months
DEFAULT_COMPUTED_RETENTION = 366 * 5 *86400 #5 Years

class MetaData:
    """
    Stores metadatas for a mesure point
    timestamp of the first raw data
    timestamp of the first agregated data
    timestamp of the last agregated data
    """

    def __init__(self,storage, ds_name, first_raw, last_agregated, raw_retention, computed_retention,
                 ds_infos=None, last_inserted_ts= None, create_or_update=False):
        """
        Create a MetadaObjet with given parameters
        Objet is not inserted in cassandra database except if parameter create_or_update is set to true
        In this case the object is inserted if there are no object corresponding to this ds_name 
        or is updated with given values for first_raw, first_agregated, last_agregated in the other case
        """
        self.storage = storage
        self.ds_name = ds_name
        self.first_raw = first_raw
        self.raw_retention = raw_retention
        self.computed_retention = computed_retention
        self.last_agregated = last_agregated
        self.ds_infos = ds_infos
        self.last_inserted_ts = last_inserted_ts
        if create_or_update:
            self.__write__()


    def __write__(self):
        """
        Wite object Metadata to cassandra storage
        Object can be updated or created if needed
        """
        get_metadata = self.storage.get_metadata_req.bind((self.ds_name,))
        get_res = self.storage.session.execute(get_metadata)        
        row = None
        for row in get_res:
            if self.first_raw != row[0] or  self.last_agregated != row[1] \
                or self.raw_retention != row[2] or self.computed_retention != row[3] or self.ds_infos != row[4] or self.last_inserted_ts != row[5]:
                update_metadata = self.storage.update_metadata.bind((self.first_raw, self.last_agregated, \
                                                                     self.raw_retention, self.computed_retention,
                                                                     self.ds_infos, self.last_inserted_ts, self.ds_name))
                self.storage.session.execute(update_metadata)
        if not row:
            insert_metadata = self.storage.insert_metadata.bind((self.ds_name, self.first_raw, self.last_agregated,
                                                                 self.raw_retention, self.computed_retention, self.ds_infos, self.last_inserted_ts))
            self.storage.session.execute(insert_metadata)

    def set_first_raw(self, new_value):
        self.first_raw = new_value
        self.__write__()

    def set_last_agregated(self, new_value):
        self.last_agregated = new_value
        self.__write__()

    def set_raw_retention(self, new_value):
        self.raw_retention = new_value
        self.__write__()

    def set_computed_retention(self, new_value):
        self.computed_retention = new_value
        self.__write__()

    def set_ds_infos(self, infos):
        self.ds_infos = infos
        self.__write__()

    def set_default_retentions(self):
        self.computed_retention = DEFAULT_COMPUTED_RETENTION
        self.raw_retention = DEFAULT_RAW_RETENTION
        self.__write__()


    def set_last_inserted_ts(self, last_inserted_ts):
        self.last_inserted_ts = last_inserted_ts
        self.__write__()


    def __str__(self):
        return "Metadata %s"%str(self.to_array)


    def to_array(self):
        return {'ds_name':self.ds_name, 'first_raw':self.first_raw, 'raw_retention': self.raw_retention,
         'computed_retention': self.computed_retention, 'last_agregated':  self.last_agregated, 'ds_infos': self.ds_infos, 'last_inserted_ts': self.last_inserted_ts}


class MetaDataStorage(CassandraKeyspace):
    """
    Manages storage of metadatas in cassandra database
    """

    class NoSuchMetaData(Exception):
        pass

    class MetaDataAlreadyExists(Exception):
        pass

    """
    hash indexed by ds_name (id_host:id_oid) to stores all metadatas
    """
   

    
    def __prepare_requests__(self):
        create_cf = "CREATE TABLE metadatas (ds_name ascii, first_raw int, last_agregated int, raw_retention int, computed_retention int, last_inserted_ts int, ds_infos ascii,"
        create_cf += "PRIMARY key(ds_name))"
        self.create_cf = self.session.prepare(create_cf)


        delete_metadatas = "DELETE FROM metadatas where ds_name=?"
        self.delete_metadatas_req = self.session.prepare(delete_metadatas)

        get_metadata = "SELECT first_raw, last_agregated, raw_retention, computed_retention, ds_infos, last_inserted_ts FROM metadatas where ds_name=?"
        insert_metadata = "INSERT INTO metadatas(ds_name,first_raw,last_agregated, raw_retention, computed_retention, ds_infos, last_inserted_ts) values(?,?,?,?,?,?,?)"
        update_metadata = "UPDATE metadatas SET first_raw=?, last_agregated=?, raw_retention=?, computed_retention=?, ds_infos=?,last_inserted_ts=?  where ds_name=?"
        self.get_metadata_req = self.session.prepare(get_metadata)
        self.get_metadata_req.consistency_level = self.read_consistency
        self.insert_metadata = self.session.prepare(insert_metadata)
        self.insert_metadata.consistency_level = self.write_consistency
        self.update_metadata = self.session.prepare(update_metadata)
        self.update_metadata.consistency_level = self.write_consistency
                
    def __init__(self, config):
        CassandraKeyspace.__init__(self, config)
        self.metadatas = {}
        try:
            self.__prepare_requests__()
        except InvalidRequest as e:
            self.session.execute(self.create_cf)
            self.__prepare_requests__()



    def get_metadata(self, ds_name, use_cache=False):
        """
        if requested metadata not found a NoSuchMetaData Exception is thrown
        """
        metadata = self.__get_metadata__(ds_name, use_cache)
        if metadata:
            return metadata
        else:
            raise MetaDataStorage.NoSuchMetaData("No such metadata %s"%ds_name)


    def __get_metadata__(self, ds_name, use_cache=False):
        if use_cache and self.metadatas.has_key(ds_name):
            return self.metadatas[ds_name]
        else:
            select_metadata = self.get_metadata_req.bind((str(ds_name),))
            result = self.session.execute(select_metadata)
            for row in result:
                if row[2] is None and row[3] is None:
                    metadata = MetaData(self, ds_name, row[0], row[1], DEFAULT_RAW_RETENTION, 
                                        DEFAULT_COMPUTED_RETENTION, row[4], row[5])
                else:
                    metadata = MetaData(self, ds_name, row[0], row[1], row[2], row[3], row[4], row[5])
                self.metadatas[ds_name] = metadata
                return metadata
            else:
                return None

    def create_metadata(self, ds_name, first_raw=None, last_agregated = None, raw_retention=DEFAULT_RAW_RETENTION, computed_retention=DEFAULT_COMPUTED_RETENTION):
        """
        Create a new metadata for id_host,id_oid, values first_raw, first_agregated_raw
        If metadata already exists a MetaDataAlreadyExists is thrown
        By default retention is 13 months for raw datas and 5 years for computed datas
        """
        if self.__get_metadata__(ds_name, True):
            raise MetaDataStorage.MetaDataAlreadyExists("Metadata %s already exists"%(ds_name))
        else:
            #it is the nominal case
            new_metadata = MetaData(self, ds_name, first_raw, last_agregated, raw_retention, computed_retention, None, None, True)
            self.metadatas[ds_name] = new_metadata
            return new_metadata

    def delete_metadata(self, ds_name):
        delete_metadatas = self.delete_metadatas_req.bind((ds_name,))
        self.session.execute(delete_metadatas)

