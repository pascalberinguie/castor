#!/usr/bin/python
import unittest
import time
from castor.rrd import *
from castor.operations import *
from castor.config import *
from castor.storage import *
from castor.castor_api import *
import md5

CONF_FILE = '/etc/hebex/netstat/netstat.conf'

class FakeRows(list):

    class FakeRow:

        def __init__(self,row):
            (tst,val) = row.split(':')
            if len(tst.split('@')) > 1:
                (tag, ts) = tst.split('@')
                self.tag = tag
            else:
                ts = tst
            (gauge,counter) = val.split(',')
            self.timestamp = int(ts)
            self.gauge = None
            self.counter = None
            if gauge != '':
                self.gauge = float(gauge)
            if counter != '':
                self.counter = int(counter)

    def __init__(self, string):

        for e in string.split(';'):
            self.append(FakeRows.FakeRow(e))



class FakeDataset(Dataset):
    """
    For testing purpose
    """
    def __init__(self, result, start, end, wanted_step, tag):
        self.data = result
        self.first = start
        self.last = end
        self.step = wanted_step
        self.first_raw = start
        self.tag = tag
        self.rows = len(result.keys())



class CassandraRequester(CassandraKeyspace):

    def __init__(self):
        CassandraKeyspace.__init__(self, CastorConfiguration(CONF_FILE))
        self.delete_metadata_req = self.session.prepare("DELETE FROM metadatas WHERE ds_name=?")
        self.delete_raw = self.session.prepare("DELETE FROM raw_data WHERE ds_name=? and period=?")
        self.delete_daily = self.session.prepare("DELETE FROM daily_data WHERE ds_name=? and period=?")
        self.delete_weekly = self.session.prepare("DELETE FROM weekly_data WHERE ds_name=? and period=?")
        self.get_wmax_req = self.session.prepare("SELECT max FROM weekly_data WHERE ds_name=? and period=? and timestamp = ?")
        self.get_raw_req = self.session.prepare("SELECT timestamp,gauge, counter FROM raw_data WHERE ds_name=? and period=?")
        self.get_metadata = self.session.prepare("SELECT computed_retention,first_raw,last_agregated,raw_retention FROM metadatas WHERE   ds_name=?")

    def purge_ds(self,ds_name,ts_ref):
        req = self.delete_metadata_req.bind((ds_name,))
        self.session.execute(req)
        req = self.delete_raw.bind((ds_name,int(ts_ref/86400)))
        self.session.execute(req)
        req = self.delete_raw.bind((ds_name,int(ts_ref/86400) +1))
        self.session.execute(req)
        req = self.delete_daily.bind((ds_name,int(ts_ref/86400)))
        self.session.execute(req)
        req = self.delete_daily.bind((ds_name,int(ts_ref/86400) +1))
        self.session.execute(req)
        req = self.delete_weekly.bind((ds_name,int(ts_ref/(86400*7))))
        req = self.delete_weekly.bind((ds_name,int(ts_ref/(86400*7)+1)))
        self.session.execute(req)


    def purge_raw(self,ds_name,ts_ref):
        """
        purge only raw datas to simulate the effect of retention reached
        """
        req = self.delete_raw.bind((ds_name,int(ts_ref/86400)))
        self.session.execute(req)
        req = self.delete_raw.bind((ds_name,int(ts_ref/86400) +1))


    def get_raw(self, ds_name, ts):
        period = int(ts /(86400))
        resp = {}
        req = self.get_raw_req.bind((ds_name, period))
        for r in self.session.execute(req):
            if not resp.has_key(r[0]):
                resp[r[0]] = dict()
            resp[r[0]]['gauge'] = r[1]
            resp[r[0]]['counter'] = r[2]
        return resp


    def get_wmax(self, ds_name, ts):
        period = int(ts /(86400*7))
        req = self.get_wmax_req.bind((ds_name, period, ts))
        for r in self.session.execute(req):
            return r[0]

    def get_metadata_as_array(self, ds_name):
        req = self.get_metadata.bind((ds_name,))
        for r in self.session.execute(req):
            metadata = {}
            metadata['computed_retention'] = r[0]
            metadata['first_raw'] = r[1]
            metadata['last_agregated'] = r[2]
            metadata['raw_retention'] = r[3]
            return metadata

    def delete_metadata(self, ds_name):
        req = self.delete_metadata_req.bind((ds_name,))
        self.session.execute(req)


class TestCastor (unittest.TestCase):


    def setUp(self):
        self.dataset_1 = FakeDataset({10: {'AVG':65}, 20: {'AVG':60}, 30: {'AVG':None}, 40: {'AVG':20}},10,40,10, None)
        self.dataset_2 = FakeDataset({10: {'AVG':30}, 20: {'AVG':20}, 30: {'AVG':10}, 40:{'AVG':None}},10,40,10, None)
       

    def test001_test_valid_rpn(self):
        operation = Operation("var1:UNDEF:0,var2:UNDEF:0,+,2,*")
        operation.set_dataset('var1', self.dataset_1)
        operation.set_dataset('var2', self.dataset_2)
        expected = {40: 40.0, 10: 190.0, 20: 160.0, 30: 20.0}
        result = {} 
        for t in range(10,50,10):
            result[t] = operation.evaluate(t)
        self.assertEquals(expected,result)



    def test002_test_empty_stack(self):
        operation = Operation("var1:UNDEF:0,var2:UNDEF:0,+,*,2,*")
        operation.set_dataset('var1', self.dataset_1)
        operation.set_dataset('var2', self.dataset_2)
        with self.assertRaises(Operation.StackError):
            for t in range(10,50,10):
                operation.evaluate(t)




    def test003_too_much_data_in_stack(self):
        operation = Operation("var1:UNDEF:0,var2:UNDEF:0,+,2,*,0")
        operation.set_dataset('var1', self.dataset_1)
        operation.set_dataset('var2', self.dataset_2)
        with self.assertRaises(Operation.StackError):
            for t in range(10,50,10):
                operation.evaluate(t) 


    def test004_nan_value(self):
        operation = Operation("var1:UNDEF:0,var2,+,2,*")
        operation.set_dataset('var1', self.dataset_1)
        operation.set_dataset('var2', self.dataset_2)
        expected = {40: None, 10: 190.0, 20: 160.0, 30: 20.0}
        result = {}
        for t in range(10,50,10):
            result[t] = operation.evaluate(t)
        self.assertEquals(expected,result)
 

    def test005_no_dataset(self):
        operation = Operation("var1:UNDEF:0,var2,+,2,*")
        operation.set_dataset('var1', self.dataset_1)
        with self.assertRaises(Operation.NoDataSetException):
            operation.evaluate(10)


    def test006_discretize(self):
        rows = FakeRows("10:15,;20:20,")
        dataset = Dataset(rows, 0 , 20, 10, None)
        self.assertEquals(dataset.get_data(),{0: {'MAX': None, 'AVG': None}, 10: {'MAX': 15.0, 'AVG': 15.0}, 20: {'MAX': 20.0, 'AVG': 20.0}})


    def test007_discretize(self):
        rows = FakeRows("10:15,;20:20,;30:10,;40:5,")
        dataset = Dataset(rows, 0, 40, 20, None)
        self.assertEquals(dataset.get_data(),{0: {'MAX': 15.0, 'AVG': 15.0}, 40: {'MAX': 5.0, 'AVG': 5.0}, 20: {'MAX': 20.0, 'AVG': 15.0}})


    def test008_discretize(self):
        rows = FakeRows("10:15,;20:20,;30:10,;40:5,")
        dataset = Dataset(rows, 0, 40,  20, None)
        self.assertEquals(dataset.get_data(),{0: {'MAX': 15.0, 'AVG': 15.0}, 40: {'MAX': 5.0, 'AVG': 5.0}, 20: {'MAX': 20.0, 'AVG': 15.0}}) 

    def test008b_discretizeZeros(self):
        rows = FakeRows("10:0,;20:0,;30:0,;40:0,")
        dataset = Dataset(rows, 0, 40,  20, None)
        self.assertEquals(dataset.get_data(),{0: {'MAX': 0.0, 'AVG': 0.0}, 40: {'MAX': 0.0, 'AVG': 0.0}, 20: {'MAX': 0.0, 'AVG': 0.0}})


    def test009_discretize_with_holes(self):
        rows = FakeRows("17:15,;19:15,;22:20,;24:20,;57:30,;59:30,;62:40,;64:40,")
        dataset = Dataset(rows, 0, 70, 20 , None)
        self.assertEquals(dataset.get_data(),{0: {'MAX': None, 'AVG': None}, 40: {'MAX': None, 'AVG': None}, 20: {'MAX': 20.0, 'AVG': 17.5}, 60: {'MAX': 40.0, 'AVG': 35.0}})

    def test010_discretize_with_holes_at_begining(self):
        rows = FakeRows("87:15,;89:15,;92:20,;94:20,;127:30,;129:30,;132:20,;134:40,")
        dataset = Dataset(rows, 0,140,20, None)
        self.assertEquals(dataset.get_data(),{0: {'MAX': None, 'AVG': None}, 20: {'MAX': None, 'AVG': None}, 40: {'MAX': None, 'AVG': None},
                                        60: {'MAX': None, 'AVG': None}, 80: {'MAX': 15, 'AVG': 15}, 100: {'MAX': 20, 'AVG': 20}, 
                                        120: {'MAX': 30, 'AVG': 30}, 140: {'MAX': 40, 'AVG': 30}})

    def test011_discretize_with_counters(self):
        rows = FakeRows("10:,100;20:,120;30:,180;40:,200")
        dataset = Dataset(rows, 0,40, 10, None)
        self.assertEquals(dataset.get_data(),{0: {'MAX': None, 'AVG': None}, 10: {'MAX': None, 'AVG': None}, 20: {'MAX': 2.0, 'AVG': 2.0},
                                        30: {'MAX': 6.0, 'AVG': 6.0}, 40: {'MAX': 2.0, 'AVG': 2.0}
                                       })  


    def test012_discretize_with_counters2(self):
        rows = FakeRows("10:,100;20:,120;30:,180;40:,200")
        dataset = Dataset(rows, 0 , 40, 20, None)
        self.assertEquals(dataset.get_data(),{0: {'MAX': None, 'AVG': None}, 20: {'MAX': 6.0, 'AVG': 4.0}, 40: {'MAX': 2.0, 'AVG': 2.0}
                                       })

    def test012b_discretize_bogus1(self):
        rows = FakeRows("1486425600:1.18,;1486512000:0.82,;1486569600:1.02,;148659840:0.74,;1486656000:0.76,;1487808000:0.96,")
        dataset = Dataset(rows, 1486252801 ,1495065600 ,144000, None)
        for i in range(1486252801,1487808000,144000):
            self.assertTrue(dataset.get_data().has_key(i))

    def test013_get_data_no_discretize(self):
        rows = FakeRows("10:,100;20:,120;30:,180")
        dataset = Dataset(rows, 0 , 30, None, None)
        self.assertEquals(dataset.get_data(),{10: {'MAX': None, 'AVG': None}, 20: {'MAX': 2.0, 'AVG': 2.0}, 30: {'MAX': 6.0, 'AVG': 6.0}
                                       })


    def test014_get_data_gauge_no_discretize(self):
        rows = FakeRows("10:100,;20:120,;30:180,")
        dataset = Dataset(rows, 0 , 30, None, None)
        self.assertEquals(dataset.get_data(),{10: {'MAX': 100, 'AVG': 100}, 20: {'MAX': 120, 'AVG': 120}, 30: {'MAX': 180, 'AVG': 180}
                                       }) 


    def test015_get_data_gauge_with_tags(self):
        rows = FakeRows("tag1@10:100,;tag2@20:120,;tag1@30:180,")
        dataset = Dataset(rows, 0 , 30, None, 'tag2')
        self.assertEquals(dataset.get_data(),{20: {'MAX': 120.0, 'AVG': 120.0}})


    def test016_test_interpolation(self):
        rows = FakeRows("10:5,;20:15,;30:10,;32:0,")
        dataset = Dataset(rows, 10, 35,  5, None)
        dataset.interpolate(5)
        self.assertEquals(dataset.get_data(),{25: {'MAX': 12.5, 'AVG': 10.0}, 10: {'MAX': 5.0, 'AVG': 5.0}, 20: {'MAX': 15.0, 'AVG': 15.0}, 30: {'MAX': 10.0, 'AVG': 5.0}, 15: {'MAX': 10.0, 'AVG': 10.0}})
    

    def test017_test_interpolation_hole_too_big(self):
        #interval between 20 and 35 is too big so interpolation wont be made
        rows = FakeRows("10:5,;20:15,;35:10,;37:0,")
        dataset = Dataset(rows, 10, 40,  5, None)
        dataset.interpolate(5)
        self.assertEquals(dataset.get_data(),{25: {'MAX': None, 'AVG': None}, 10: {'MAX': 5.0, 'AVG': 5.0}, 20: {'MAX': 15.0, 'AVG': 15.0}, 30: {'MAX': None, 'AVG': None}, 15: {'MAX': 10.0, 'AVG': 10.0}, 35: {'MAX': 10.0, 'AVG': 5.0}})


    def test020_get_unknown_metadata(self):
        try:
            config = CastorConfiguration(CONF_FILE)
        except IOError:
                self.skipTest("Not a complete system with cassandra (netstat.conf is not present) some tests will be skipped.")
        self.metadatastorage= MetaDataStorage(config)	
        try:
                self.metadatastorage.delete_metadata('test')
	except Exception:
	    pass	
	with self.assertRaises(MetaDataStorage.NoSuchMetaData):
            self.metadatastorage.get_metadata('test')

    def test021_create_retrieve_and_update_metadata(self):
        try:
            config = CastorConfiguration(CONF_FILE)
        except IOError:
                self.skipTest("Not a complete system with cassandra (netstat.conf is not present) some tests will be skipped.")
        self.metadatastorage= MetaDataStorage(config)    
	md = self.metadatastorage.create_metadata('test')
	md.set_last_agregated(10)
        get_md = self.metadatastorage.get_metadata('test')
        self.assertEquals(get_md.last_agregated, 10)


    def test022_test_cassandra_insert_and_delete(self):
        #the test below don't test that metadata has been realy inserted in cassandra
        try:
            config = CastorConfiguration(CONF_FILE)
        except IOError:
                self.skipTest("Not a complete system with cassandra (netstat.conf is not present) some tests will be skipped.")
        self.metadatastorage= MetaDataStorage(config)
        md = self.metadatastorage.get_metadata('test')
        self.assertEquals(md.last_agregated, 10)
        self.metadatastorage.delete_metadata('test')


    
    def test023_operation_with_intepolation(self):
        rows = FakeRows("10:5,;20:15,;30:10,;32:0,")
        dataset1 = Dataset(rows, 10, 35,  5, None)
        rows = FakeRows("15:0,;25:10,;35:10,")
        dataset2 = Dataset(rows, 10, 35,  5, None)
        operation = Operation("var1:UNDEF:0,var2,+")
        operation.set_dataset('var1', dataset1)
        #interpolation --> 10:5 15:10 20:15 25:10 30:5
        operation.set_dataset('var2', dataset2)
        #interpolation --> 15:0 20:5 25:10 30:10 35:10
        self.assertEquals(operation.evaluate(15), 10)
        self.assertEquals(operation.evaluate(20), 20)
        self.assertEquals(operation.evaluate(25), 20)
        self.assertEquals(operation.evaluate(30), 15)
        
    def test024_operation_with_intepolation_hole_too_big(self):
        rows = FakeRows("10:5,;12:5,;20:15,;34:10,;35:10,")
        dataset1 = Dataset(rows, 10, 35,  5, None)
        rows = FakeRows("15:0,;25:10,;35:10,")
        dataset2 = Dataset(rows, 10, 35,  5, None)
        operation = Operation("var1:UNDEF:0,var2,+")
        operation.set_dataset('var1', dataset1)
        #interpolation --> 10:5 15:10 20:15 25:None 30:None 35:10
        operation.set_dataset('var2', dataset2)
        #interpolation --> 15:0 20:5 25:10 30:10 35:10
        self.assertEquals(operation.evaluate(15), 10)
        self.assertEquals(operation.evaluate(20), 20)
        self.assertEquals(operation.evaluate(25), 10)
        self.assertEquals(operation.evaluate(30), 10)


    def test030_post_datasource_api(self):
        try:
            engine = CastorEngine(CONF_FILE)
            api = DataSourcesApi(engine)
        except IOError:
            self.skipTest("Not a complete system with cassandra (netstat.conf is not present) some tests will be skipped.")
        date = int(time.time())
        cass_req = CassandraRequester()
        cass_req.purge_ds('test_api_post',date)
        api.post('test_api_post', {'ds_infos':'Test automatise', 'values': {date:10, date+30:30, date+60:5, date+90:15}})
        get = api.get('test_api_post')
        self.assertEquals(get['last_inserted_ts'], date+90)
        self.assertEquals(cass_req.get_raw('test_api_post', date)[date+30]['gauge'], 30)
        #same test inserting a counter
        api.post('test_api_post', {'ds_infos':'Test automatise', 'values_type':'counter', 'values': {date+120:1000}})
        self.assertEquals(cass_req.get_raw('test_api_post', date)[date+120]['counter'], 1000)
        cass_req.purge_ds('test_api_post',date)




    def test035_create_and_update_metadatas(self):
        try:
            engine = CastorEngine(CONF_FILE)
            cass_req = CassandraRequester()
        except IOError:
            self.skipTest("Not a complete system with cassandra (netstat.conf is not present) some tests will be skipped.")
        cass_req.delete_metadata('00:00')
        engine.update_or_create_metadata('00:00')
        #engine.update_or_create_metadata('00:00',first_raw=None, last_agregated=None, raw_retention=None, computed_retention=None)
        #request in cassandra database created metadata attributes
        metadata =  cass_req.get_metadata_as_array('00:00')
        self.assertEquals(metadata['first_raw'], None)
        self.assertEquals(metadata['last_agregated'], None)
        self.assertEquals(metadata['raw_retention'], 13*31*86400) #default 13 months
        self.assertEquals(metadata['computed_retention'], 5*366*86400) #default 5 years
        engine.update_or_create_metadata(ds_name='00:00',raw_retention=5*366*86400)
        engine.update_or_create_metadata(ds_name='00:00',computed_retention=10*366*86400)
        engine.update_or_create_metadata(ds_name='00:00',first_raw=10,last_agregated=900)
        metadata =  cass_req.get_metadata_as_array('00:00')
        self.assertEquals(metadata['first_raw'], 10)
        self.assertEquals(metadata['last_agregated'], 900)
        self.assertEquals(metadata['raw_retention'], 5*366*86400) #default 13 months
        self.assertEquals(metadata['computed_retention'], 10*366*86400) #default 5 years


    def test040_put_and_get_values_one_day(self):
        """
        Full test
        We store datas concerning 2 datasources
        We evaluate a rpn expression using this 2 datasources
        """
        ts = int(time.time()/86400)*86400 + 43200 - 365 * 86400 #1 year ago at 12H00 to avoid periods between two days
        try:
            storage = CastorEngine(CONF_FILE)
            cass_req = CassandraRequester()
            cass_req.purge_ds('01:00',ts)
            cass_req.purge_ds('01:10',ts)
        except IOError:
            self.skipTest("Not a complete system with cassandra (netstat.conf is not present) some tests will be skipped.")
        ts = int(time.time()) - 365 * 86400 #1 year ago
        storage.insert_collected_values('var1', 'g', {ts+10 : 5.0, ts+20: 10.0, ts+30: 20.5, ts+40: 16}, use_batch=False)
        storage.insert_collected_values('var2', 'g', {ts+10 : 100, ts+20: 15.0, ts+30: 22, ts+40:1 }, use_batch=False)
        evaluated_expr = storage.eval_cdef('var1,var2,+,10,*', ts, ts+50, 10)
        self.assertEquals(evaluated_expr[ts+10]['AVG'], 1050)
        self.assertEquals(evaluated_expr[ts+40]['AVG'], 170)

    def test041_put_and_get_values_two_day(self):
        """
        Full test
        We store datas concerning 2 datasources
        We evaluate a rpn expression using this 2 datasources
        with a range that exceed period of one day two mak two extractions
        """
        date = time.time()
        ts = int(date/86400)*86400 + 43200 - 365 * 86400 #1 year ago at 12H00 to avoid periods between two days
        try:
            cass_req = CassandraRequester()
            cass_req.purge_ds('var1',ts)
            cass_req.purge_ds('var2',ts)
            storage = CastorEngine(CONF_FILE)
        except IOError:
            self.skipTest("Not a complete system with cassandra (netstat.conf is not present) some tests will be skipped.")
        storage.insert_collected_values('var1', 'g', {ts+10 : 5.0, ts+20: 10.0, ts+30: 20.5, ts+40: 16,
                                                       ts+86410: 12, ts+86420: 20, ts+86430: 50}, use_batch=False)
        storage.insert_collected_values('var2', 'g', {ts+10 : 100, ts+20: 15.0, ts+30: 22, ts+40:1,
                                                       ts+86410: 58, ts+86420: 40, ts+86430: 20}, use_batch=False)
        evaluated_expr = storage.eval_cdef('var1,var2,+,10,*', ts, ts+90000, 10)
        self.assertEquals(evaluated_expr[ts+10]['AVG'], 1050)
        self.assertEquals(evaluated_expr[ts+40]['AVG'], 170)
        self.assertEquals(evaluated_expr[ts+86410]['AVG'], 700)
        self.assertEquals(evaluated_expr[ts+86420]['AVG'], 600)


    def test042_put_archive_and_get_values_two_day(self):
        date = time.time()
        ts = int(date/(86400*7))*(86400*7) + (86400*7/2) - 365 * 86400 #1 year ago at the middle of the week 
        try:
            cass_req = CassandraRequester()
            cass_req.purge_ds('var1', ts)
            cass_req.purge_ds('var2', ts)
            storage = CastorEngine(CONF_FILE)
        except IOError:
            self.skipTest("Not a complete system with cassandra (netstat.conf is not present) some tests will be skipped.")
        storage.insert_collected_values('var1', 'g', {ts-20000 : 2.0, ts+10 : 5.0, ts+20: 10.0, ts+30: 20.5, ts+40: 16,
                                                       ts+86410: 23, ts+86420: 20, ts+86430: 50}, use_batch=False)
        storage.insert_collected_values('var2', 'g', {ts-20000 : 2.0, ts+10 : 100, ts+20: 15.0, ts+30: 22, ts+40:1,
                                                       ts+86410: 58, ts+86420: 40, ts+86430: 22}, use_batch=False)
        storage.archive_ds('var1', True, ts+86400*7) #samples on which we work are situated during the first two days after ts 
        storage.archive_ds('var2', True, ts+86400*7) #so it's not necessary to archive data after the first week after ts
        #0.5 days ==> 43200s weekly step=3000 => shift = 1200s
        #req_ts = ts - 1200
        self.assertEquals(cass_req.get_wmax('var1', ts), 20.5) 
        self.assertEquals(cass_req.get_wmax('var1', ts+86400), 50)
        #evaluated_expr = storage.eval_cdef('01:00,01:10,+', 0, 90000, 3000)#step >= 3000 ---> we use weekly data
        evaluated_expr = storage.eval_cdef('var1,var2,+', ts-3000, ts+30*3000, 3000)#step >= 3000 ---> we use weekly data
        #first ts (req_ts) not available because first_raw_data > req_ts and loop starts at first_raw_data
        self.assertEquals(evaluated_expr[ts+29*3000]['AVG'], 71)


    def test043_test_upper_retention(self):
        ts = int(time.time()/86400)*86400 + 43200 - 2 * 365 * 86400 #more than 13 months
        try:
            cass_req = CassandraRequester()
            cass_req.purge_ds('var1', ts)
            storage = CastorEngine(CONF_FILE)
        except IOError:
            self.skipTest("Not a complete system with cassandra (netstat.conf is not present) some tests will be skipped.") 
        storage.insert_collected_values('var1', 'g', {ts+200 : 5.0, ts+300: 10.0, ts+400: 20.5, ts+500: 16}, use_batch=False)
        storage.archive_ds('var1', True, ts+86400)
        cass_req.purge_raw('var1', ts)
        #now we are sure that datas can't be get in raw_data table
        evaluated_expr = storage.eval_cdef('var1,10,*', ts, ts+1000, 10)
        self.assertEquals(evaluated_expr[ts+300]['MAX'],205)


    
    def test_050_api_datapoints(self):
        try:
            storage = CastorEngine(CONF_FILE)
            cass_req = CassandraRequester()
            dp_api = DataPointsApi(storage)
        except IOError:
            self.skipTest("Not a complete system with cassandra (netstat.conf is not present) some tests will be skipped.")
        ts = int(time.time()) - 60 * 5
        cass_req.purge_ds('vartest', ts)
        storage.insert_collected_values('vartest','g',{ts : 5, ts+60 : 10, ts+120 : 12, 
                                                   ts+180 : 4, ts+240 : 10, ts+300 : 2})
        #without hash
        with self.assertRaisesRegexp(ApiError, "Parameter hash is required"):
            dp_api.post({'cdef_expr': "vartest,100,/","stime":ts,"etime":ts+300},'keytest')
        #with bad hash
        m = md5.new('toto')
        with self.assertRaisesRegexp(ApiError, "Bad value for parameter hash"):
            dp_api.post({'cdef_expr': "vartest,100,/","stime":ts,"etime":ts+300, "hash": m.hexdigest()},'keytest')
        #with good hash
        m = md5.new(",".join(["vartest,100,/",'keytest']))
        res = dp_api.post({'cdef_expr': "vartest,100,/","stime":ts,"etime":ts+300, "hash": m.hexdigest()},'keytest')
        self.assertEquals(len(res['AVG']), 6)
        #with step 60 5 results
        res = dp_api.post({'cdef_expr': "vartest,100,/","stime":ts,"etime":ts+300,
                           'step': 300, "hash": m.hexdigest()},'keytest')
        self.assertEquals(len(res['AVG']), 2)
        #with no hash check (second parameter no passed and no field "hash" in json)
        res = dp_api.post({'cdef_expr': "vartest,100,/","stime":ts,"etime":ts+300})
        self.assertEquals(len(res['AVG']), 6)
        #with no id_host
        res = dp_api.post({'cdef_expr': "vartest,100,/","stime":ts,"etime":ts+300})
        self.assertEquals(len(res['AVG']), 6)
        #with bad cdef
        with self.assertRaisesRegexp(ApiError, "StackError"):
            res = dp_api.post({'cdef_expr': "vartest,100","stime":ts,"etime":ts+300})
        with self.assertRaisesRegexp(ApiError, "UnknownDatasourceError"):
            res = dp_api.post({'cdef_expr': "varinexistante","stime":ts,"etime":ts+300})
        with self.assertRaisesRegexp(ApiError, "UnknownDatasourceError"):
            res = dp_api.post({'cdef_expr': "nonolemecano","stime":ts,"etime":ts+300})
        #with missing parameter
        with self.assertRaisesRegexp(ApiError, "Parameter .* is mandatory"):
            res = dp_api.post({'cdef_expr': "vartest,100,/"})
        #computed MAX
        res = dp_api.post({'cdef_expr': "vartest,100,/","stime":ts,"etime":ts+300, "func":"MAX"})
        self.assertEquals(len(res['MAX']), 6)


if __name__ == "__main__":
    unittest.main()
