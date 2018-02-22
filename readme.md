Test with api:
==============
Configure demo/castor.conf

Run in a shell
python demo/api.py


In another terminal:

#to post values for a variable_name 'testapi' that will be created if not exists

curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' -d '{"ds_infos":"test via api", "values_type":"gauge", "values":{"1515167463":8,"1515167493":16}}' 'http://127.0.0.1:8000/datasources/testapi'

{
    "id": "testapi", 
    "success": true
}

#variable has been created

curl http://127.0.0.1:8000/datasources/testapi

{
    "computed_retention": 158112000, 
    "ds_infos": "test via api", 
    "ds_name": "testapi", 
    "first_raw": null, 
    "last_agregated": null, 
    "last_inserted_ts": 1515167493, 
    "raw_retention": 34819200
}

#to post values for another variable

curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' -d '{"ds_infos":"test via api", "values_type":"gauge", "values":{"1515167462":10,"1515167496":30}}' 'http://127.0.0.1:8000/datasources/testapi2'
{
    "id": "testapi2", 
    "success": true
}


#to request values corresponding to (testapi1+testapi2)*10


curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json' -d '{"cdef_expr": "testapi,testapi2,+,10,*","stime":1515167460,"etime":1515167500}' 'http://127.0.0.1:8000/datapoints'

{
    "AVG": [
        [
            1515167460000, 
            180.0
        ], 
        [
            1515167490000, 
            460.0
        ], 
        [
            1515167500000, 
            null
        ]
    ], 
    "cdef": "testapi,testapi2,+,10,*", 
    "etime": 1515167500, 
    "stime": 1515167460
}
