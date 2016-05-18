# elasticsearch-mapreduce
Tools about using elasticsearch as input and output of mapreduce

### Elasticsearch index to Hdfs file
##### Command:

```shell
hadoop jar elasticsearch-mapreduce.jar org.creditease.mr.Main \
    --es_nodes es1:9200,es2:9200 \
    --es_user user \
    --es_pswd password \
    --job es2json \
    --es_resource index/type \
    --outputformat sequencefile \
    --reducernum 10 \
    --outputfile /path/to/save
```
##### Note:
- All types of index param: **es_resource index/**
- Number of reducer can be 0
- Output format: sequencefile or text, sequencefile is the only input format of **hdfs2es** job for now.

##### Output:
- key: id
- value: json format doc.

### Hdfs file to Elasticsearch
##### Command:

```shell
hadoop jar elasticsearch-mapreduce.jar org.creditease.mr.Main \
    --es_nodes es1:9200,es2:9200 \
    --es_user user \
    --es_pswd password \
    --job hdfs2es \
    --inputfile /path/to/save \
    --es_resource index/type \
    --es_mapping_id key_field_name \
    --es_mapping_setid false
```

##### Input
- Input format should be sequencefile. Value is json format string.
- If you want to set key as the key of the doc in es, and the key isnot in json, set **es_mapping_id** to **id** and **es_mapping_setid** to **true**. This setting will add a key id in your doc.
- If you want to set key as one column in json, set **es_mapping_id** to your column name, and **es_mapping_setid** to **false**.
- If you want to generate key by itself, set **es_mapping_id** to **null**, and **es_mapping_setid** to **false**.
