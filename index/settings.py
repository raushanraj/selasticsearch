import os
INDEX_LOCK_QUEUE='index-lock-queue'
INDEX_NAME='oneindex'
DOC_SIZE_FILE='/data/size.json'
LAST_USED_SHARD='last_used_shard'
STOP_WORDS=['is','or','&','and','a']
SHARD_BASE_PATH=os.getcwd()+'/index/data/shard_'
DATA_SHARD_PATH=os.getcwd()+'/index/data/shard_%s/datafile.dat'
DATA_BASE_PATH=os.getcwd()+'/index/data/'
DOC_SIZE_RKEY='last_doc_id'
DOCMAP_FILE=os.getcwd()+'/index/data/docid_mmap.json'
SHARDS=['shard_0','shard_1','shard_2']


