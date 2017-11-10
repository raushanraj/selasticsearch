import json

from flask import Flask
from flask import request

from index.SelasticSearch import SelasticSeacrh
from index.settings import DOCMAP_FILE
from queue import RedisQueue

app = Flask(__name__)
from index.settings import INDEX_LOCK_QUEUE
queue=RedisQueue(INDEX_LOCK_QUEUE)

REVERSE_INDEX=SelasticSeacrh.MERGE_SEGMENTS()
doc_memorymap=json.load(open(DOCMAP_FILE,'r'))

data=[{'desc': 'unique to this doc', 'name': 'Raushan Raj'}]

#print SelasticSeacrh(['unique'],REVERSE_INDEX,doc_memorymap).phrase_search(['raushan','now','and','now'],25,'')
SelasticSeacrh(data,REVERSE_INDEX,doc_memorymap).index()
print REVERSE_INDEX