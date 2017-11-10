import json

from flask import Flask
from flask import request

from index.SelasticSearch import SelasticSeacrh
from index.settings import DOCMAP_FILE
from queue import RedisQueue

app = Flask(__name__)
from index.settings import INDEX_LOCK_QUEUE
queue=RedisQueue(INDEX_LOCK_QUEUE)
CACHE={}
REVERSE_INDEX=SelasticSeacrh.MERGE_SEGMENTS()
doc_memorymap=json.load(open(DOCMAP_FILE,'r'))
@app.route('/')
def hello_world():
    return 'Selasticsearch is up now'

@app.route('/search')
def search():
    q=request.args['q']
    cache_q=q
    field=request.args.get('field','')
    q=map(lambda x:x.lower(),q.split())
    search=SelasticSeacrh([],REVERSE_INDEX,doc_memorymap)
    if len(q)>1:
        if cache_q in CACHE:
            return json.dumps(CACHE[cache_q])
        else:
            temp_cache=search.phrase_search(q,25,field)
            CACHE[cache_q]=temp_cache
            return json.dumps(temp_cache)
    else:
        return json.dumps(search.keyword_search(q,25,field))


@app.route('/index', methods=['POST'])
def index():
    global REVERSE_INDEX
    global doc_memorymap
    if request.method=="POST":
        #queue.put(json.dumps(request.data))
        data=json.loads(request.data)
        if type(data)!=list:
            data=[data]
        SelasticSeacrh(data,REVERSE_INDEX,doc_memorymap).index()
        return json.dumps({'msg': 'data is indexed'})
    else:
        return json.dumps({'msg':'method not supported to index'})

if __name__=="__main__":
    app.run(port=5000)