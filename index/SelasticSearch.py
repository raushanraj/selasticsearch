from __future__ import division
import json
import os
import math
import redis
from glob import glob

from index.settings import DOC_SIZE_RKEY,DATA_SHARD_PATH
from index.settings import SHARDS
from sharding import Shard

#last_doc_id=json.load(open(DOC_SIZE_FILE,'r'))['last_doc_id']
rconn=redis.StrictRedis()


class SelasticSeacrh(object):
	def __init__(self,*args,**kwargs):
		self.data=args[0]
		self.REVERSE_INDEX=args[1]
		self.doc_memorymap=args[2]
		#self.queue=RedisQueue(INDEX_LOCK_QUEUE)

	def index(self):
		'''
		start fetching from queue continously , it can be scaled.
		:return: 
		'''

		last_doc_id=rconn.get(DOC_SIZE_RKEY)
		if last_doc_id is None:
			last_doc_id=1
		last_doc_id=int(last_doc_id)
		queue_data=[]
		for doc in self.data:
			last_doc_id+=1
			doc['_id']=last_doc_id
			queue_data.append(doc)
		rconn.set(DOC_SIZE_RKEY, last_doc_id)
		Shard(queue_data,self.REVERSE_INDEX,self.doc_memorymap).round_robin()


	@staticmethod
	def MERGE_SEGMENTS():
		final_data={}
		for shard in SHARDS:
			path=os.getcwd()+'/index/data/'+shard+"/"
			main_data=json.load(open(path+'MAIN_INDEX.json','r'))
			for file_name in glob(path+'*.json'):
				if file_name.find("MAIN_INDEX.json")!=-1:
					pass
				else:
					main_data=SelasticSeacrh.recursive_merge(main_data,json.load(open(file_name,'r')))
					#main_data.update(json.load(open(file_name,'r')))
					os.remove(file_name)
			json.dump(main_data,open(path+'MAIN_INDEX.json','w+'))
			final_data=SelasticSeacrh.recursive_merge(final_data,main_data)
			#final_data.update(main_data)
		return final_data

	@staticmethod
	def recursive_merge(d1,d2):
		for key in d2:
			if key not in d1:
				d1[key]=d2[key]
			else:
				d1[key].update(d2[key])
			maxDocs = int(rconn.get(DOC_SIZE_RKEY))
			idf_term = 1 + math.log(maxDocs / (d1[key]['_tc_'] + 1))
			d1[key]['idf'] = idf_term
		return d1



		# for i in range(0,size):
		# 	last_doc_id+=1
		# 	data=json.loads(self.queue.get_nowait())
		# 	data['_id']=last_doc_id
		# 	queue_data.append(data)
		# 	Shard(data).round_robin()
		# 	rconn.set(DOC_SIZE_RKEY,last_doc_id)
				#json.dump({'last_doc_id':last_doc_id},open(DOC_SIZE_FILE,'r'))

	def fetch_doc_from_datfile(self,docs):
		final_data=[]
		file_list=[open(DATA_SHARD_PATH%'0','r+'),open(DATA_SHARD_PATH%'1','r+'),open(DATA_SHARD_PATH%'2','r+')]
		for item in docs:
			doc=item[0]
			weight=item[1]
			mem_data=self.doc_memorymap[doc]
			shard_num=mem_data[0]
			fileobj=file_list[int(shard_num)]
			seek_to=mem_data[1]
			fileobj.seek(seek_to,0)
			doc_data=json.loads(fileobj.readline()[0:-1])
			doc_data['wt']=weight
			final_data.append(doc_data)
		return final_data

	def create_segment_by_pool(self):
		pass

	def create_segment(self):
		pass

	def keyword_search(self,query,count,field):
		if query:
			query=query[0]
		else:
			pass
		if query in self.REVERSE_INDEX:
			doc_list=self.REVERSE_INDEX[query]
			#inverse document frequency IDF
			maxDocs=int(rconn.get(DOC_SIZE_RKEY))
			idf_term=1 + math.log(maxDocs/(doc_list['_tc_'] + 1))
			docs=doc_list.keys()
			docs.remove('_tc_')
			docs.remove('idf')
			final_docs=[]
			for item in docs:
				if field:
					for val in self.REVERSE_INDEX[query][item]['ind']:
						if val[0]==field:
							final_docs.append((item, idf_term / math.sqrt(self.REVERSE_INDEX[query][item]['_c_'])))
							break

				else:
					final_docs.append((item,idf_term/math.sqrt(self.REVERSE_INDEX[query][item]['_c_'])))
			final_docs.sort(key=lambda x: x[1])
			if count=="":
				return final_docs
			return self.fetch_doc_from_datfile(final_docs[0:count])
		else:
			return {}

	def phrase_lookup(self,keyword):
		return_data=[]
		if keyword in self.REVERSE_INDEX:
			data=self.REVERSE_INDEX[keyword]
			return_data=[]
			for item in data:
				if item not in ['idf','_tc_']:
					ind=data[item]['ind']
					return_data.extend(map(lambda x:[item,x[0],x[1]],ind))
		return return_data

	def phrase_search(self,query,count,field):
		keywords=query
		activepos=self.phrase_lookup(keywords[0])
		for key in keywords[1:]:
			newactivepos=[]
			nextdocpos=self.phrase_lookup(key)
			if nextdocpos:
				for pos in activepos:
					if [pos[0],pos[1],pos[2]+1] in nextdocpos:
						newactivepos.append([pos[0],pos[1],pos[2]+1])
				activepos=newactivepos
		return self.fetch_doc_from_datfile(map(lambda x:[x[0],0],activepos))




