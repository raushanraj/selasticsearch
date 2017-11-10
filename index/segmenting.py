import json
import os
import uuid
import math,redis

from index.settings import DATA_SHARD_PATH, DOCMAP_FILE,DOC_SIZE_RKEY

rconn=redis.StrictRedis()
class Segment(object):
	def __init__(self,*args,**kwargs):
		self.path=args[0]
		self.data=args[1]
		self.reverse_index=args[2]
		self.shard_num=args[3]
		self.REVERSE_INDEX=args[4]
		self.doc_memorymap=args[5]

	def commit(self):
		self.merge_segment_data()
		self.save_original_data()
		self.save_segment_index()
		return self.REVERSE_INDEX

	def save_original_data(self):
		fileobj=open(DATA_SHARD_PATH%str(self.shard_num),'a+')
		now_size=os.path.getsize(DATA_SHARD_PATH%str(self.shard_num))
		for doc in self.data:
			jsonDoc=json.dumps(doc)
			jsonSize=len(jsonDoc)
			fileobj.write(jsonDoc+"\n")
			self.doc_memorymap[doc['_id']] = [self.shard_num,now_size]
			now_size+=jsonSize
			json.dump(self.doc_memorymap,open(DOCMAP_FILE,'w+'))
		fileobj.close()

	def merge_segment_data(self):
		for key in self.reverse_index:
			if key not in self.REVERSE_INDEX:
				self.REVERSE_INDEX[key]=self.reverse_index[key]
			else:
				self.REVERSE_INDEX[key].update(self.reverse_index[key])
			maxDocs = int(rconn.get(DOC_SIZE_RKEY))
			idf_term = 1 + math.log(maxDocs / (self.REVERSE_INDEX[key]['_tc_'] + 1))
			self.REVERSE_INDEX[key]['idf']=idf_term

	def save_segment_index(self):
		segment_name=uuid.uuid4().hex
		json.dump(self.reverse_index,open(self.path+segment_name+'.json','w+'))
