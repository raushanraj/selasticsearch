import os

import redis

from index.settings import LAST_USED_SHARD
from indexing import ReverseIndex
from segmenting import Segment


class Shard(object):
	'''
	Sharding is for horizontal scaling along multiple node, current implementation is for Single node only
	'''
	def __init__(self,*args,**kwargs):
		self.docs=args[0]
		self.base_path=os.getcwd()+'/index/data/shard_'
		self.path=os.getcwd()+'/index/data/shard_0'
		self.REVERSE_INDEX=args[1]
		self.doc_memorymap = args[2]

	def round_robin(self):
		r = redis.StrictRedis()
		shard_num=r.get(LAST_USED_SHARD)
		if shard_num is None:
			shard_num=-1
		shard_num=int(shard_num)
		if os.path.isdir(self.base_path+str(shard_num+1)):
			pass
		else:
			shard_num=-1
		self.path = self.base_path + str(shard_num + 1)+"/"
		r.set(LAST_USED_SHARD, shard_num + 1)
		reverse_index=ReverseIndex(self.docs).generate()
		return Segment(self.path,self.docs,reverse_index,shard_num+1,self.REVERSE_INDEX,self.doc_memorymap).commit()
