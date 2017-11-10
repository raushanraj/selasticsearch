import re

from index.settings import STOP_WORDS


class ReverseIndex(object):
	def __init__(self,*args,**kwargs):
		self.docs=args[0]

	@staticmethod
	def tokenize(data):
		'''
		Split data by white spaces and filter STOP WORDS
		:param data: 
		:return: 
		'''
		return map(lambda x:x.lower(),filter(lambda x:x.lower() not in STOP_WORDS,re.split('\s+', data)))



	def generate(self):
		reverse_index={}
		for doc in self.docs:
			doc_id=doc['_id']
			for field in doc:
				if type(doc[field])==int:
					continue
				field_data=ReverseIndex.tokenize(doc[field])
				for (index,token) in enumerate(field_data):
					if token in reverse_index:
						if doc_id in reverse_index[token]:
							reverse_index[token][doc_id]['ind'].append([field,index])
							reverse_index[token][doc_id]['_c_']+=1
						else:
							reverse_index[token][doc_id]={'ind':[[field,index]],'_c_':1}
							reverse_index[token]['_tc_']+=1
					else:
						reverse_index[token]={doc_id:{'ind':[[field,index]],'_c_':1}}
						reverse_index[token]['_tc_']=1
		return reverse_index
