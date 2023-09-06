import plotly.graph_objects as go
import tabulate
import networkx as nx
from matplotlib.pyplot import hist
import matplotlib.pyplot as plt
from IPython.display import HTML, display
import sys,json,operator
import Query
import warnings
warnings.filterwarnings('ignore')

dbpedia_concepts=[]
f=open("dbpedia_concepts.txt","r")
for line in f:
	line=line.strip()
	line=line.lower()
	dbpedia_concepts.append(line)

wikidata_concepts=[]
f=open("wikidata_concepts.txt","r")
for line in f:
	line=line.strip()
	line=line.lower()
	wikidata_concepts.append(line)

wikidata_concepts=list(set(wikidata_concepts))
dbpedia_concepts=list(set(dbpedia_concepts))

class semantic_mapping:
	def __init__(self,df,header=None,content=None):
        #Input data set 
		self.df = df
		self.column_charac={}	
		self.header=list(df.columns)
		self.content={}#content
		for column in self.header:
			self.content[column]=df[column]
		self.concept_lst=[]
		self.dataset_topic=''
		self.new_columns=[]
	'''def __init__(self,header,content):
        #Input data set 
		#self.df = df
		self.header=header
		self.content=content
		self.concept_lst=[]
		self.dataset_topic=''
	'''
	def index_concepts(self,dbpedia_concepts,wikidata_concepts):
		dbpedia_index_map={}
		for c in dbpedia_concepts:
			lst=c.split()
			for token in lst:
				block=[]
				if token in dbpedia_index_map.keys():
					block=dbpedia_index_map[token]
				block.append(c)
				dbpedia_index_map[token]=block
		wikidata_index_map={}
		for c in wikidata_concepts:
			lst=c.split()
			for token in lst:
				block=[]
				if token in wikidata_index_map.keys():
					block=wikidata_index_map[token]
				block.append(c)
				wikidata_index_map[token]=block

		return (dbpedia_index_map,wikidata_index_map)
		


	def prepare_dataset(self,filename):
	    #Dataset File
	    data = filename.read()
	    filename.close()
	    data_text = data.decode('cp866', 'replace')
	    table_json = json.loads(data_text)


	    column_lst=table_json["relation"]
	    headers=[]
	    data_content={}
	    #print ("Columns of the table are")
	    for column in column_lst:
	        #iter=0
	        #print (column[0],column)
	        headers.append(column[0])
	        data_content[column[0]]=column[1:]
	    self.header=headers
	    self.content=data_content
	    return headers,data_content

	#Write a matcher between header and concept in the list

	
	#print (header)
	'''
	iter=0
	while iter<=len(content[header[0]]):
		if iter==0:
			print (','.join(header))
		else:
			out=[]
			j=0
			while j<len(header):
				out.append(content[header[j]][iter-1])
				j+=1
			print (','.join(out))
		iter+=1
	'''
	#print (content)

	def get_column_concept_without_viz(self,column_header):
		column_lst=self.df[column_header]
		options=[]
		for val in column_lst:
			#print ("before",val,options)
			options.extend(Query.get_rels(str(val)))
			#print ("after",val,options)
		orig_options=options
		options=list(filter(lambda a: a != "NotFound", options))#options.remove('NotFound')
		if len(options)==0:
			return ""

		uniq_options=list(set(options))
		option_count={}
		for op in uniq_options:
			option_count[op]=options.count(op)

		sorted_lst = sorted(option_count.items(), key=operator.itemgetter(1),reverse=True)


		#print (sorted_lst)

		concept= (max(options,key=options.count))
		#print ("Concept is ",concept)
		return concept
	def get_column_concept(self,column_header):
		column_lst=self.df[column_header]
		options=[]
		for val in column_lst:
			#print ("before",val,options)
			options.extend(Query.get_rels(val))
			#print ("after",val,options)
		orig_options=options
		options=list(filter(lambda a: a != "NotFound", options))#options.remove('NotFound')
		if len(options)==0:
			return ""

		uniq_options=list(set(options))
		option_count={}
		for op in uniq_options:
			option_count[op]=options.count(op)

		sorted_lst = sorted(option_count.items(), key=operator.itemgetter(1),reverse=True)


		#print (sorted_lst)

		concept= (max(options,key=options.count))
		print ("Concept is ",concept)
		print ("Fraction of rows matching this concept",option_count[concept]*1.0/len(column_lst))
		
		print ("Other options ")
		#Turned off for now
		fig=plt.figure(figsize=(10, 4), dpi= 80, facecolor='w', edgecolor='k')
		keylst=[]
		vallst=[]
		for (a,b) in sorted_lst:
			keylst.append(a.strip())
			vallst.append(b)
		plt.bar(range(len(keylst)), list(vallst), align='center')
		#plt.bar(range(len(keylst)), list(self.predict_correlation.values()), align='center')
		plt.title('Fraction of hits')
		plt.xticks(range(len(sorted_lst)), list(keylst), rotation='vertical')
		plt.show()

		#Not used
		'''
		table=[]#['Value','Url']]
	        while iter<min(5,len(example_hits)):
	            table.append([example_hits[iter],'<a href="http://dbpedia.org/page/'+example_hits[iter]+'">http://dbpedia.org/page/'+example_hits[iter]+'</a>'])#print (example_hits[iter])
	            iter+=1
	        #df1=(pd.DataFrame(table, columns=["Foo", "Bar"]))
	        display(HTML(tabulate.tabulate(table, tablefmt='html',  headers=['Value','Url'],stralign='center')))
	        
		'''
		return

	def identify_new_feat(self,column_lst,column_header):

		new_feat_options=[]
		feat_lst=[]
		iter=0
		count=0
		for val in column_lst:
			#print (iter,len(orig_options))
			#if orig_options[iter]==concept:
			new_feat=Query.get_features(val)
			new_feat_options.extend(list(new_feat.keys()))
			feat_lst.append(new_feat)
			count+=1
				#print (new_feat)
			iter+=1
		new_feat_options=list(set(new_feat_options))
		#print (new_feat_options)

		final_dic={}
		for feat in new_feat_options:
			final_dic[feat]=[]

		for feat in feat_lst:
			for key in feat.keys():
				lst=final_dic[key]
				if not isinstance(feat[key],list):
					lst.append(feat[key])
				else:
					lst.extend(list(set(feat[key])))
				final_dic[key]=lst
		#print (final_dic)
		count_dic={}
		for key in final_dic:
			lst=final_dic[key]
			try:
				#print (lst)
				uniq_terms=list(set(lst))
				for v in uniq_terms:
					#print (final_dic['instance of'].count(concept),concept)
					#print (final_dic.keys())
					#print ("more than 90percent",v,lst.count(v),final_dic['instance of'].count(concept),key)
					count_dic[v+";"+key]=lst.count(v)*1.0/len(column_lst)
					#if lst.count(v)*1.0/final_dic['instance of'].count(concept) > 0.8:
					#	print ("more than 90percent",v,lst.count(v),final_dic['instance of'].count(concept),key)
			except:
				continue
		sorted_lst = sorted(count_dic.items(), key=operator.itemgetter(1),reverse=True)

		#i=0
		#while i<3:
		#	print (sorted_lst[i])
		#	i+=1
		#print (sorted_lst)
		return (sorted_lst[:2]) 

	def get_dataset_concepts(self):
		if len(self.concept_lst)>0:
			iter=0
			while iter<len(self.concept_lst):
				column=self.header[iter]
				print ("Concept for column named ",column," is: ",self.concept_lst[iter])
				iter+=1
			return
		for column in self.header:
			column_list=self.content[column]
			#print ("column list",column_list)
			self.concept_lst.append (self.get_column_concept_without_viz(column))
			if len(self.concept_lst[-1])==0:
				print ("Concept for column named ",column," is: not found")#,self.concept_lst[-1])
			else:
				print ("Concept for column named ",column," is: ",self.concept_lst[-1])
			if len(self.concept_lst[-1])>0:
				self.column_charac[column]=self.identify_new_feat(column_list,column)

		#print ("headers",self.header)
		#print ("identified concepts",self.concept_lst)
		self.dataset_topic=self.get_dataset_topic()
		print ("Data set is about:",self.dataset_topic)
	def plot_new_feat():
		self.concept_lst=[]
		for column in self.header:
			column_list=self.content[column]
			print ("processing  column ",column)
			print ("new columns")
			print (self.column_charac)


	def get_dataset_topic(self):
		i=0
		total_dic={}
		while i<len(self.content[self.header[0]]):
			valuelst=[]
			for j in range(len(self.header)):
				valuelst.append(self.content[self.header[j]][i])
			#print (valuelst)
			count_dic=Query.get_dataset_topic(valuelst,self.concept_lst)
			for v in count_dic:
				if v in total_dic:
					total_dic[v]+=count_dic[v]
				else:
					total_dic[v]=count_dic[v]
			i+=1
		#print (total_dic)
		self.dataset_topic=max(total_dic.items(), key = operator.itemgetter(1))[0]
		index=self.concept_lst.index(self.dataset_topic)

		#print ("relationship between columns",Query.get_relationship(self.concept_lst,index,self.content,self.header))

		try:
			return (max(total_dic.items(), key = operator.itemgetter(1))[0])
		except:
			return ""


	def identify_new_columns(self,topic_index):
		#topic_index=self.concept_lst.index(self.dataset_topic)
		column=self.content[self.header[topic_index]]
		new_content={}
		for val in column:
			newcolumns=Query.identify_new_columns(val,self.dataset_topic)
			for v in newcolumns:
				if v in new_content.keys():
					new_content[v]+=1
				else:
					new_content[v]=1
			#print (val,newcolumns)
		sorted_lst = sorted(new_content.items(), key=operator.itemgetter(1),reverse=True)

		dont_consider=['types','label','id','factsArray','description_exact','facts','title','instance of','factsCount','label_exact']
		final_options=[]
		for (c,v) in sorted_lst:
			if c in dont_consider:
				continue
			if v*1.0/len(column)> 0.2:
				#print (c,v*1.0/len(column))
				final_options.append((c,v*1.0/len(column)))
		#print (new_content,len(column))
		self.new_columns=final_options

		return_op=[]
		for (u,v) in final_options:
			return_op.append(u)
		return return_op[:2]

	def show_new_samples(self,topic_index,new_columns):
		#topic_index=self.concept_lst.index(self.dataset_topic)
		column=self.content[self.header[topic_index]]
		new_content={}
		for v in new_columns:
			new_content[v]=[]
		for val in column:
			newvalues=Query.identify_new_columns_content(val,self.dataset_topic,new_columns)
			for v in new_columns:
				lst=new_content[v]
				lst.append(newvalues[v])

		print (new_content)
	def display_new_columns(self):
		new_cols={}
		iter=0
		while iter<len(self.header):
			if len(self.concept_lst[iter])>0:		
				new_cols[iter]=self.identify_new_columns(iter)
			iter+=1
		l1=[]
		l2=[]
		iter=0
		for c in self.header:
			if len(self.concept_lst[iter])==0:
				l1.append((c,"not found"))#concept_lst[iter]))
			else:
				l1.append((c,self.concept_lst[iter]))
			if iter in new_cols.keys():
				l2.extend(new_cols[iter])
			iter+=1
		self.plot_graph(l1,l2,self.dataset_topic,[(1,1),(1,2),(2,3),(2,4)],[],"author")


	def get_summary(self):
		iter=0
		table=[]#['Value','Url']]
		while iter<len(self.header):
			table.append([self.header[iter],self.concept_lst[iter]])#[example_hits[iter],'<a href="http://dbpedia.org/page/'+example_hits[iter]+'">http://dbpedia.org/page/'+example_hits[iter]+'</a>'])#print (example_hits[iter])
			iter+=1
		display(HTML(tabulate.tabulate(table, tablefmt='html',  headers=['Column header','Concept'],stralign='center')))

		#print ()
		#print (self.concept_lst)
		print ("The dataset is about: ",self.dataset_topic)
		print ("The pivotal column corresponds to ", self.header[self.concept_lst.index(self.dataset_topic)])
		print ("\n\n")
		#print (self.column_charac)
		print ("Column characteristics")
		for token in self.column_charac.keys():
			print ("\nColumn name:", token)
			print (self.column_charac[token][0][1]," fraction of values have '",self.column_charac[token][0][0].split(';')[1],"' attribute value= ",self.column_charac[token][0][0].split(';')[0])
			print (self.column_charac[token][1][1]," fraction of values have '",self.column_charac[token][1][0].split(';')[1],"' attribute value= ",self.column_charac[token][1][0].split(';')[0])
		#print (self.column_charac)
		return
	def change_concept(self,cname,newconcept):
		index=self.header.index(cname)
		self.concept_lst[index]=newconcept
		print ("Changed concept value for ",cname)

	def plot_graph(self,l1,l2,column_name,edges,node_val,attrname):
        #Plot the graphical snapshot of the knowledge graph
        	N=1+len(l1)+len(l2)
        	G = nx.random_geometric_graph(N, 0.0)

	        curr=0
	        G.node[curr]['pos']=[0.4,1.5]
	        G.node[curr]['text']="Table is about: "+column_name
        	curr+=1
        	while curr<1+len(l1):
            		G.node[curr]['pos']=[0.1*curr+0.2,1.3]
            		G.node[curr]['text']=l1[curr-1][1]#l1[curr-1][0]
            		#print (curr,G.node[curr])
            		G.add_edge(0,curr)
            		curr+=1

        	while curr<1+len(l1)+len(l2):
             		G.node[curr]['pos']=[0.1*(curr-len(l1))+0.2,1.1]
             		G.node[curr]['text']=l2[curr-1-len(l1)]#l2[curr-1-len(l1)]+":  "+str(node_val[curr-1-len(l1)][0])+" ("+str(node_val[curr-1-len(l1)][1])+"%)"
            	 	#print (curr,G.node[curr])
             		curr+=1
        	node_adjacencies = []
        	node_text = []
        	for node, adjacencies in enumerate(G.adjacency()):
        	    node_adjacencies.append(len(adjacencies[1]))
        	    #print (node)
        	    node_text.append(G.node[node]['text'])

        	for (a,b) in edges:
        	     G.add_edge(a,b+len(l1))
        	edge_x = []
        	edge_y = []
        	edge_text=[]
        	for edge in G.edges():
        	    x0, y0 = G.node[edge[0]]['pos']
        	    x1, y1 = G.node[edge[1]]['pos']
        	    edge_x.append(x0)
        	    edge_x.append(x1)
	            edge_x.append(None)
        	    edge_y.append(y0)
        	    edge_y.append(y1)
        	    edge_y.append(None)
        	    edge_text.append(attrname)

        	edge_trace = go.Scatter(
        	x=edge_x, y=edge_y,
        	line=dict(width=1, color='#888'),
        	text=edge_text,
        	mode='lines')

        	node_x = []
        	node_y = []
        	for node in G.nodes():
        	    x, y = G.node[node]['pos']
        	    node_x.append(x)
        	    node_y.append(y)

        	node_trace = go.Scatter(
        	x=node_x, y=node_y,
        	mode='markers+text',
       	 	hoverinfo='text',
        	text=node_text,
        	textposition="top center",
        	marker=dict(
        	showscale=False,
        	# colorscale options
        	# 'Greys' | 'YlGnBu' | 'Greens' | 'YlOrRd' | 'Bluered' | 'RdBu' |
        	#' Reds' | 'Blues' | 'Picnic' | 'Rainbow' | 'Portland' | 'Jet' |
        	#'Hot' | 'Blackbody' | 'Earth' | 'Electric' | 'Viridis' |
        	colorscale='YlGnBu',
        	reversescale=True,
        	color=[],
        	size=40,
        	colorbar=dict(
        	    thickness=15,
        	    title='Node Connections',
        	    xanchor='left',
        	    titleside='right'
        	),
        	line_width=2))
        	node_trace.marker.color = node_adjacencies
        	node_trace.text = node_text
        	fig = go.Figure(data=[edge_trace, node_trace],
        	      layout=go.Layout(
        	        title='<br>Dataset contents',
        	        titlefont_size=16,
        	        showlegend=False,
        	        hovermode='closest',
        	        margin=dict(b=20,l=5,r=5,t=40),
        	        annotations=[ ],
        	        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        	        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
        	        )
        	fig.update_xaxes(range=[-0.2, 0.8])
        	fig.add_annotation(
        	x=0.22,
        	y=1.335,
        	xref="x",
        	yref="y",
        	text="Concepts",
        	showarrow=False,
        	align="center",
       		bgcolor="#ff7f0e",
        	opacity=0.8
        	)
        	fig.add_annotation(
        	x=0.2,
        	y=1.13,
        	xref="x",
        	yref="y",
        	text="New Columns/Concepts",
        	showarrow=False,
        	align="center",
        	bgcolor="#ff7f0e",
        	opacity=0.8
        	)
        	fig.show()
if __name__ == '__main__':		
	sm = semantic_mapping()
	filename=open(sys.argv[1],"rb")

	(header,content)=sm.prepare_dataset(filename)
	#(dbpedia_index_map,wikidata_index_map)=sm.index_concepts(dbpedia_concepts,wikidata_concepts)
	sm.get_dataset_concepts()

	sm.get_dataset_topic()
	print ("New columns ",sm.identify_new_columns())

	sm.show_new_samples(['author'])

	sm.get_summary()
