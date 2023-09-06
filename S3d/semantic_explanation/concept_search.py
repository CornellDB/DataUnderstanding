import pickle
import plotly.graph_objects as go
import tabulate
import networkx as nx
from matplotlib.pyplot import hist
import matplotlib.pyplot as plt
from IPython.display import HTML, display
import sys,json,operator
import Query
from IPython.display import Markdown, display
from IPython import get_ipython
import get_concept
import pandas as pd
import Query

class join_datasets:

	#ag="sovereign state"
	#tag="lake"
	#tag="political party"#international airport"
	def __init__(self,df,dataset_topic=None,concept_dic=None):
		f = open("concept.pkl","rb")
		self.concept_dic=pickle.load(f)#        pickle.dump(concept_dic,f)
		f.close()


		f = open("dataset.pkl","rb")
		self.dataset_topic=pickle.load(f)
		f.close()
		self.tag="sovereign state"#international airport"
		self.pivot_col=[]
		self.lst=[]
		self.pivot_df=df
		self.pivot_concept=[]
		self.display_pivot=self.pivot_df.head()
	def get_mocodo(self):
		str_obj=self.get_string(self.new_dataset_cols[:6],list(self.pivot_df.columns))
		#print (str_obj)
		get_ipython().run_cell_magic('mocodo', '', str_obj)#'Table, 1N Table2: column1, col2\nTable2: col,c2\nTable3: col,c2')

	def get_string(self,col_lst,central_feat):
	    lst=range(len(col_lst))
	    a=''
	    iter=0
	    while iter<int(len(lst)/2):
	        a+='NewTable'+str(iter)+", 11 InputTable"+":"
	        for l in col_lst[iter]:
	            l=l.replace('%','')
	            a+=l+","
	        a+="\n"
	        iter+=1

	    a+='\nInputTable:'+','.join(central_feat)+"\n\n"

	    while iter<len(lst):
	        a+='NewTable'+str(iter)+", 11 InputTable"+":"
	        for l in col_lst[iter]:
	            l=l.replace('%','')
	            a+=l+","
	        a+="\n"
	        iter+=1
	    return a
	def get_count(self):

		count={}
		for v in self.dataset_topic.keys():
			if self.tag in self.dataset_topic[v]:
			#if "sovereign state" in dataset_topic[v]:
				#print (v,self.dataset_topic[v])
				self.lst.append(v)
			#lst.append(dataset_topic[v])
			if self.dataset_topic[v]in count.keys():
				count[self.dataset_topic[v]]+=1
			else:
				count[self.dataset_topic[v]]=1
		#print (count)


	def get_column(self,df, conceptlst,j):
		ind=list(df.columns)[conceptlst.index(self.tag)]
		new_col_name=list(df.columns)[j]
		val_dic={}
		for index,row in df.iterrows():
			val_dic[row[ind]]=row[new_col_name]

		lst=[]
		for val in self.pivot_col:
			if val in val_dic.keys():
				lst.append(val_dic[val])		
			else:
				lst.append('')

		#print (lst)
		return lst

	def add_new_col(self,colname,df):
		df[colname]=self.new_cols[colname]
		s=df.head().style.set_properties(subset=[colname], **{'background-color': 'yellow'})
		return s
	def get_intersection(self,l1,l2):
		return len(list(set(l1)&set(l2)))
	
	def add_rows(self):
		lst=Query.get_rows("sovereign state")
		#print (self.display_pivot.columns,self.pivot_concept)
		row_lst=[]
		for val in lst:
			fcts=val['facts']
			row=[]
			row.append(fcts['label'])
			iter=0
			for c in self.pivot_concept:
				if iter>=1:
					row.append(fcts[c])
				iter+=1
			#print (row)
			row_lst.append(row)
			if len(row_lst)>=2:
				break
		df2 = pd.DataFrame(row_lst, columns=list(self.display_pivot.columns))
		
		self.display_pivot=self.display_pivot.append(df2,ignore_index=True)
		#print(self.display_pivot)
		s=self.display_pivot.style.set_properties(subset=pd.IndexSlice[[5, 6], :], **{'background-color': 'yellow'})#df.style.apply(highlight, subset=(0,))

		return s

	def query(self,query_text):
		concept=query_text.split(' ')[-1]

		if "similar tables" in query_text:
			self.populate_joinable()
		elif "identify tables" in query_text:
			self.identify_datasets(concept)

	def plot_graph(self,l1,l2,l3,column_name,edges,node_val,attrname):
	    N=len(l1)+len(l2)+len(l3)
	    G = nx.random_geometric_graph(N, 0.0)

	    curr=0
	    
	    while curr<len(l1):
	            G.nodes[curr]['pos']=[0.25*curr+0.2,1.3]
	            G.nodes[curr]['text']=l1[curr]
	            #print (curr,G.node[curr])
	            #G.add_edge(0,curr)
	            curr+=1

	    while curr<len(l1)+len(l2):
	            G.nodes[curr]['pos']=[0.25*(curr-len(l1))+0.2,1.2]
	            G.nodes[curr]['text']=l2[curr-len(l1)]#l2[curr-1-len(l1)]+":  "+str(node_val[curr-1-len(l1)][0])+" ("+str(node_val[curr-1-len(l1)][1])+"%)"
	            curr+=1
	            
	    while curr<N:
	            G.nodes[curr]['pos']=[0.25*(curr-len(l2)-len(l1))+0.2,1]
	            G.nodes[curr]['text']=l3[curr-len(l2)-len(l1)]#l2[curr-1-len(l1)]+":  "+str(node_val[curr-1-len(l1)][0])+" ("+str(node_val[curr-1-len(l1)][1])+"%)"
	            curr+=1
	            
	    node_adjacencies = []
	    node_text = []
	    for node, adjacencies in enumerate(G.adjacency()):
	        node_adjacencies.append(len(adjacencies[1]))
	        node_text.append(G.nodes[node]['text'])

	    for (a,b) in edges:
	         G.add_edge(a,b)
	    edge_x = []
	    edge_y = []
	    edge_text=[]
	    for edge in G.edges():
	        x0, y0 = G.nodes[edge[0]]['pos']
	        x1, y1 = G.nodes[edge[1]]['pos']
	        edge_x.append(x0)
	        edge_x.append(x1)
	        edge_x.append(None)
	        edge_y.append(y0)
	        edge_y.append(y1)
	        edge_y.append(None)
	        edge_text.append("0.95")

	    edge_trace = go.Scatter(
	    x=edge_x, y=edge_y,
	    line=dict(width=1, color='#888'),
	    text=edge_text,
	    mode='lines')

	    node_x = []
	    node_y = []
	    for node in G.nodes():
	        x, y = G.nodes[node]['pos']
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
	            title='<br>Index<br>',
	            titlefont_size=22,
	            font=dict(size=15),
	            showlegend=False,
	            hovermode='closest',
	            margin=dict(b=20,l=5,r=5,t=40),
	            annotations=[ ],
	            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
	            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
	            )
	    fig.update_xaxes(range=[-0.2, 0.8])
	    fig.add_annotation(
	    x=0.02,
	    y=1.335,
	    xref="x",
	    yref="y",
	    text="Concepts",
	    font=dict(size=15),
	    showarrow=False,
	    align="center",
	    bgcolor="#ff7f0e",
	    opacity=0.8
	    )
	    fig.add_annotation(
	    x=0.02,
	    y=1.205,
	    xref="x",
	    yref="y",
	    text="Columns",
	    font=dict(size=15),
	    showarrow=False,
	    align="center",
	    bgcolor="#ff7f0e",
	    opacity=0.8
	    )
	    fig.add_annotation(
	    x=0.02,
	    y=1.015,
	    xref="x",
	    yref="y",
	    text="Values",
	    font=dict(size=15),
	    showarrow=False,
	    align="center",
	    bgcolor="#ff7f0e",
	    opacity=0.8
	    )
	    fig.show()
	def show_index(self):
		self.plot_graph(['/wiki/Q6256(country)','/wiki/Property:P30(continent)','/wiki/Property:P1082(population)'],['Table.country','Table.population'],['India','China','1184639000'],"",[(0,3),(2,4),(3,5),(3,6),(4,7)],'','')
	def populate_joinable(self):
		self.get_count()
		iter=0
		
		self.pivot_col=[]
		self.new_cols={}
		self.new_datasets=[]
		self.new_dataset_cols=[]
		for v in self.lst:
			df=pd.read_csv("T2Dv2/tables_csv/"+v+".csv")
			#print (v,df.head())
			conceptlst=self.concept_dic[v]
			if iter==0:
				cols=list(df.columns)[1:]
				#self.pivot_df=df[cols]
				index=conceptlst.index(self.tag)-1#'sovereign state')#print ("pivot",concept_dic[v])
				#print ("index",index)
				#print (index,list(pivot_df.columns),conceptlst)
				colname=list(self.pivot_df.columns)[index]
				self.pivot_col=list(self.pivot_df[colname])
				sm=get_concept.semantic_mapping(self.pivot_df)
				sm.get_dataset_concepts()
				self.pivot_concept=sm.concept_lst
				#print (colname)
				#jkl
			else:
				try:
					interesting_col=conceptlst.index(self.tag)#'sovereign state')
				except:
					continue
				colname=list(df.columns)[interesting_col]
				col_lst=list(df[colname])
				if self.get_intersection(col_lst,self.pivot_col)>50:
					self.new_datasets.append(v)
					self.new_dataset_cols.append(list(df.columns))
					#print (list(df.columns),conceptlst)
					#print (v)
					#print ("intersection",self.get_intersection(col_lst,self.pivot_col))
					j=0
					while j<len(conceptlst):
						if conceptlst[j]==self.tag:#'sovereign state':
							j+=1
							continue
						self.new_cols[list(df.columns)[j]]=self.get_column(df,conceptlst,j)#get_intersection(col_lst,pivot_col)
						j+=1
			iter+=1
		self.get_mocodo()
	def identify_datasets(self,concept):
		result_num=1
		for fileid in self.concept_dic.keys():
			if concept in self.concept_dic[fileid]:
				print ("Result number ",result_num)

				filename=open("T2Dv2/Tables/"+fileid,"rb")
				data = filename.read()
				filename.close()
				data_text = data.decode('cp866', 'replace')
				table_json = json.loads(data_text)
				print ("Table source: ",table_json["url"])#,self.concept_dic[fileid])
				df=pd.read_csv("T2Dv2/tables_csv/"+fileid+".csv")
				df.head()
				iter=0
				table=[]
				while iter<4:
					table.append(df.iloc[iter])#[example_hits[iter],'<a href="http://dbpedia.org/page/'+example_hits[iter]+'">http://dbpedia.org/page/'+example_hits[iter]+'</a>'])#print (example_hits[iter])
					iter+=1
				display(HTML(tabulate.tabulate(table, tablefmt='html',  headers=list(df.columns),stralign='center')))
				result_num+=1
				#print (list(df.columns))

	def identify_datasets_withrelation(self,concept,relation):
		result_num=1
		for fileid in self.concept_dic.keys():
			if concept in self.concept_dic[fileid] and relation in self.concept_dic[fileid]:
				print (fileid,self.concept_dic[fileid])
				df=pd.read_csv("T2Dv2/tables_csv/"+fileid+".csv")
				#print(df.head())
				print ("Result number",result_num)
				table=[]#['Value','Url']]
				iter=0
				while iter<4:
					table.append(df.iloc[iter])#[example_hits[iter],'<a href="http://dbpedia.org/page/'+example_hits[iter]+'">http://dbpedia.org/page/'+example_hits[iter]+'</a>'])#print (example_hits[iter])
					iter+=1
				display(HTML(tabulate.tabulate(table, tablefmt='html',  headers=list(df.columns),stralign='center')))
				result_num+=1

				#print (list(df.columns))

if __name__ == '__main__':
	jd=join_datasets()
	jd.populate_joinable()



	print (jd.new_cols)

	print  (jd.new_datasets)
	print  (jd.new_dataset_cols)
