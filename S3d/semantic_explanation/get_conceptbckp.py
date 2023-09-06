import networkx as nx
import plotly.graph_objects as go
import sys,json,operator
import Query

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
def plot_graph(l1,l2,column_name,edges,node_val,attrname):
        #Plot the graphical snapshot of the knowledge graph
        N=1+len(l1)+len(l2)
        G = nx.random_geometric_graph(N, 0.0)
        #print (G.nodes())
        #print (G.node[0])

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
             print (curr)
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
            print (edge[0],edge[1])
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
        '''
        fig.add_annotation(
        x=0.45,
        y=1.3,
        xref="x",
        yref="y",
        text="",
        showarrow=True,
        font=dict(
            family="Courier New, monospace",
            size=16,
            color="#ffffff"
            ),
        align="center",
        arrowhead=2,
        arrowsize=1,
        arrowwidth=2,
        arrowcolor="#636363",
        ax=-130.5,
        ay=0,
        #bordercolor="#c7c7c7",
        #borderwidth=2,
        #borderpad=4,
        bgcolor="#ff7f0e",
        opacity=0.8
        )
        '''
        fig.show()
def index_concepts(dbpedia_concepts,wikidata_concepts):
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
	


def prepare_dataset(filename):
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

    return headers,data_content

#Write a matcher between header and concept in the list

'''filename=open(sys.argv[1],"rb")

(header,content)=prepare_dataset(filename)
(dbpedia_index_map,wikidata_index_map)=index_concepts(dbpedia_concepts,wikidata_concepts)
'''#print (header)
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

def get_column_concept(column_lst,column_header):
	options=[]
	for val in column_lst:
		#print ("before",val,options)
		options.extend(Query.get_rels(val))
		#print ("after",val,options)

	options=list(filter(lambda a: a != "NotFound", options))#options.remove('NotFound')
	#print (options,"hre")
	if len(options)==0:
		return ""
	return (max(options,key=options.count))


def get_dataset_topic(header,content,concept_lst):
	i=0
	total_dic={}
	while i<len(content[header[0]]):
		valuelst=[]
		for j in range(len(header)):
			valuelst.append(content[header[j]][i])
		#print (valuelst)
		count_dic=Query.get_dataset_topic(valuelst,concept_lst)
		for v in count_dic:
			if v in total_dic:
				total_dic[v]+=count_dic[v]
			else:
				total_dic[v]=count_dic[v]
		i+=1
	#print (total_dic)
	dataset_topic=max(total_dic.items(), key = operator.itemgetter(1))[0]
	index=concept_lst.index(dataset_topic)

	print ("relationship between columns",Query.get_relationship(concept_lst,index,content,header))
	#print ("index of dataset topic is ",index)

	try:
		return (max(total_dic.items(), key = operator.itemgetter(1))[0])
	except:
		return ""


def identify_new_columns(content,header,concept_lst,dataset_topic):
	topic_index=concept_lst.index(dataset_topic)
	column=content[header[topic_index]]
	print ("topic index is ",topic_index)
	new_content={}
	for val in column:
		newcolumns=Query.identify_new_columns(val,dataset_topic)
		for v in newcolumns:
			if v in new_content.keys():
				new_content[v]+=1
			else:
				new_content[v]=1
		#print (val,newcolumns)
	sorted_lst = sorted(new_content.items(), key=operator.itemgetter(1),reverse=True)

	dont_consider=['label','id','factsArray','description_exact','facts','title','instance of','factsCount','label_exact']
	final_options=[]
	for (c,v) in sorted_lst:
		if c in dont_consider:
			continue
		if v*1.0/len(column)> 0.2:
			#print (c,v*1.0/len(column))
			final_options.append((c,v*1.0/len(column)))
	#print (new_content,len(column))
	return final_options

def show_new_samples(content,header,column,dataset_topic,new_columns,concept_lst):
	topic_index=concept_lst.index(dataset_topic)
	column=content[header[topic_index]]
	print ("topic index is ",topic_index)
	new_content={}
	for v in new_columns:
		new_content[v]=[]
	for val in column:
		newvalues=Query.identify_new_columns_content(val,dataset_topic,new_columns)
		for v in new_columns:
			lst=new_content[v]
			lst.append(newvalues[v])

	print (new_content)

		

'''
concept_lst=[]
for column in header:
	column_list=content[column]
	print ("processing  column ",column)
	#print ("column list",column_list)
	concept_lst.append (get_column_concept(column_list,column))

print ("headers",header)
print ("identified concepts",concept_lst)
dataset_topic=get_dataset_topic(header,content,concept_lst)
print ("Data set is about:",dataset_topic)


print ("New columns ",identify_new_columns(content,header,concept_lst,dataset_topic))

show_new_samples(content,header,column,dataset_topic,['author'])
'''
