#!/usr/bin/env python
# coding: utf-8

# # Tabular Data Annotation (Matching to KBs)
# 
# This notebook shows a set of functions for matching tables with knowledge base concepts.

# ## Setup
# 
# Imports for setup:

# In[1]:


import pandas as pd
## COS connection imports
from botocore.client import Config
import ibm_boto3
## EDS connection imports
from elasticsearch import Elasticsearch
from ssl import create_default_context


# We need conncections to Cloud Object Storage (COS) and Elasticsearch for Databases Service (EDS).
# 
# For COS, you need the following:
# ```
# s3 = ibm_boto3.resource(service_name='s3',
#     ibm_api_key_id='YOUR_KEY',
#     ibm_auth_endpoint="https://iam.ng.bluemix.net/oidc/token",
#     config=Config(signature_version='oauth'),
#     endpoint_url='https://s3.us-east.objectstorage.service.networklayer.com')
# bucket_name = 'BUCKET_NAME'
# bucket = s3.Bucket(bucket_name)
# ```
# Documentation for COS / S3 APIs available here: https://ibm.github.io/ibm-cos-sdk-python/index.html
# 
# 
# In the following (hidden) cell, we create context through:
# ```
# context = create_default_context( cadata = "YOUR FULL CADATA WITH NEW LINES HERE" )
# ```
# and elasticsearch connection through:
# ```
# es = Elasticsearch(
#     ['host.databases.appdomain.cloud'],
#     http_auth=('ibm_cloud_username',
#                'secret (password)'),
#     scheme="https",
#     port=31728,
#     ssl_context=context,
# )
# ```

# In[2]:


# @hidden_cell

# The following code accesses a file in your IBM Cloud Object Storage. It includes your credentials.
# You might want to remove those credentials before you share your notebook.
s3 = ibm_boto3.resource(service_name='s3',
    ibm_api_key_id='Bf1YuaPIEnK1HFbAx3Res6OV_womxKSv3ZwpQb7cfOKe',
    ibm_auth_endpoint="https://iam.ng.bluemix.net/oidc/token",
    config=Config(signature_version='oauth'),
    endpoint_url='https://s3.us-east.objectstorage.service.networklayer.com')
bucket_name = 'tables-for-annotation'
bucket = s3.Bucket(bucket_name)
context = create_default_context(
   cadata = "-----BEGIN CERTIFICATE-----\n"+
     "MIIDDzCCAfegAwIBAgIJANEH58y2/kzHMA0GCSqGSIb3DQEBCwUAMB4xHDAaBgNV\n"+
     "BAMME0lCTSBDbG91ZCBEYXRhYmFzZXMwHhcNMTgwNjI1MTQyOTAwWhcNMjgwNjIy\n"+
     "MTQyOTAwWjAeMRwwGgYDVQQDDBNJQk0gQ2xvdWQgRGF0YWJhc2VzMIIBIjANBgkq\n"+
     "hkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA8lpaQGzcFdGqeMlmqjffMPpIQhqpd8qJ\n"+
     "Pr3bIkrXJbTcJJ9uIckSUcCjw4Z/rSg8nnT13SCcOl+1to+7kdMiU8qOWKiceYZ5\n"+
     "y+yZYfCkGaiZVfazQBm45zBtFWv+AB/8hfCTdNF7VY4spaA3oBE2aS7OANNSRZSK\n"+
     "pwy24IUgUcILJW+mcvW80Vx+GXRfD9Ytt6PRJgBhYuUBpgzvngmCMGBn+l2KNiSf\n"+
     "weovYDCD6Vngl2+6W9QFAFtWXWgF3iDQD5nl/n4mripMSX6UG/n6657u7TDdgkvA\n"+
     "1eKI2FLzYKpoKBe5rcnrM7nHgNc/nCdEs5JecHb1dHv1QfPm6pzIxwIDAQABo1Aw\n"+
     "TjAdBgNVHQ4EFgQUK3+XZo1wyKs+DEoYXbHruwSpXjgwHwYDVR0jBBgwFoAUK3+X\n"+
     "Zo1wyKs+DEoYXbHruwSpXjgwDAYDVR0TBAUwAwEB/zANBgkqhkiG9w0BAQsFAAOC\n"+
     "AQEAJf5dvlzUpqaix26qJEuqFG0IP57QQI5TCRJ6Xt/supRHo63eDvKw8zR7tlWQ\n"+
     "lV5P0N2xwuSl9ZqAJt7/k/3ZeB+nYwPoyO3KvKvATunRvlPBn4FWVXeaPsG+7fhS\n"+
     "qsejmkyonYw77HRzGOzJH4Zg8UN6mfpbaWSsyaExvqknCp9SoTQP3D67AzWqb1zY\n"+
     "doqqgGIZ2nxCkp5/FXxF/TMb55vteTQwfgBy60jVVkbF7eVOWCv0KaNHPF5hrqbN\n"+
     "i+3XjJ7/peF3xMvTMoy35DcT3E2ZeSVjouZs15O90kI3k2daS2OHJABW0vSj4nLz\n"+
     "+PQzp/B9cQmOO8dCe049Q3oaUA==\n"+
     "-----END CERTIFICATE-----")

es = Elasticsearch(
    ['e055b946-0f9c-44ee-bd74-550a416ee2a7.b8a5e798d2d04f2e860e54e5d042c915.databases.appdomain.cloud'],
    http_auth=('ibm_cloud_0270a36f_ce40_44af_8d39_286c95d54770',
               '464d53e422ff088ffa70b754a7d2e5b7b0c96808bcc4024f05604fb842dd8040'),
    scheme="https",
    port=31728,
    ssl_context=context,
)


# Checking the connection by printing out elasticsearch server health:

# In[3]:


#print(es.cluster.health())


# ## Data Profiling

# ### Profiling Functions
# 
# Functions needed for profiling tabular data. These functions perform basic data type identification.

# In[4]:


TYPE_BOOL = 'BOOL'
"""String: A boolean variable"""

TYPE_NUM = 'NUM'
"""String: A numerical variable"""

TYPE_MNUM = 'MNUM'
"""String: A mostly numerical variable"""

TYPE_DATE = 'DATE'
"""String: A numeric variable"""

TYPE_CONST = 'CONST'
"""String: A constant variable"""

TYPE_CAT1 = 'CAT1'
"""String: A categorical variable with a small number of distinct values (e.g. state, gender) or a short average length"""

TYPE_CAT2 = 'CAT2'
"""String: A categorical variable with a large number of distinct values (e.g., first name, city, country) and average length above 5"""

S_TYPE_UNSUPPORTED = 'UNSUPPORTED'
"""String: An unsupported variable"""


def is_mostly_num(vals):
    count = 0
    for i in vals[:100]:
        try:
            x = float(i)
            count += 1
        except:
            continue
    if count>10:
        return True
    return False


def get_groupby_statistic(data):
    """Calculate value counts and distinct count of a variable (technically a Series).
    The result is cached by column name in a global variable to avoid recomputing.
    Source: https://github.com/pandas-profiling/pandas-profiling/blob/master/pandas_profiling/base.py
    Parameters
    ----------
    data : Series
        The data type of the Series.
    Returns
    -------
    list
        value count and distinct count
    """
    value_counts_with_nan = data.value_counts(dropna=False)
    value_counts_without_nan = value_counts_with_nan.loc[value_counts_with_nan.index.dropna()]
    distinct_count_with_nan = value_counts_with_nan.count()
    avg_len = data.astype(str).str.len().mean()

    # When the inferred type of the index is just "mixed" probably the types within the series are tuple, dict, list and so on...
    if value_counts_without_nan.index.inferred_type == "mixed":
        raise TypeError('Not supported mixed type')

    result = [value_counts_without_nan, distinct_count_with_nan, avg_len]

    return result


def get_vartype(data):
    """Infer the type of a variable (technically a Series).
    The types supported are split in standard types and special types.
    Source: https://github.com/pandas-profiling/pandas-profiling/blob/master/pandas_profiling/base.py
    Standard types:
        * Categorical (`TYPE_CAT`): the default type if no other one can be determined
        * Numerical (`TYPE_NUM`): if it contains numbers
        * Boolean (`TYPE_BOOL`): at this time only detected if it contains boolean values, see todo
        * Date (`TYPE_DATE`): if it contains datetime
    Special types:
        * Constant (`S_TYPE_CONST`): if all values in the variable are equal
        * Unique (`S_TYPE_UNIQUE`): if all values in the variable are different
        * Unsupported (`S_TYPE_UNSUPPORTED`): if the variable is unsupported
     The result is cached by column name in a global variable to avoid recomputing.
    Parameters
    ----------
    data : Series
        The data type of the Series.
    Returns
    -------
    str
        The data type of the Series.
    Notes
    ----
        * Should improve verification when a categorical or numeric field has 3 values, it could be a categorical field
        or just a boolean with NaN values
        * #72: Numeric with low Distinct count should be treated as "Categorical"
    """
    vartype = None
    is_key = None
    #try:
    (value_counts_without_nan, distinct_count, avg_len) = get_groupby_statistic(data)
    leng = len(data)
    idata = data.infer_objects()
    if distinct_count == leng:
        is_key = True
    if distinct_count <= 1:
        vartype = TYPE_CONST
    elif pd.api.types.is_bool_dtype(idata) or (distinct_count == 2 and pd.api.types.is_numeric_dtype(idata)):
        vartype = TYPE_BOOL
    elif pd.api.types.is_numeric_dtype(idata):
        vartype = TYPE_NUM
    elif is_mostly_num(data.sample(min(len(data),100)).tolist()):
        vartype = TYPE_MNUM
    elif pd.api.types.is_datetime64_dtype(idata):
        vartype = TYPE_DATE
    elif (distinct_count/leng >= 0.5 or distinct_count>20) and avg_len>5:
        vartype = TYPE_CAT2
    else:
        vartype = TYPE_CAT1
    #except:
    #    vartype = S_TYPE_UNSUPPORTED

    return (vartype,is_key)


# ### Profiling data sets, reading from COS
# 
# We go through all the data sets, find those that can be parsed as tabular, and perform datatype identification. We also store candidate tables/columns for KB-based annotation.
# 
# First, reading and parsing:

# In[7]:
# In[19]:


def get_rels(value):
    types = set()
    rel_labels = set()
    rel_labels_count={}
    index_name = "wikidata-facts-v4"
    res = es.search(
        index= index_name,
        body=
        {
          "query": {
            "bool": {
              "should": [
                { "match_phrase": {"factsArray":"*: "+value+"*"}}
              ]
            }
          },
          "sort": [
            {
              "factsCount": {
                "order": "desc"
              }
            }
          ]
        }
        
    )

    #print("Got %d Hits." % res['hits']['total'])
    max_hits = 10000
    found_entities=[]
    for hit in enumerate(res['hits']['hits'],1):
        src = hit[1]["_source"]
        #print (src)
        if src["label"] == value:
            found_entities.append(src)
        elif  "altLabels" in src.keys() and  value in src["altLabels"]:
            found_entities.append(src)
        '''
        if "types" in src:
            types.update(src["types"])
        if "factsArray" in src:
            factsArray = src["factsArray"]
            for fact in factsArray:
                if value in fact:
                    rel_label = fact[:fact.rfind(":")]
                    if rel_label in rel_labels_count.keys():
                        rel_labels_count[rel_label]+=1
                    else:
                        rel_labels_count[rel_label]=1
                    rel_labels.add(rel_label)
        '''
    #print (rel_labels_count)
    #print (value,found_entities)
    return found_entities


# In[23]:



import sys,json

def prepare_dataset(df,pivot_id):#,filename):
    #Dataset File
    

    column_lst=list(df.columns)#table_json["relation"]

    #Table is a dictionary indexed by rowid and has a list as value
    table={}

    #Numerical attributes of the pivot's neighbors, key is the attribute name and value is a dictionary of values
    numeric_dic={}

    #Construct the table by populating table dictionary
    curr_column=0
    for c in column_lst:
        column=df[c]
        iter=0
        #print (column[0])
        for column_cell in column:
            column_cell=str(column_cell)
            if iter in table.keys():
                row=table[iter]
            else:
                row=[]
            row.append(column_cell.replace(',',''))
            table[iter]=row
            iter+=1
            if not curr_column==pivot_id:
                continue
            #print (column_cell,curr_column,pivot_id)
            #Look up knowledge graph if it is a pivot
            json_hit=get_rels(column_cell)

            if json_hit is None:
                continue
            for i in range(len(json_hit)):
                #Look in factsArray for every hit and find the numeric attributes
                facts_lst=json_hit[i]["factsArray"]
                
                for fact in facts_lst:
                    try:
                        factids=fact.split(':')
                        if factids[0] in numeric_dic.keys():
                            fact_dic=numeric_dic[factids[0]]
                        else:
                            fact_dic={}
                            numeric_dic[factids[0]]=fact_dic

                        fact_dic[column_cell] = int(factids[1].split('-')[0].replace(',',''))
                        numeric_dic[factids[0]] = fact_dic
                    except:
                        continue
        curr_column+=1

    return table,numeric_dic

#L1 distance between two lists
def calculate_distace(lst1, lst2):
    count=0
    total_dist=0
    for i in range(min(len(lst1),len(lst2))):
        if lst1[i]==-1 or lst2[i]==-1:
            continue
        else:
            count+=1
            total_dist+= abs(lst1[i]-lst2[i])
    if count==0 or count<10:
        #print (count)
        return 10000000000
    #print (count,lst1,lst2)
    return total_dist*1.0/count

#Compare the different distributions of numeric_dic with that of table
def identify_distribution(table, column_id, pivot_column, numeric_dic):
    input_distr=[]
    testing_dist={}
    for numeric_dist_keys in numeric_dic.keys():
        testing_dist[numeric_dist_keys] = []

    for row_id in table.keys():
        row=table[row_id]
        #print (row)
        try:
            input_distr.append(float(row[column_id]))
        except:
            input_distr.append(-1)
        for numeric_dist_key in numeric_dic.keys():
            lst=testing_dist[numeric_dist_key]
            if row[pivot_column] in numeric_dic[numeric_dist_key].keys():
                lst.append(numeric_dic[numeric_dist_key][row[pivot_column]])
            else:
                lst.append(-1)
            testing_dist[numeric_dist_key]=lst
    
    minv=10000000
    min_relation=''
    for relationship_key in testing_dist.keys():
        currval=calculate_distace(input_distr,testing_dist[relationship_key])
        #print (relationship_key,currval)
        if currval<minv:
            minv=currval
            min_relation=relationship_key

    return min_relation
if __name__ == '__main__':      
    filename=open(sys.argv[1],"rb")
    pivot_id=int (sys.argv[2])
    numeric_column=int (sys.argv[3])
    (table,numeric_dic)=prepare_dataset(pivot_id,filename)
    identify_distribution(table,numeric_column,pivot_id,numeric_dic)
#6 and 0
#2 and 1
#print (numeric_dic.keys())
#get_rels("1961-08-04T00")
#get_rels("4 August 1961")
#get_rels("Coniston Old Man")
#get_rels("Old Man of Coniston")
#get_rels("84080")


# In[ ]:




