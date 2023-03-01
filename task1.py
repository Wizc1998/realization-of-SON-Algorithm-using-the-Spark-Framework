# -*- coding: utf-8 -*-
"""
Created on Mon Feb 20 16:41:18 2023

@author: jason
"""

from pyspark import SparkContext
import os
import sys
import time
import itertools
import math

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

user_bus_filepath = './data/small2.csv'

output_filepath = 'task1_result_case2.txt'

case_num = 1

threshhold = 4

partition_num = 2

sc = SparkContext("local","task").getOrCreate()
sc.setLogLevel('ERROR')

start_time = time.perf_counter()
user_bus_rdd = sc.textFile(user_bus_filepath)
header = user_bus_rdd.first()
user_bus_rdd = user_bus_rdd.filter(lambda x: x != header)

#reviews_rdd = sc.textFile(review_filepath).map(lambda x: json.loads(x))

#%%
user_bus_rdd.collect()


if case_num == 1:    
    zz = user_bus_rdd.map(lambda x: (x.split(',')[0], x.split(',')[1]))
else: 
    zz = user_bus_rdd.map(lambda x: (x.split(',')[1], x.split(',')[0]))

transactions_rdd = zz \
            .groupByKey() \
            .map(lambda x: (x[0], list(set(x[1]))))
transactions_rdd.collect()




#%%
def filter_with_support(k_tuples_list, partition_list, adj_threshhold):

    support_compare_dict = {}
    
    for k_tuple in k_tuples_list:
        k_tuple = list(k_tuple)
        #print(k_tuple)
        support_count = 0
        for basket in partition_list:
            if all(item in basket for item in k_tuple):
                support_count+=1
                
        k_tuple = tuple(k_tuple)
        if k_tuple not in support_compare_dict:
            support_compare_dict[k_tuple] = support_count
        else:
            support_compare_dict[k_tuple] = 'error_evidence: repeated value'
            #print(support_count)
            #print(basket)
       
    filtered_list = []
    for k,v in support_compare_dict.items():
        if v >= adj_threshhold:
            filtered_tuple = set(k)
            filtered_list.append(filtered_tuple)
    return filtered_list

def find_all_combinations(input_list):
    if len(input_list) ==0:
        return []
    k_tup = len(input_list[0])+1
    #print(k_tup)
    all_permutations = []
    for item in input_list:
        for another in input_list:
            combination = set(item.union(another))
            
            if len(combination) == k_tup:
                #print(combination)
                #print('pass check 1 on length')
                #print(all_permutations)
                if not combination in all_permutations:
                    #print('pass check 2 on exist')
                    all_permutations.append(combination)
            
    final_all_permutation = []
    #print('all permus are: ', all_permutations)
    for permutation in all_permutations:
        
        #print('\ncurrent processing: ',permutation)
        inner_combos = [set(i) for i in list(itertools.combinations(permutation,k_tup-1))]
        #print('the components are: ',inner_combos)
        #print('original list are: ',input_list)
        if all(item in input_list for item in inner_combos):
            #print('pass check 3 on components')
            final_all_permutation.append(permutation)
    return final_all_permutation


def generate_A_priori_subsets(partition, total_trans_count):
    
    result_list = []
    
    partition_list = [x[1] for x in partition]
    
    #local_length = len(list(partition))
    for x in partition:
        print(x[1])

    adj_threshhold = math.ceil(threshhold * len(partition_list) / total_trans_count)

    #apriori pass 1: Read baskets and count in main memory the occurrences of each single item
    single_dict = {}
    for item in partition_list:
        for subitem in item:
            if subitem not in single_dict:
                single_dict[subitem]=1
            else:
                single_dict[subitem] = single_dict[subitem]+1
    
    filtered_single_dict = {}
    
    for k,v in single_dict.items():
        if v >= adj_threshhold:
            filtered_single_dict[k] = v
            
            
    #filtered_single_dict is the dict that contains all singles that are frequent
    
    #apriori pass 2 and above
    #get the key list
    k_tuples_list = list(filtered_single_dict.keys())
    #put all tuples in the key list into a set bucket
    k_tuples_list = [{i} for i in k_tuples_list]
    #append to result
    k_tuples_list_for_add = [tuple(i) for i in k_tuples_list]
    result_list = result_list+k_tuples_list_for_add
    
    while len(k_tuples_list) > 0:
        k_tuples_list = find_all_combinations(k_tuples_list)
        #print('something',k_tuples_list)
        
        #TODO: do something to k_tuples_list, filter out those shows up below the threshhold
        k_tuples_list = filter_with_support(k_tuples_list, partition_list, adj_threshhold)
        
        
        k_tuples_list_for_add = [tuple(i) for i in k_tuples_list]
        result_list =result_list+ k_tuples_list_for_add
    
    
    return result_list



#list(transactions_rdd)
total_trans_count = transactions_rdd.count()
#print('total length: ',total_trans_count)

#son pass 1 - generate candidates
z1 = transactions_rdd \
    .mapPartitions(lambda partition: generate_A_priori_subsets(partition=partition, total_trans_count = total_trans_count)) \
    .distinct().sortBy(lambda x: (len(x), x)) 
candidates = z1.collect()



#%%

from operator import add

def count_tuple_sum(candidates, row):
    current_row_count = {}
    basket_content = row[1]
    for k_tup in candidates:
        if all(item in basket_content for item in k_tup):
            #condition were this tuple is showed up in the row
            if k_tup not in current_row_count:
                current_row_count[k_tup] = 1
            else:
                current_row_count[k_tup] = current_row_count[k_tup] + 1
    
    
    yield [item for item in current_row_count.items()]
#son pass 2 - exam the candidates pair on whole dataset
z2 = transactions_rdd \
    .flatMap(lambda row: count_tuple_sum(candidates = candidates, row = row)) \
    .flatMap(lambda x: x) \
    .reduceByKey(add)\
    .filter(lambda x: x[1] >= int(threshhold))\
    .map(lambda x: x[0])\
    .sortBy(lambda x: (len(x), x))
Frequent_itemsets = z2.collect()



#%%

#re-organize sets
from itertools import groupby

def sort_tuple(k_tup):
    result = list(k_tup)
    result.sort()
    return tuple(result)


def sort_itemset_inside_out(itemset):

    new_itemset = []
    for item in itemset:
        new_itemset.append(sort_tuple(item))
    
    itemset = new_itemset
    
    zz = [list(g) for k, g in groupby(itemset, key=len)]
    
    new_itemset = []
    for bucket in zz:
        bucket.sort()
        new_itemset = new_itemset+bucket
    
    return new_itemset


candidates = sort_itemset_inside_out(candidates)

Frequent_itemsets = sort_itemset_inside_out(Frequent_itemsets)





#%%

#write the candidates


outfile = open(output_filepath, "w")

outfile.write('Candidates:\n')

start_of_line_flag = 1
length_mark = 1
for k_tup in candidates:
    if len(k_tup) !=1:
        outstr = str(k_tup)
    else:
        outstr = str(k_tup)[:-2]+')'
    
    
    if len(k_tup)!=length_mark:
        start_of_line_flag = 1
        length_mark+=1
        outfile.write('\n\n')
    
    if start_of_line_flag == 1:
        outfile.write(outstr)
    else:
        outfile.write(',')
        outfile.write(outstr)
    start_of_line_flag = 0
    

#write the actual pass 2 results
outfile.write('\n\nFrequent Itemsets:\n')

start_of_line_flag = 1
length_mark = 1
for k_tup in Frequent_itemsets:
    if len(k_tup) !=1:
        outstr = str(k_tup)
    else:
        outstr = str(k_tup)[:-2]+')'
    
    
    if len(k_tup)!=length_mark:
        start_of_line_flag = 1
        length_mark+=1
        outfile.write('\n\n')
    
    if start_of_line_flag == 1:
        outfile.write(outstr)
    else:
        outfile.write(',')
        outfile.write(outstr)
    start_of_line_flag = 0

outfile.close()


end_time = time.perf_counter()

time_used = end_time - start_time
print('Duration:',time_used)











