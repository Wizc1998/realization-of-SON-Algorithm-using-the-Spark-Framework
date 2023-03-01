# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 23:35:16 2023

@author: jason
"""

from pyspark import SparkContext
import os
import sys
import time
import itertools
import math
import csv

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#data pre-processing phase

input_filepath = './data/ta_feng_all_months_merged.csv'

intermediate_filepash = 'dt-customer_product.csv'

output_filepath = 'task2_result_s.txt'

threshhold = 50

filter_threshold = 20


with open(input_filepath, 'r',encoding='utf-8') as rawfile:
    csvreader = csv.reader(rawfile)
    with open(intermediate_filepash, 'w',newline='') as output:
        writer = csv.writer(output)
        cc = 0
        for row in csvreader:
            if cc==0:
                row = ['DATE-CUSTOMER_ID', 'PRODUCT_ID']
                writer.writerow(row)
            else:
                product_id = str(int(row[5]))
                
                row = [row[0]+'-'+row[1],product_id]
                writer.writerow(row)
            #print(row)
            cc+=1
            #if cc>20000:
                #break

row[0]
rawfile.close()
output.close()

#%%


sc = SparkContext("local","task").getOrCreate()
sc.setLogLevel('ERROR')

start_time = time.time()
user_bus_rdd = sc.textFile(intermediate_filepash)

def remove_header(itr_index, itr):
    return iter(list(itr)[1:]) if itr_index == 0 else itr
user_bus_rdd = user_bus_rdd.mapPartitionsWithIndex(remove_header)


#header = user_bus_rdd.first()
#user_bus_rdd = user_bus_rdd.filter(lambda x: x != header)

#user_bus_rdd.take(10)



#%%

zz = user_bus_rdd.map(lambda x: (x.split(',')[0], x.split(',')[1]))

transactions_rdd = zz \
            .groupByKey() \
            .map(lambda x: (x[0], list(set(x[1]))))\
            .filter(lambda x: len(x[1]) > int(filter_threshold))
#transactions_rdd.collect()



#%%
def filter_with_support(k_tuples_list, partition_sets, adj_threshhold):
    
    
    support_compare_dict = {}
    
    
    for k_tuple in k_tuples_list:
        
        support_count = 0
        for basket in partition_sets:
            
            if k_tuple.issubset(basket):
                support_count+=1
                
        k_tuple = tuple(k_tuple)
        if k_tuple not in support_compare_dict:
            support_compare_dict[k_tuple] = support_count
        else:
            support_compare_dict[k_tuple] = 'error_evidence: repeated value'

    
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
    

    
    if k_tup == 2:
        for comb in itertools.combinations(input_list, k_tup):
            all_permutations.append(comb[0].union(comb[1]))
    
    else:
        #print('input_list is: ', type(input_list), ' - ',input_list)
        for item in input_list:
            for another in input_list:
                combination = item.union(another)
                
                if len(combination) == k_tup:
    
                    if not combination in all_permutations:
    
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
    partition_sets = [set(x) for x in partition_list]

    adj_threshhold = math.ceil(threshhold * len(partition_sets) / total_trans_count)

    #apriori pass 1: Read baskets and count in main memory the occurrences of each single item
    single_dict = {}
    for item in partition_sets:
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
        
        
        k_tuples_list = filter_with_support(k_tuples_list, partition_sets, adj_threshhold)
        
        k_tuples_list_for_add = [tuple(i) for i in k_tuples_list]
        result_list =result_list+ k_tuples_list_for_add
    
    
    return result_list


#%%

total_trans_count = transactions_rdd.count()

print('yikes1')


#son pass 1 - generate candidates
z1 = transactions_rdd \
    .mapPartitions(lambda partition: generate_A_priori_subsets(partition=partition, total_trans_count = total_trans_count)) \
    .distinct().sortBy(lambda x: (len(x), x)) 
candidates = z1.collect()


#time2 = time.time()
#print('\ntime summary1: ',time2-time1)


#%%
time1 = time.time()


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
print('yikes22')
#time1 = time.time()

z2 = transactions_rdd \
    .flatMap(lambda row: count_tuple_sum(candidates = candidates, row = row)) \
    .flatMap(lambda x: x) \
    .reduceByKey(add)\
    .filter(lambda x: x[1] >= int(threshhold))\
    .map(lambda x: x[0])\
    .sortBy(lambda x: (len(x), x))
Frequent_itemsets = z2.collect()

print('checkpoint#1: ',time.time()-time1)

#%%#re-organize sets
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



#%%file write

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

end_time = time.time()

time_used = end_time - start_time
print('\nDuration:',time_used)









