import sys
from pyspark import SparkContext

import os, math
from time import perf_counter
import copy, operator, collections
import itertools

# collect the code into main() function to be run
def main():
    time_start = perf_counter()
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    case = sys.argv[1]
    support = int(sys.argv[2])
    csv_file = sys.argv[3]
    output_file = sys.argv[4]
    # create SparkContext object
    sc = SparkContext.getOrCreate()
    # decrease number of warn messages
    sc.setLogLevel("WARN")


    # transform csv file into rdd file
    rdd_file = sc.textFile(csv_file)
    # Call
    clean_data = case_num(int(case), rdd_file)   # [('1', ['100', '101', '102', '98'])
    second_part = clean_data.map(lambda x: x[1]) # [['100', '101', '102', '98']. []
    # total number of baskets
    total_baskets = second_part.count()

    # partition format is [['100', '101', '97', '99'], ['102', '103', '105', '97', '98', '99'], ['97', '98']] جز من البيانات
    # first_item.collect() # so I can see the print inside the function, I did action statement



    # first phase SON
    first_candidates = second_part.mapPartitions(lambda chunk: candidate(chunk, support, total_baskets)).distinct()
    candidates = first_candidates.collect()


    def frequents(chunk):

        num = collections.defaultdict(int)
        for value in list(copy.deepcopy(list(chunk))):
            for item in candidates:
                if item.issubset(value):
                    num[item] += 1

        return [(x, n) for x, n in num.items()]

    # Second phase SON
    f1 = second_part.mapPartitions(frequents)
    frequent_itemsets = f1.reduceByKey(operator.add).filter(lambda s: s[1] >= support).map(lambda u: u[0]).collect()


    with open(output_file, 'w') as file:
        TheOutput = 'Candidates:\n'+output_items(candidates)+'\n\n'+'Frequent Itemsets:\n'+output_items(frequent_itemsets)
        file.write(TheOutput)
    file.close()


    time_stop = perf_counter()
    difference = time_stop - time_start  # calculate the execution time
    print(f"Duration:{difference}")




def candidate(chunk, support, total_baskets):
    chunk = list(copy.deepcopy(list(chunk)))

    # to count the number of baskets in each partition
    num_baskets_p = []
    for item in chunk:
        num_baskets_p.append(item)
    len_chunk = len(num_baskets_p)

    chunk_support = math.ceil(support * (len_chunk/total_baskets))

    output_priori = get_apriori_algo(chunk_support,chunk)
    return transforming_apprior(output_priori)

# def printf(ite):
#     par = list(ite)
#     print(par)
def case_num (case, rdd_file):
    # Pre-proces the file
    remove_header = rdd_file.first()
    filter_file = rdd_file.filter(lambda line: line != remove_header)  # ['1,100', '1,98', '1,101']
    if case == 1:
        user_business = filter_file.map(lambda x: x.split(','))  # [['1', '100'], ['1', '98'], ['1', '101']]
        group_user = user_business.map(lambda x: (x[0], x[1])).groupByKey()  # [('1', ResultIterable),
        sorted_noDuplicate = group_user.map(lambda x: (x[0], sorted(list(set(x[1])))))  # [('1', ['100', '101', '102', '98']),

        return sorted_noDuplicate
    elif case == 2:
        business_user = filter_file.map(lambda x: x.split(','))  # [['1', '100'], ['1', '98'], ['1', '101']]
        group_business = business_user.map(lambda x: (x[1], x[0])).groupByKey()  # [('1', ResultIterable),
        sorted_noDuplicate = group_business.map(lambda x: (x[0], sorted(list(set(x[1])))))  # [('1', ['100', '101', '102', '98']),
        # print('\n The business id first ', sorted_noDuplicate.take(3))
        return sorted_noDuplicate


def get_apriori_algo(support, total_baskets):
    # Get a dictionary of individual items and their frequency in the "all_baskets" using the "one basket" function.


    # Initialize an empty dictionary to store the frequent itemsets.
    frequent_itemsets = {}

    # Filter the items to get frequent items that meet the "support_threshold" using the "candidates_deletion" function.
    frequent_itemsets[1] = candidates_deletion(total_baskets, one_basket(total_baskets), support)

    # Start the loop to generate frequent itemsets of size greater than 1.
    random_value = 2
    while frequent_itemsets[random_value- 1]:
        # Store the frequent itemsets of size "random_value-1" in the "frequent_itemsets" dictionary.
        frequent_itemsets[random_value - 1] = frequent_itemsets[random_value - 1]

        # Generate candidate itemsets of size random item by joining the frequent itemsets of size "random_value-1".
        candidate_itemsets = candidate_genrator(frequent_itemsets[random_value - 1], random_value)

        # Prune the candidate itemsets to remove the ones that are not frequent using the "pruning" function.
        pruned_itemsets = unwanted_candidates(candidate_itemsets, frequent_itemsets[random_value - 1], random_value - 1)

        # Filter the pruned itemsets to get frequent itemsets that meet the "support_threshold".
        frequent_itemsets[random_value] = candidates_deletion(total_baskets, pruned_itemsets, support)

        # Increment random_value to generate frequent itemsets of size "random_value+1".
        random_value += 1

    # Return the dictionary of frequent itemsets.
    return frequent_itemsets





def one_basket(total_baskets):
    return {frozenset({item}) for basket in total_baskets for item in basket}


def candidate_genrator(item_sets, current_length):
    return {x.union(y) for x in item_sets for y in item_sets if len(x.union(y)) == current_length}


def candidates_deletion(total_baskets, candidates, support):
    # Initialize an empty set
    output = set()

    # If the "candidates" list is empty, return the empty "result" set.
    if not candidates:
        return output
    nums = collections.defaultdict(int)
    nums = collections.Counter(candidate for basket in total_baskets for candidate in candidates if candidate.issubset(basket))
    output = {itemset for itemset, n in nums.items() if n >= support}
    return output

def unwanted_candidates(candidates, frequent_c ,current_length):
    copy_candidates = candidates.copy()
    for candidate in candidates:
        subsets = itertools.combinations(candidate,current_length)
        for subset in subsets:
            if (frozenset(subset) not in frequent_c):
                copy_candidates.remove(candidate)
                break
    return copy_candidates

def transforming_apprior(output_apriori):
    transformed_set = set()
    for itemsets in output_apriori.values():
        for itemset in itemsets:
            transformed_set.add(itemset)
    return transformed_set


def output_items(data):
    # Convert frozensets to tuples and sort
    tuples = [tuple(sorted(frozenset)) for frozenset in data]

    # Remove duplicates
    tuples = list(set(tuples))

    # Group tuples by length
    groups = {}
    for t in tuples:
        length = len(t)
        if length not in groups:
            groups[length] = []
        groups[length].append(t)

    # Create formatted output
    output = []
    for length in sorted(groups.keys()):
        if output:
            output.append('')
        for t in sorted(groups[length]):
            if len(t) == 1:
                output.append("('" + str(t[0]) + "')")
            else:
                output.append(str(t))

    result_join = ",".join(output)
    split_result = result_join.split(',,')
    result = '\n\n'.join([s for s in split_result])

    return "".join(result) # so I can concatenate ["list" output ] to str of TheOutput = 'Candidates:\n'+output_items(candidates)+

if __name__ == '__main__':
    main()
