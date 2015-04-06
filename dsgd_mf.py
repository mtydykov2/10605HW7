'''
Created on Apr 2, 2015

@author: root
'''

import sys
import os
from pyspark import SparkContext, SparkConf
import random
import numpy

def get_matrix_dimensions(tuples):
    num_movies = tuples.map(lambda element: element[0]).distinct().count()
    num_users = tuples.map(lambda element: element[1]).distinct().count()
    return num_movies, num_users

def get_iteration_ranges(w, h, num_workers):
    """
    Given the dimensions of w and h and the number of workers,
    determine the range of valid indeces for each block row for each iteration.
    """
    width = len(h)/num_workers
    height = len(w)/num_workers
    iteration_ranges = {}
    for i in range(0, num_iterations):
        stratum = {}
        #every worker get some number of rows
        for block_row in range(num_workers):
            stratum[block_row] = {}
            min_user = block_row*height
            min_movie = (block_row*width + width*i)%len(h)
            stratum[block_row]["min_user"] = min_user
            stratum[block_row]["max_user"] = stratum[block_row]["min_user"]+height
            stratum[block_row]["min_movie"] = min_movie
            stratum[block_row]["max_movie"] = stratum[block_row]["min_movie"] + width
        
        iteration_ranges[i] = stratum
    return iteration_ranges    
    
def run_sgd(w, h, num_iterations, beta_value, lambda_value, num_workers, user_ids, movie_ids, all_data, num_ratings_per_user, num_ratings_per_movie):
    """
    Run stochastic gradient descent until convergence.
    """
    iteration_ranges = get_iteration_ranges(w, h, num_workers)
    iteration = 0
    while iteration < num_iterations:
        def update_sgd(data):
            """
            Perform updates to factor matrices on each iteration.
            """
            for it in data:
                movie_id, user_id = it[0]
                values = it[1]
                movie_rating = 0
                for element in values:
                    for val in element:
                        movie_rating = val
                movie_column = movie_ids[movie_id]
                user_row = user_ids[user_id]
                movie_rating = movie_rating
                w[user_row, :] -= 2 * (movie_rating - w[user_row, :] * h[:, movie_column]) * h[:, movie_column] + 2 * lambda_value / num_ratings_per_user[user_id] * numpy.transpose(w[user_row, :])
                h[:, movie_column] = -2 * (movie_rating - w[user_row, :] * h[:, movie_column]) * numpy.transpose(w[user_row, :]) + 2 * lambda_value / num_ratings_per_movie[movie_id] * h[:, movie_column]
        def get_movie_user_pairs(element):
            """
            Filter that returns True if element
            is a valid part of a stratum during the current iteration.
            """
            for block_row in iteration_ranges[iteration].keys():
                block = iteration_ranges[iteration][block_row]
                if block["min_user"] <= user_ids[element[1]] and block["max_user"] > user_ids[element[1]] and block["min_movie"] <= movie_ids[element[0]] and block["max_movie"] > movie_ids[element[0]]:
                    return True
            return False
        partition = all_data.filter(get_movie_user_pairs).map(lambda x: ((x[0],x[1]), x[2]))      
        partitioned_data = all_data.map(lambda x: ((x[0],x[1]), x[2])).cogroup(partition, num_workers) 
        partitioned_data.foreachPartition(update_sgd)
        iteration += 1

def reorder_data_from_dir(partition):
    first_element = True
    current_movie_id = 0
    for element in partition:
        if first_element or ":" in element:
            current_movie_id = element.replace(":", "")
            first_element = False
        else:
            yield (int(current_movie_id), int(element.split(",")[0]), int(element.split(",")[1]))

def reorder_data_from_file(partition):
    for element in partition:
        parts = element.split(",")
        yield (int(parts[0]), int(parts[1]), int(parts[2]))

def get_num_ratings_per_user(textfiles):
    userresult1 = textfiles.map(lambda line: (line[1], 1))
    userresult2 = userresult1.reduceByKey(lambda v1, v2: v1 + v2)
    return {user_id: number for user_id, number in userresult2.collect()}

def get_num_ratings_per_movie(textfiles):
    movieresult1 = tuples.map(lambda line: (line[0], 1)).reduceByKey(lambda v1, v2: v1 + v2)
    return {movie_id: number for movie_id, number in movieresult1.collect()}

def write_output(matrix, filename):
    numpy.savetxt(filename, matrix, delimiter=",")

#set up environment
os.environ["SPARK_HOME"] = "/home/mtydykov/Downloads/spark/spark-1.3.0"
conf = SparkConf().setAppName("whatever").setMaster("local")
sc = SparkContext(conf=conf)

#read CL args
num_factors = int(sys.argv[1])
num_workers = int(sys.argv[2])
num_iterations = int(sys.argv[3])
beta_value = float(sys.argv[4])
lambda_value = float(sys.argv[5])
input_V_filepath = sys.argv[6]
output_W_filepath = sys.argv[7]
outoutH_filepath = sys.argv[8]
fromDir = False
textfiles = sc.textFile(input_V_filepath)
#transform data, whether from directory or autolab format, into one format
if os.path.isdir(input_V_filepath):
    fromDir = True
    tuples = textfiles.mapPartitions(reorder_data_from_dir, False)
else:
    tuples = textfiles.mapPartitions(reorder_data_from_file, False)


user_ids = {user_id: index for user_id, index in tuples.map(lambda line: line[1]).distinct().sortBy(lambda x: x).zipWithIndex().collect()}
movie_ids = {movie_id: index for movie_id, index in tuples.map(lambda line: line[0]).distinct().sortBy(lambda x: x).zipWithIndex().collect()}
num_ratings_per_movie = get_num_ratings_per_movie(tuples)
num_ratings_per_user = get_num_ratings_per_user(tuples)
num_movies, num_users = get_matrix_dimensions(tuples)
h = numpy.array([[random.randint(0, 10) for col in range(num_movies)] for row in range(num_factors)])
w = numpy.array([[random.randint(0, 10) for col in range(num_factors)] for row in range(num_users)])
run_sgd(w, h, num_iterations, beta_value, lambda_value, num_workers, user_ids, movie_ids, tuples, num_ratings_per_user, num_ratings_per_movie)
outfile_w = file("w.csv", "w")
write_output(w, outfile_w)
outfile_h = file("h.csv","w")
write_output(h, outfile_h)