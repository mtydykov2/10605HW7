'''
Created on Apr 2, 2015

@author: root
'''

import sys
import os
from pyspark import SparkContext, SparkConf, AccumulatorParam
import random
import numpy
import math

class VectorAccumulatorParam(AccumulatorParam):
    def zero(self, initialValue):
        return numpy.array(0)

    def addInPlace(self, v1, v2):
        print "my value: " + str(self)
        print "adding " + str(v1) +  " to " + str(v2)
        return numpy.add(v1, v2)


def get_matrix_dimensions(data):
    """
    Given data in form of tuples, return #
    of movies and users.
    """
    number_movies = data.map(lambda element: element[0]).distinct().count()
    number_users = data.map(lambda element: element[1]).distinct().count()
    return number_movies, number_users

def get_iteration_ranges(w_mat, h_mat, number_workers, num_iterations):
    """
    Given the dimensions of w and h and the number of workers,
    determine the range of valid indeces for each block row for each iteration.
    """
    width = len(h_mat)/number_workers
    height = len(w_mat)/number_workers
    iteration_ranges = {}
    for i in range(0, num_iterations):
        stratum = {}
        #every worker get some number of rows
        for block_row in range(number_workers):
            stratum[block_row] = {}
            min_user = block_row*height
            min_movie = (block_row*width + width*i)%len(h_mat)
            stratum[block_row]["min_user"] = min_user
            stratum[block_row]["max_user"] = stratum[block_row]["min_user"]+height
            stratum[block_row]["min_movie"] = min_movie
            stratum[block_row]["max_movie"] = stratum[block_row]["min_movie"] + width   
        iteration_ranges[i] = stratum
    return iteration_ranges    
    
def run_sgd(w, h, num_iterations, beta_value, lambda_value, num_workers, user_ids, movie_ids, all_data, num_ratings_per_user, num_ratings_per_movie, sc):
    """
    Run stochastic gradient descent until convergence.
    """
    iteration_ranges = get_iteration_ranges(w, h, num_workers, num_iterations)
    iteration = 0 
    
    wAccum = sc.accumulator(w, VectorAccumulatorParam())
    hAccum = sc.accumulator(h, VectorAccumulatorParam())
    print "original w: " + str(wAccum.value)
    print "original h: " + str(hAccum.value)
    l2 = False
    while iteration < num_iterations:
        def update_sgd(data):
            """
            Perform updates to factor matrices on each iteration.
            """
            num_local_updates = 0
            factor_h = numpy.array([[0 for col in range(len(h[0]))]
                            for row in range(len(h))])
            factor_w = numpy.array([[0 for col in range(len(w[0]))]
                            for row in range(len(w))])
            for item in data:
                iteration_estimated = (iteration * num_workers/len(h)*len(h)*len(w)) + num_local_updates
                epsilon = math.pow((100 + iteration_estimated),(-1*beta_value))
                print "epsilon: " + str(epsilon)
                movie_id, user_id = item[0]
                values = item[1]
                movie_rating = 0
                for element in values:
                    for val in element:
                        movie_rating = val
                print "movie rating: " + str(movie_rating)

                movie_column = movie_ids[movie_id]
                user_row = user_ids[user_id]
                print "movie id: " + str(movie_id)
                print "user id: " + str(user_id)
                print "movie column: " + str(movie_column)
                print "user_row: " + str(user_row)
                
                l2_w = 0
                l2_h = 0
                if l2:
                    l2_w = 2 * lambda_value / num_ratings_per_user[user_id] * numpy.transpose(w[user_row, :])
                    l2_h = 2 * lambda_value / num_ratings_per_movie[movie_id] * h[:, movie_column]
                print "l2_w: " + str(l2_w)
                print "l2_h: " + str(l2_h)                
                #calculate amount that should be added
                #to final factor matrices
                factor_w[user_row, :] -= epsilon*(-2 * (movie_rating - w[user_row, :]*h[:, movie_column])* h[:, movie_column] +
                                                  l2_w)
                factor_h[:, movie_column] -= epsilon*(-2 * (movie_rating - w[user_row, :]*h[:, movie_column])* numpy.transpose(w[user_row, :]) + 
                                                  l2_h)
                print "factor_w: " + str(factor_w)
                print "factor_h: " + str(factor_h)

                num_local_updates+=1
                #numpy.savetxt("w_iter_"+str(iteration), w)
                #numpy.savetxt("h_iter_"+str(iteration), h)
            wAccum.add(factor_w)
            hAccum.add(factor_h)
            
        def get_movie_user_pairs(element):
            """
            Filter that returns True if element
            is a valid part of a stratum during the current iteration.
            """
            for block_row in iteration_ranges[iteration].keys():
                block = iteration_ranges[iteration][block_row]
                if (block["min_user"] <= user_ids[element[1]] and
                    block["max_user"] > user_ids[element[1]] and
                    block["min_movie"] <= movie_ids[element[0]] and
                    block["max_movie"] > movie_ids[element[0]]):
                    return True
            return False
        
        partition = (all_data.filter(get_movie_user_pairs)
                     .map(lambda x: ((x[0],x[1]), x[2])))
        partitioned_data = (all_data.map(lambda x: ((x[0],x[1]), x[2]))
                            .cogroup(partition, num_workers))
        partitioned_data.cache()
        partitioned_data.foreachPartition(update_sgd)
        w = wAccum.value
        h = hAccum.value
        print "accumulated w: " + str(wAccum.value)
        print "accumulated h: " + str(hAccum.value)
        iteration += 1
    return h, w

def reorder_data_from_dir(partition):
    """
    Given data from directory, transform to tuple of
    form (movie_id, user_id, rating).
    """
    first_element = True
    current_movie_id = 0
    for element in partition:
        if first_element or ":" in element:
            current_movie_id = element.replace(":", "")
            first_element = False
        else:
            yield (int(current_movie_id), int(element.split(",")[0]),
                   int(element.split(",")[1]))

def reorder_data_from_file(partition):
    """
    Given data from file, transform to tuple of
    form (movie_id, user_id, rating).
    """
    for element in partition:
        parts = element.split(",")
        yield (int(parts[0]), int(parts[1]), int(parts[2]))

def get_num_ratings_per_user(data):
    """
    Get number of ratings per user id.
    """
    userresult1 = data.map(lambda line: (line[1], 1))
    userresult2 = userresult1.reduceByKey(lambda v1, v2: v1 + v2)
    return {user_id: number for user_id, number in userresult2.collect()}

def get_num_ratings_per_movie(data):
    """
    Get number of ratings per movie id.
    """
    mapped_data = data.map(lambda line: (line[0], 1))
    reduced_data = mapped_data.reduceByKey(lambda v1, v2: v1 + v2)
    return {movie_id: number for movie_id, number in reduced_data.collect()}

def write_output(matrix, filename):
    """
    Given a matrix and filename,
    write to file using numpy.
    """
    numpy.savetxt(filename, matrix, delimiter=",")

def run_all(num_factors, num_workers, num_iterations, beta_value, lambda_value, input_V_filepath, output_W_filepath, output_H_filepath):
    """
    Sets up and runs SGD.
    """
    #set up environment
    os.environ["SPARK_HOME"] = "/home/mtydykov/Downloads/spark/spark-1.3.0"
    conf = SparkConf().setAppName("whatever").setMaster("local")
    sc = SparkContext(conf=conf)
    textfiles = sc.textFile(input_V_filepath)
    #transform data, whether from directory or autolab format, into one format
    if os.path.isdir(input_V_filepath):
        #wholeTextFiles
        tuples = textfiles.mapPartitions(reorder_data_from_dir, False)
    else:
        tuples = textfiles.mapPartitions(reorder_data_from_file, False)
    user_ids = {user_id: index for user_id, index in (tuples.
                                                      map(lambda line: line[1]).
                                                      distinct().
                                                      sortBy(lambda x: x).
                                                      zipWithIndex().collect())}
    movie_ids = {movie_id: index for movie_id, index in (tuples.
                                                         map(lambda line:
                                                             line[0]).
                                                         distinct().
                                                         sortBy(lambda x:
                                                                x).
                                                         zipWithIndex().
                                                         collect())}
    num_ratings_per_movie = get_num_ratings_per_movie(tuples)
    num_ratings_per_user = get_num_ratings_per_user(tuples)
    num_movies, num_users = get_matrix_dimensions(tuples)
    factor_h = numpy.array([[random.randint(0, 10) for col in range(num_movies)]
                            for row in range(num_factors)])
    factor_w = numpy.array([[random.randint(0, 10) for col in range(num_factors)]
                            for row in range(num_users)])
    factor_h, factor_w = run_sgd(factor_w, factor_h, num_iterations, beta_value, lambda_value, num_workers, user_ids, movie_ids, tuples, num_ratings_per_user, num_ratings_per_movie, sc)
    outfile_w = file(output_W_filepath, "w")
    write_output(factor_w, outfile_w)
    outfile_h = file(output_H_filepath,"w")
    print numpy.dot(factor_w, factor_h)
    write_output(factor_h, outfile_h)

run_all(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]),
        float(sys.argv[4]), float(sys.argv[5]), sys.argv[6], sys.argv[7], sys.argv[8])