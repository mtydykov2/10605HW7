'''
Created on Apr 2, 2015

@author: root
'''

import sys
import os
from pyspark import SparkContext, SparkConf, Accumulator
import random
import numpy
import math
from functools import partial
from operator import add
import collections

def g(x):
    print x
    
def f(x):
    print "NEW ITERATION"
    for iter in x:
        print iter

def get_matrix_dimensions(data, is_from_dir):
    """
    Given data in form of tuples, return #
    of movies and users.
    """
    if is_from_dir:
        number_movies = data.map(lambda element: element[1]).distinct().count()
        number_users = data.map(lambda element: element[0]).distinct().count()
    else:
        number_movies = data.map(lambda element: element[1]).max()
        number_users = data.map(lambda element: element[0]).max()
    return number_movies, number_users

def get_iteration_ranges(len_w, len_h, number_workers):
    """
    Given the dimensions of w and h and the number of workers,
    determine the range of valid indeces for each block row for each iteration.
    """
    width = len_h/number_workers
    iteration_ranges = {}
    
    initial_stratum_index = 1
    for i in range(0, number_workers):
        stratum = {}
        prev_max = initial_stratum_index
        #every worker get some number of rows
        for block_row in range(number_workers):
            stratum[block_row] = {}
            min_movie = prev_max
            if min_movie >= len_h: min_movie = 1
            max_movie = min_movie + width
            if (len_h - max_movie) < width/2: max_movie = len_h
            prev_max = max_movie
            #min_movie = (block_row*width + width*i)%len_h+1
            stratum[block_row]["min_movie"] = min_movie
            stratum[block_row]["max_movie"] = max_movie
        initial_stratum_index+= width
        iteration_ranges[i] = stratum
    return iteration_ranges

def create_h_maps(len_w, len_h, number_workers, h_rdd):
    """
    Chop h into chunks based on num_workers
    """
    rdd_blocks = {}
    width = len_h/number_workers
    min_movie = 1
    for stratum_num in range(number_workers):
        max_movie = min_movie + width
        if (len_h - max_movie) < width/2: max_movie = len_h
        rdd_blocks[(min_movie,max_movie)] = h_rdd.filter(lambda x: x[0] >= min_movie and x[0] < max_movie).collectAsMap()
        min_movie = max_movie
    return rdd_blocks

def calc_epsilon(num_updates, num_local_updates, beta_value):
    """
    Calculates epsilon for the given data point.
    """
    iteration_estimated =  num_updates + num_local_updates
    epsilon = math.pow((100 + iteration_estimated),(-1*beta_value))
    return epsilon
    
def calc_regularizations(lambda_value, num_ratings_per_user, num_ratings_per_movie, user_id, movie_id, user_vals, movie_vals):
    """ 
    Calculate regularizations.
    """
    l2_w = 2 * lambda_value / num_ratings_per_user[user_id] * numpy.transpose(user_vals[user_id])
    l2_h = 2 * lambda_value / num_ratings_per_movie[movie_id] * movie_vals[movie_id]
    return l2_w, l2_h

def update_matrices(lambda_value, beta_value, num_ratings_per_user, num_ratings_per_movie, num_updates, broadcasted_maps,iteration_ranges_b, stratum_num_b, iterable):
    """
    Perform updates to factor matrices on each iteration.
    Data looks like: u_id, (m_id, rating, <w values>)
    """
    user_vals = {}
    movie_vals = {}
    num_local_updates = 0
    total_squared_error = 0
    for data in iterable:
        worker_num, user_id, movie_id, movie_rating, iterable_w = data
        if user_id not in user_vals.keys():
            user_vals[user_id] = iterable_w
        if movie_id not in movie_vals.keys():
            min = iteration_ranges_b.value[stratum_num_b.value][worker_num]["min_movie"]
            max = iteration_ranges_b.value[stratum_num_b.value][worker_num]["max_movie"]
            needed_map = broadcasted_maps[(min,max)].value
            movie_vals[movie_id] = needed_map[movie_id]  
        epsilon = calc_epsilon(num_updates.value, num_local_updates, beta_value.value)
        l2_w, l2_h = calc_regularizations(lambda_value.value, num_ratings_per_user.value, num_ratings_per_movie.value, user_id, movie_id, user_vals, movie_vals)
        error = movie_rating -numpy.dot(user_vals[user_id],movie_vals[movie_id])
        user_vals[user_id] -= epsilon*(-2 * error * movie_vals[movie_id] + l2_w)
        movie_vals[movie_id] -= epsilon*(-2 * error* numpy.transpose(user_vals[user_id]) + l2_h)
        total_squared_error+=error**2
        num_local_updates+=1
    #yield final values for this iteration
    for user_id in user_vals.keys():
        yield (user_id, user_vals[user_id], "users")
    for movie_id in movie_vals.keys():
        yield (movie_id, movie_vals[movie_id], "movies", worker_num)
    yield ("error", total_squared_error)

def get_partition(index, iterator):
    """
    Return the index of the given partition.
    """
    for val in iterator:
        yield (index, val)

def filter_partition(iteration, iteration_ranges, x):
    """
    Given the stratification dictionary, the current iteration,
    and the current data point, return True if
    the data point belongs in this iteration.
    """
    index = x[0]
    val = x[1]
    block = iteration_ranges.value[iteration.value][index]
    return block["min_movie"] <= val[1][0] and block["max_movie"] > val[1][0]

def run_sgd(w_rdd, h_rdd, num_iterations, beta_value, lambda_value, num_workers, all_data, num_ratings_per_user, num_ratings_per_movie, max_w, max_h, get_error, sc, errorfile):
    """
    Run stochastic gradient descent until convergence.
    """
    iteration = 0 
                        #uid, ((mid, rating), (<w>))
    final = all_data.map(lambda x: (x[0], (x[1], x[2]))).join(w_rdd).map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][1]))).partitionBy(num_workers)
    final.persist()
    iteration_ranges = get_iteration_ranges(max_w, max_h+1, num_workers)
    rdd_maps = create_h_maps(max_w, max_h+1, num_workers, h_rdd)
    iteration_ranges_b = sc.broadcast(iteration_ranges)
    num_updates = 0
    total_squared_error_for_iteration = {}
    final_iteration = None
    lambda_value_b = sc.broadcast(lambda_value)
    beta_value_b = sc.broadcast(beta_value) 
    num_ratings_per_user_b = sc.broadcast(num_ratings_per_user)
    num_ratings_per_movie_b = sc.broadcast(num_ratings_per_movie)
    errorfile_out = None
    previous_error_for_iteration = 0
    total_error_for_iteration = 100
    broadcasts = {}
    if get_error:
        errorfile_out = open(errorfile,"w")
    done = False
    while not done:
        previous_error_for_iteration = total_error_for_iteration
        total_error_for_iteration = 0
        if iteration == 0:
            final_iteration = final.map(lambda x: x, True)
            for dictionary in rdd_maps.keys():
                broadcasts[dictionary] = sc.broadcast(rdd_maps[dictionary])
        for strata in range(num_workers):
            strata_b = sc.broadcast(strata)
            
            #filter data such that rows form each partition only work on 
            #indices specifically assigned for the current iteration
            if iteration*strata % 10 == 0: final_iteration.count()
            filtered_data = final_iteration.mapPartitionsWithIndex(get_partition, True).filter(partial(filter_partition, strata_b, iteration_ranges_b)).map(lambda x: (x[0], x[1][0], x[1][1][0], x[1][1][1], x[1][1][2]), True)
            num_updates += filtered_data.count()
            num_updates_b = sc.broadcast(num_updates)
            new_rdd = filtered_data.mapPartitions(partial(update_matrices, lambda_value_b, beta_value_b, num_ratings_per_user_b, num_ratings_per_movie_b, num_updates_b, broadcasts, iteration_ranges_b, strata_b), True)
            if iteration*strata % 10 == 0: new_rdd.count()
            total_error_for_iteration += new_rdd.filter(lambda x: x[0] == "error").reduceByKey(add).collectAsMap()["error"]
            #get back separate w and h matrices from combined values returned by update_matrices
            new_w_rdd = new_rdd.filter(lambda x: len(x) ==3 and x[2] == "users").map(lambda x: (x[0], x[1]), True)
            if iteration*strata % 10 == 0: new_w_rdd.count()
            #if we've lost any users or movies because they didn't appear
            #in any blocks in current iteration, put them back
            next_w_rdd = w_rdd.subtractByKey(new_w_rdd)
            if iteration*strata % 10 == 0: next_w_rdd.count()
            w_rdd = next_w_rdd.union(new_w_rdd).partitionBy(num_workers)
            w_rdd.persist()
            if iteration*strata % 10 == 0: w_rdd.count()
            for worker_num in range(num_workers):
                new_h_map = new_rdd.filter(lambda x: len(x) > 2 and x[2] == "movies" and x[3] == worker_num).map(lambda x: (x[0], x[1]), True).collectAsMap()
                min, max = iteration_ranges_b.value[strata][worker_num]["min_movie"],iteration_ranges_b.value[strata][worker_num]["max_movie"]
                broadcasts[(min, max)].unpersist()
                rdd_maps[(min, max)].update(new_h_map)
                broadcasts[(min, max)] = sc.broadcast(rdd_maps[(min, max)])

            # get rid of old w and join with the new one (preserving partition by users) and replacing h as well
            final_iteration = final.join(w_rdd).map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][1])), True)
            final_iteration.persist()
        #append all data points from V updated in this iteration

        iteration += 1
        if get_error:
            errorfile_out.write(str(total_error_for_iteration)+"\n")
        #always stop if # iterations surpassed    
        if (iteration > num_iterations):
            done = True
        # if running autolab experiment, stop if convergence reached
        if (not get_error) and (abs(previous_error_for_iteration - total_error_for_iteration) < .1):
            done = True
    if errorfile_out:
        errorfile_out.close()
    h_result = {}
    for rdd_range in rdd_maps.keys():
        h_result.update(rdd_maps[rdd_range])
    return h_result, w_rdd, total_squared_error_for_iteration

def reorder_data_from_file(partition):
    """
    Given data from file, transform to tuple of
    form (user_id, movie_id, rating).
    """
    for element in partition:
        parts = element.split(",")
        yield (int(parts[0]), int(parts[1]), float(parts[2]))

def get_num_ratings_per_user(data):
    """
    Get number of ratings per user id.
    """
    userresult1 = data.map(lambda line: (line[0], 1))
    userresult2 = userresult1.reduceByKey(lambda v1, v2: v1 + v2)
    return dict((user_id, number) for user_id, number in userresult2.collect())

def get_num_ratings_per_movie(data):
    """
    Get number of ratings per movie id.
    """
    mapped_data = data.map(lambda line: (line[1], 1))
    reduced_data = mapped_data.reduceByKey(lambda v1, v2: v1 + v2)
    return dict((movie_id, number) for movie_id, number in reduced_data.collect())

def write_output(matrix, filename):
    """
    Given a matrix and filename,
    write to file using numpy.
    """
    numpy.savetxt(filename, matrix, delimiter=",")
    
def output_w_h(factor_w, h, output_H_filepath, output_W_filepath):
    """
    Output w and h to files.
    """
    w = factor_w.sortByKey().collect()
    h = collections.OrderedDict(sorted(h.items()))
    w_numpy = w[0][1]
    h_numpy = None
    for row in w[1:]:
        w_numpy = numpy.vstack((w_numpy, row[1]))
    for row in h:
        if h_numpy == None:
            h_numpy = h[row[1]]
        else:
            h_numpy = numpy.vstack((h_numpy, h[row[1]]))
    h_numpy = numpy.transpose(h_numpy)
    write_output(h_numpy, output_H_filepath)
    write_output(w_numpy, output_W_filepath)

def flatten_values(x):
    """
    Given contents for each movie, return 
    a list of tuples with user ID as key
    and movie id and rating as values.
    """
    entire_file = x[1].split("\n")
    movie_id = entire_file[0][:-1]
    tuples = []
    for values in entire_file[1:]:
        elements= values.split(",")
        if len(elements) > 1:
            tuples.append(tuple((int(elements[0]), int(movie_id), int(elements[1]))))
    return tuples

def run_from_dir(directory, num_iterations, beta_value, lambda_value, num_factors, num_workers, output_H_filepath, output_W_filepath, sc, errorfile):
    """
    Runs SGD if input is from a directory.
    """
    #wholeTextFiles
    tuples = sc.wholeTextFiles(directory).flatMap(flatten_values)
    #map ids to column/row numbers if from directory
    user_ids = dict((user_id, index) for user_id, index in (tuples.
                                                  map(lambda line: line[0]).
                                                  distinct().
                                                  sortBy(lambda x: x).
                                                  zipWithIndex().collect()))
    movie_ids = dict((movie_id, index) for movie_id, index in (tuples.
                                                     map(lambda line:
                                                         line[1]).
                                                     distinct().
                                                     sortBy(lambda x:
                                                            x).
                                                     zipWithIndex().
                                                     collect()))
    tuples.map(lambda x: (user_ids[x[0]], movie_ids[x[1]], x[2]))
    num_movies, num_users = get_matrix_dimensions(tuples, True)
    factor_h, factor_w, total_squared_error_for_iteration = setup_and_run(tuples, num_movies, num_users, num_iterations, beta_value, lambda_value, num_factors, num_workers, output_H_filepath, output_W_filepath, sc, True, errorfile)
    #output_error(total_squared_error_for_iteration)

def run_netflix(textfiles, num_iterations, beta_value, lambda_value, num_factors, num_workers, output_H_filepath, output_W_filepath, sc, errorfile):
    tuples = textfiles.mapPartitions(reorder_data_from_file, False)
    user_ids = dict((user_id, index) for user_id, index in (tuples.
                                                  map(lambda line: line[0]).
                                                  distinct().
                                                  sortBy(lambda x: x).
                                                  zipWithIndex().collect()))
    movie_ids = dict((movie_id, index) for movie_id, index in (tuples.
                                                     map(lambda line:
                                                         line[1]).
                                                     distinct().
                                                     sortBy(lambda x:
                                                            x).
                                                     zipWithIndex().
                                                     collect()))
    tuples.map(lambda x: (user_ids[x[0]], movie_ids[x[1]], x[2]))
    num_movies, num_users = get_matrix_dimensions(tuples, True)
    setup_and_run(tuples, num_movies, num_users, num_iterations, beta_value, lambda_value, num_factors, num_workers, output_H_filepath, output_W_filepath, sc, True, errorfile)

def run_from_file(textfiles, num_iterations, beta_value, lambda_value, num_factors, num_workers, output_H_filepath, output_W_filepath, sc, errorfile):
    """
    Runs SGD if input is from file.
    """
    tuples = textfiles.mapPartitions(reorder_data_from_file, False)
    num_movies, num_users = get_matrix_dimensions(tuples, False)
    factor_h, factor_w, total_squared_error_for_iteration = setup_and_run(tuples, num_movies, num_users, num_iterations, beta_value, lambda_value, num_factors, num_workers, output_H_filepath, output_W_filepath, sc, False, errorfile)
    output_w_h(factor_w, factor_h, output_H_filepath, output_W_filepath)

def setup_and_run(tuples, num_movies, num_users, num_iterations, beta_value, lambda_value, num_factors, num_workers, output_H_filepath, output_W_filepath, sc, get_error, errorfile):
    """
    Set up and setup_and_run SGD for the given loaded data.
    """
    num_ratings_per_movie = get_num_ratings_per_movie(tuples)
    num_ratings_per_user = get_num_ratings_per_user(tuples)
    factor_h = []
    factor_w = []
    for row in range(num_movies):
        array = numpy.array([random.random() for col in range(num_factors)])
        factor_h+=[(row + 1, array)]
    for row in range(num_users):
        array = numpy.array([random.random() for col in range(num_factors)])
        factor_w+=[(row + 1, array)] 
    w_rdd = sc.parallelize(factor_w).partitionBy(num_workers)
    h_rdd = sc.parallelize(factor_h) 
    factor_h, factor_w, total_squared_error_for_iteration = run_sgd(w_rdd, h_rdd, num_iterations, beta_value, lambda_value, num_workers, tuples, num_ratings_per_user, num_ratings_per_movie, num_users, num_movies, get_error, sc, errorfile)
    return factor_h, factor_w, total_squared_error_for_iteration

def run_all(num_factors, num_workers, num_iterations, beta_value, lambda_value, input_V_filepath, output_W_filepath, output_H_filepath, errorfile):
    """
    Sets up and runs SGD.
    """
    task = str(random.random())
    #set up environment
    #os.environ["SPARK_HOME"] = "/home/mtydykov/Downloads/spark/spark-1.3.0"
    #conf = SparkConf().setAppName("whatever"+task).setMaster("local[%s]" % num_workers)
    os.environ["SPARK_HOME"] = "spark"
    conf = SparkConf().setAppName("whatever"+task).setMaster("spark://ip-172-31-35-43.us-west-2.compute.internal:7077")
    sc = SparkContext(conf=conf)
    textfiles = sc.textFile(input_V_filepath)
    #transform data, whether from directory or autolab format, into one format
    if "autolab_train" not in input_V_filepath:
        run_netflix(textfiles, num_iterations, beta_value, lambda_value, num_factors, num_workers, output_H_filepath, output_W_filepath, sc, errorfile)
    else:
        run_from_file(textfiles, num_iterations, beta_value, lambda_value, num_factors, num_workers, output_H_filepath, output_W_filepath, sc, errorfile)

if "autolab_train" in sys.argv[6]:
    run_all(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]),
        float(sys.argv[4]), float(sys.argv[5]), sys.argv[6], sys.argv[7], sys.argv[8], None)
else:
    run_all(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]),
        float(sys.argv[4]), float(sys.argv[5]), sys.argv[6], sys.argv[7], sys.argv[8], sys.argv[9])