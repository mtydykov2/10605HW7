'''
Created on Apr 9, 2015

@author: root
'''
import random
import numpy
num_movies = 10
num_users = 3
num_factors = 2
factor_h = []
factor_w = []
for row in range(num_movies):
    array = numpy.array([random.randint(0, 10) for col in range(num_factors)])
    factor_h+=[(row, array)]
for row in range(num_users):
    array = numpy.array([random.randint(0, 10) for col in range(num_factors)])
    factor_w+=[(row, array)]
print factor_w