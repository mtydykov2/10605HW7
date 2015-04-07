'''
Created on Apr 7, 2015

@author: root
'''
import numpy
import random 

factor_h = numpy.array([[random.randint(0, 10) for col in range(5)]
                            for row in range(2)])
factor_w = numpy.array([[random.randint(0, 10) for col in range(2)]
                            for row in range(5)])
numpy.savetxt("h_correct_answer", factor_h)
numpy.savetxt("w_correct_answer", factor_w)

result = numpy.dot(factor_w, factor_h)
numpy.savetxt("test_dsgd_mf_correct_answer", result)
out = ""
for row in range(len(result)):
    for col in range(len(result[row])):
        out+= str(row) + "," + str(col) +"," + str(result[row,col])+"\n"
f = open("test_dsgd_mf","w")
f.write(out)
f.close()