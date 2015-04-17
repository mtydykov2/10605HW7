'''
Created on Apr 7, 2015

@author: root
'''
import numpy
import random 

factor_h = numpy.array([[random.uniform(0, 2) for col in range(10)]
                            for row in range(4)])
factor_w = numpy.array([[random.uniform(0, 2) for col in range(4)]
                            for row in range(12)])
numpy.savetxt("h_correct_answer", factor_h)
numpy.savetxt("w_correct_answer", factor_w)

result = numpy.dot(factor_w, factor_h)
numpy.savetxt("test_dsgd_mf_correct_answer", result)
out = ""
for row in range(len(result)):
    for col in range(len(result[row])):
        out+= str(row+1) + "," + str(col+1) +"," + str(int(result[row,col]))+"\n"
f = open("test_dsgd_mf","w")
f.write(out)
f.close()