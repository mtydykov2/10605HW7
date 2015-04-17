#!/bin/bash
time spark/bin/spark-submit dsgd_mf.py 20 2 30 0.6 0.1 /hw7/nf_subsample.csv w.csv h.csv exp2/nzslerrors_exp2_2workers> exp2/experiment2_2workers_log 2>&1
time spark/bin/spark-submit dsgd_mf.py 20 3 30 0.6 0.1 /hw7/nf_subsample.csv w.csv h.csv nexp2/zslerrors_exp2_3workers> exp2/experiment2_3workers_log 2>&1
time spark/bin/spark-submit dsgd_mf.py 20 4 30 0.6 0.1 /hw7/nf_subsample.csv w.csv h.csv exp2/nzslerrors_exp2_4workers> exp2/experiment2_4workers_log 2>&1
time spark/bin/spark-submit dsgd_mf.py 20 5 30 0.6 0.1 /hw7/nf_subsample.csv w.csv h.csv exp2/nzslerrors_exp2_5workers> exp2/experiment2_5workers_log 2>&1
time spark/bin/spark-submit dsgd_mf.py 20 6 30 0.6 0.1 /hw7/nf_subsample.csv w.csv h.csv exp2/nzslerrors_exp2_6workers> exp2/experiment2_6workers_log 2>&1
time spark/bin/spark-submit dsgd_mf.py 20 7 30 0.6 0.1 /hw7/nf_subsample.csv w.csv h.csv exp2/nzslerrors_exp2_7workers> exp2/experiment2_7workers_log 2>&1
time spark/bin/spark-submit dsgd_mf.py 20 8 30 0.6 0.1 /hw7/nf_subsample.csv w.csv h.csv exp2/nzslerrors_exp2_8workers> exp2/experiment2_8workers_log 2>&1
time spark/bin/spark-submit dsgd_mf.py 20 9 30 0.6 0.1 /hw7/nf_subsample.csv w.csv h.csv exp2/nzslerrors_exp2_9workers> exp2/experiment2_9workers_log 2>&1
time spark/bin/spark-submit dsgd_mf.py 20 10 30 0.6 0.1 /hw7/nf_subsample.csv w.csv h.csv exp2/nzslerrors_exp2_10workers> exp2/experiment2_10workers_log 2>&1
