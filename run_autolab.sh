#!/bin/bash
# for autolab
python eval_true.py autolab_exp/eval_acc.log /home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 3 100 0.9 0.1 autolab_train.csv autolab_exp/w.csv autolab_exp/h.csv > autolab_exp/spark_dsgd.log 2>&1

# exp 1

/home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 4 100 0.6 0.1 nf_subsample.csv w.csv h.csv exp1/nzslerrors_exp1 > exp1/experiment_1_log 2>&1

#exp2
time /home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 2 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp2/nzslerrors_exp2_2workers> exp2/experiment2_2workers_log 2>&1
time /home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 3 30 0.6 0.1 nf_subsample.csv w.csv h.csv nexp2/zslerrors_exp2_3workers> exp2/experiment2_3workers_log 2>&1
time /home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 4 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp2/nzslerrors_exp2_4workers> exp2/experiment2_4workers_log 2>&1
time /home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 5 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp2/nzslerrors_exp2_5workers> exp2/experiment2_5workers_log 2>&1
time /home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 6 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp2/nzslerrors_exp2_6workers> exp2/experiment2_6workers_log 2>&1
time /home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 7 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp2/nzslerrors_exp2_7workers> exp2/experiment2_7workers_log 2>&1
time /home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 8 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp2/nzslerrors_exp2_8workers> exp2/experiment2_8workers_log 2>&1
time /home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 9 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp2/nzslerrors_exp2_9workers> exp2/experiment2_9workers_log 2>&1
time /home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 10 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp2/nzslerrors_exp2_10workers> exp2/experiment2_10workers_log 2>&1

#exp3
/home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 10 4 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp3/nzslerrors_exp3_10factors> exp3/experiment3_10factors_log
/home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 4 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp3/nzslerrors_exp3_20factors> exp3/experiment3_20factors_log
/home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 30 4 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp3/nzslerrors_exp3_30factors> exp3/experiment3_30factors_log
/home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 40 4 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp3/nzslerrors_exp3_40factors> exp3/experiment3_40factors_log
/home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 50 4 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp3/nzslerrors_exp3_50factors> exp3/experiment3_50factors_log
/home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 60 4 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp3/nzslerrors_exp3_60factors> exp3/experiment3_60factors_log
/home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 70 4 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp3/nzslerrors_exp3_70factors> exp3/experiment3_70factors_log
/home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 80 4 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp3/nzslerrors_exp3_80factors> exp3/experiment3_80factors_log
/home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 90 4 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp3/nzslerrors_exp3_90factors> exp3/experiment3_90factors_log
/home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 100 4 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp3/nzslerrors_exp3_100factors> exp3/experiment3_100factors_log

#exp4
/home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 4 30 0.5 0.1 nf_subsample.csv w.csv h.csv exp4/nzslerrors_exp4_beta05> exp4/experiment4_beta05_log
/home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 4 30 0.6 0.1 nf_subsample.csv w.csv h.csv exp4/nzslerrors_exp4_beta06> exp4/experiment4_beta06_log
/home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 4 30 0.7 0.1 nf_subsample.csv w.csv h.csv exp4/nzslerrors_exp4_beta07> exp4/experiment4_beta07_log
/home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 4 30 0.8 0.1 nf_subsample.csv w.csv h.csv exp4/nzslerrors_exp4_beta08> exp4/experiment4_beta08_log
/home/mtydykov/Downloads/spark/spark-1.3.0/bin/spark-submit dsgd_mf.py 20 4 30 0.9 0.1 nf_subsample.csv w.csv h.csv exp4/nzslerrors_exp4_beta09> exp4/experiment4_beta09_log
