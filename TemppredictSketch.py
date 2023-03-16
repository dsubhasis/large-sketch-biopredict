from explore.TempredictSketch import sketch
from joblib import Parallel, delayed
import multiprocessing
import csv

num_cores = multiprocessing.cpu_count()
path_index="/home/jovyan/postgres_public_full_catalog.csv"
with open(path_index, mode='r') as infile:
    reader = csv.reader(infile)
    dcp="/cephfs/tempredict/tempredict-base/"
    mydict = {}
    i = 0
    for rows in reader:
            i= i+ 1
            mydict[rows[1]]= dcp+ str(rows[0])
def split_dictionary(input_dict, chunk_size):
    res = []
    new_dict = {}
    for k, v in input_dict.items():
        if len(new_dict) < chunk_size:
            new_dict[k] = v
        else:
            res.append(new_dict)
            new_dict = {k: v}
    res.append(new_dict)
    return res
dicts = split_dictionary(mydict, num_cores)

def uprocess(mydict):
    for key, value in mydict.items():
        print("1."+str(value))
        t = sketch()
        t.uint_sketch(value, key)

Parallel(n_jobs=num_cores, verbose=10)(delayed(uprocess)(x) for x in dicts)