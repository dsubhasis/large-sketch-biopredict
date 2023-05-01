from explore.smartframe import bioframe
from joblib import Parallel, delayed
import multiprocessing
import csv

from datetime import datetime

import pandas as pd
from scipy.stats import kurtosis


class sketch:
    def uint_sketch(self, paths, uid, tw="300s", var='temperature', prefix_dir="/cephfs/tempredict/sketch/",
                    fillin=True):
        try:
            b = bioframe()
            df, sql = b.load(paths, uid=uid)
            df["timestamp_utc_dt"] = df.apply(lambda x: self.local_time(x["timestamp_utc"]), axis=1)
            tf = []
            tf.append("timestamp_utc_dt")
            b.add_local_time(df, tf, utc='timezone_offset_minutes')
            p_inxl = df.set_index(['timestamp_utc_dt_local'])
            p_inx_t = p_inxl.drop(
                columns=['timezone_offset_minutes', 'participant_id', 'file_format', 'data_type', 'timestamp_utc',
                         'timestamp_utc_dt', 'study_id'])
            p_inx = p_inx_t.compute()
            sktch_agg = None
            p_sample = p_inx.resample(tw)
            if fillin:
                pskt = p_sample.agg({kurtosis, 'std', 'mean', 'skew', 'var', 'count', 'min', 'max'})
            else:
                pskt = p_sample.agg({kurtosis, 'std', 'mean', 'skew', 'var', 'count', 'min', 'max'})
            if pskt is not None:
                quantile_10 = p_sample.quantile(0.10)
                quantile_20 = p_sample.quantile(0.20)
                quantile_25 = p_sample.quantile(0.25)
                quantile_30 = p_sample.quantile(0.30)
                quantile_40 = p_sample.quantile(0.40)
                quantile_50 = p_sample.quantile(0.50)
                quantile_60 = p_sample.quantile(0.60)
                quantile_70 = p_sample.quantile(0.70)
                quantile_75 = p_sample.quantile(0.75)
                quantile_80 = p_sample.quantile(0.80)
                quantile_90 = p_sample.quantile(0.90)
                pskt['10_per'] = quantile_10[var]
                pskt['20_per'] = quantile_20[var]
                pskt['25_per'] = quantile_25[var]
                pskt['30_per'] = quantile_30[var]
                pskt['40_per'] = quantile_40[var]
                pskt['50_per'] = quantile_50[var]
                pskt['60_per'] = quantile_60[var]
                pskt['70_per'] = quantile_70[var]
                pskt['75_per'] = quantile_75[var]
                pskt['80_per'] = quantile_80[var]
                pskt['90_per'] = quantile_90[var]
            pskt['pid'] = uid
            pskt.to_csv(prefix_dir + str(var) + "/" + uid + ".csv", sep=',')
            return pskt;
        except Exception as e:
            print("error", e.args, var)

    def local_time(self, x):
        y = 0
        try:
            y = pd.to_datetime(datetime.fromtimestamp(x).strftime('%Y-%m-%d-%H:%M:%S'))
        except Exception as e:
            # y = datetime.strptime("1500-12-31 21:19:00 +00:00", '%Y-%m-%d %H:%M:%S %z')
            print(str(e))
        return y


num_cores = multiprocessing.cpu_count()
print(num_cores)
path_index = "/datavol/tempredict/postgres_public_full_catalog.csv"
with open(path_index, mode='r') as infile:
    reader = csv.reader(infile)
    dcp = "/datavol/tempredict/org_data_don_use/study_id=71c9af3d-1c57-4e8e-96fb-88b10ee9acb8/"
    mydict = {}
    i = 0
    for rows in reader:
        i = i + 1
        mydict[rows[1]] = dcp + str(rows[0])


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
print(len(dicts))


def uprocess(mydict):
    for key, value in mydict.items():
        # print("1."+str(value))
        t = sketch()
        try:
            t.uint_sketch(value, key, tw="300s", var="temperature",prefix_dir="/datavol/tempredict/sketch/")
        except Exception as e:
            print("error", e.message, e.args)


Parallel(n_jobs=num_cores, verbose=20)(delayed(uprocess)(x) for x in dicts)
