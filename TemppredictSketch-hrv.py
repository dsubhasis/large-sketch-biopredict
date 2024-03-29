from explore.smartframe import bioframe
from joblib import Parallel, delayed
import multiprocessing
import csv

from datetime import datetime

import pandas as pd
from scipy.stats import kurtosis


class sketch:
    def uint_sketch(self, paths, uid, tw="300s", var='temp_skin', prefix_dir="/cephfs/tempredict/sketch/",
                    fillin=True):
        try:
            b = bioframe()
            df, sql = b.load(paths, uid=uid)
            df["timestamp_utc_dt"] = df.apply(lambda x: self.local_time(x["timestamp_utc"]), axis=1)

            tf = []
            tf.append("timestamp_utc_dt")
            b.add_local_time(df, tf, utc='timezone_offset_minutes')

            df3 = df.drop(
                columns=['timezone_offset_minutes', 'participant_id', 'file_format', 'data_type', 'timestamp_utc',
                         'timestamp_utc_dt', 'study_id'])
            # p_inx = p_inx_t.compute()
            print(df3.head(10))
            df2 = df3.compute()

            p_inxl = df2.set_index(['timestamp_utc_dt_local'])
            sktch_agg = None
            p_sample = p_inxl.resample(tw)

            if fillin:
                pskt = p_sample.agg({kurtosis, 'std', 'mean', 'skew', 'var', 'count', 'min', 'max'})
            else:
                pskt = p_sample.agg({kurtosis, 'std', 'mean', 'skew', 'var', 'count', 'min', 'max'})
            if pskt is not None:
                    quantile_10 = p_sample['HR'].quantile(0.10)
                    quantile_20 = p_sample['HR'].quantile(0.20)
                    quantile_25 = p_sample['HR'].quantile(0.25)
                    quantile_30 = p_sample['HR'].quantile(0.30)
                    quantile_40 = p_sample['HR'].quantile(0.40)
                    quantile_50 = p_sample['HR'].quantile(0.50)
                    quantile_60 = p_sample['HR'].quantile(0.60)
                    quantile_70 = p_sample['HR'].quantile(0.70)
                    quantile_75 = p_sample['HR'].quantile(0.75)
                    quantile_80 = p_sample['HR'].quantile(0.80)
                    quantile_90 = p_sample['HR'].quantile(0.90)
                    try:
                        pskt['HR_10_per'] = quantile_10[var]
                    except Exception as e:
                        pskt['HR_10_per'] = "NaN"
                    try:
                        pskt['HR_20_per'] = quantile_20[var]
                    except Exception as e:
                        pskt['HR_20_per'] = "NaN"
                    try:
                        pskt['HR_30_per'] = quantile_30[var]
                    except Exception as e:
                        pskt['HR_30_per'] = "NaN"
                    try:
                        pskt['HR_40_per'] = quantile_40[var]
                    except Exception as e:
                        pskt['HR_40_per'] = "NaN"
                    try:
                        pskt['HR_50_per'] = quantile_50[var]
                    except Exception as e:
                        pskt['HR_50_per'] = "NaN"
                    try:
                        pskt['HR_60_per'] = quantile_60[var]
                    except Exception as e:
                        pskt['HR_60_per'] = "NaN"
                    try:
                        pskt['HR_70_per'] = quantile_70[var]
                    except Exception as e:
                        pskt['HR_70_per'] = "NaN"
                    try:
                        pskt['HR_80_per'] = quantile_80[var]
                    except Exception as e:
                        pskt['HR_80_per'] = "NaN"
                    try:
                        pskt['HR_90_per'] = quantile_90[var]
                    except Exception as e:
                        pskt['HR_90_per'] = "NaN"

                    quantile_10 = p_sample['rMSSD'].quantile(0.10)
                    quantile_20 = p_sample['rMSSD'].quantile(0.20)
                    quantile_25 = p_sample['rMSSD'].quantile(0.25)
                    quantile_30 = p_sample['rMSSD'].quantile(0.30)
                    quantile_40 = p_sample['rMSSD'].quantile(0.40)
                    quantile_50 = p_sample['rMSSD'].quantile(0.50)
                    quantile_60 = p_sample['rMSSD'].quantile(0.60)
                    quantile_70 = p_sample['rMSSD'].quantile(0.70)
                    quantile_75 = p_sample['rMSSD'].quantile(0.75)
                    quantile_80 = p_sample['rMSSD'].quantile(0.80)
                    quantile_90 = p_sample['rMSSD'].quantile(0.90)

                    try:
                        pskt['rMSSD_10_per'] = quantile_10[var]
                    except Exception as e:
                        pskt['rMSSD_10_per'] = "NaN"
                    try:
                        pskt['rMSSD_20_per'] = quantile_20[var]
                    except Exception as e:
                        pskt['rMSSD_20_per'] = "NaN"
                    try:
                        pskt['rMSSD_30_per'] = quantile_30[var]
                    except Exception as e:
                        pskt['rMSSD_30_per'] = "NaN"
                    try:
                        pskt['rMSSD_40_per'] = quantile_40[var]
                    except Exception as e:
                        pskt['rMSSD_40_per'] = "NaN"
                    try:
                        pskt['rMSSD_50_per'] = quantile_50[var]
                    except Exception as e:
                        pskt['rMSSD_50_per'] = "NaN"
                    try:
                        pskt['rMSSD_60_per'] = quantile_60[var]
                    except Exception as e:
                        pskt['rMSSD_60_per'] = "NaN"
                    try:
                        pskt['rMSSD_70_per'] = quantile_70[var]
                    except Exception as e:
                        pskt['rMSSD_70_per'] = "NaN"
                    try:
                        pskt['rMSSD_80_per'] = quantile_80[var]
                    except Exception as e:
                        pskt['rMSSD_80_per'] = "NaN"
                    try:
                        pskt['rMSSD_90_per'] = quantile_90[var]
                    except Exception as e:
                        pskt['rMSSD_90_per'] = "NaN"



            pskt['pid'] = uid
            # print(pskt)
            pskt.to_csv(prefix_dir + str(var) + "/" + uid + ".csv", sep=',')

            return pskt;
        except Exception as e:
            print("error: after writing", e.args, var)

    def local_time(self, x):
        y = 0
        try:
            y = pd.to_datetime(datetime.fromtimestamp(x).strftime('%Y-%m-%d-%H:%M:%S'))
        except Exception as e:
            y = datetime.strptime("1500-12-31 21:19:00 +00:00", '%Y-%m-%d %H:%M:%S %z')
        return y


num_cores = multiprocessing.cpu_count()
print(num_cores)
path_index = "/datavol/tempredict/index/temppredict/postgres_public_full_catalog_hrv.csv"
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
# print(len(dicts))
def uprocess(mydict):
    for key, value in mydict.items():
        # print("1."+str(value))
        t = sketch()
        try:
            t.uint_sketch(value, key, tw="1800s", var="hrv",prefix_dir="/datavol/tempredict/sketch/30-min/")
        except Exception as e:
            print("error:: upeer uprocess", e.args)
# for x in dicts:
#     uprocess(x)
Parallel(n_jobs=num_cores, verbose=20)(delayed(uprocess)(x) for x in dicts)
