# coding=utf-8
import pandas as pd
import warnings
import sys
import datetime
import os
import pickle

warnings.filterwarnings('ignore')
def custom_func2(col, aggr_df):
    # col = data.iloc[:,20]
    # aggr_df = data.iloc[:,4]

    mod = sys.modules[__name__]

    setattr(mod, 'num_0', round(col.quantile(0), 1))
    setattr(mod, 'num_1', round(col.quantile(0.25), 1))
    setattr(mod, 'num_2', round(col.quantile(0.5), 1))
    setattr(mod, 'num_3', round(col.quantile(0.75), 1))
    setattr(mod, 'num_4', round(col.quantile(1), 1))

    setattr(mod, 'step1', round((num_1 - num_0) / 10, 1))
    setattr(mod, 'step2', round((num_2 - num_1) / 10, 1))
    setattr(mod, 'step3', round((num_3 - num_2) / 10, 1))
    setattr(mod, 'step4', round((num_4 - num_3) / 10, 1))
    
    finalSet = pd.DataFrame()

    for j in range(0,4):
        for k in range(0,10):
            tmp1 = getattr(mod, 'num_{}'.format(j))
            tmp2 = getattr(mod, 'step{}'.format(j + 1))

            tmpData = aggr_df[(col > tmp1 + tmp2 * k)]

            try:
                bcnt = tmpData.value_counts()[1]
            except:
                bcnt = 0.8

            try:
                dcnt = tmpData.value_counts()[0]
            except:
                dcnt = 0.8

            finalSet = finalSet.append(pd.DataFrame({'condi': [str(col.name) + ' > ' + str(round(tmp1 + tmp2 * k, 1))],
                                                    'bcnt': [bcnt],
                                                    'dcnt': [dcnt],
                                                    'bvsd': [bcnt / dcnt],
                                                    'dvsb': [dcnt / bcnt]
                                                    }, index = [col.name])
                                       )

    for j in range(0,4):
        for k in range(0,10):
            tmp1 = getattr(mod, 'num_{}'.format(j))
            tmp2 = getattr(mod, 'step{}'.format(j + 1))

            tmpData = aggr_df[(col <= tmp1 + tmp2 * k)]

            try:
                bcnt = tmpData.value_counts()[1]
            except:
                bcnt = 0.8

            try:
                dcnt = tmpData.value_counts()[0]
            except:
                dcnt = 0.8

            finalSet = finalSet.append(pd.DataFrame({'condi': [str(col.name) + ' <= ' + str(round(tmp1 + tmp2 * k, 1))],
                                                     'bcnt': [bcnt],
                                                     'dcnt': [dcnt],
                                                     'bvsd': [bcnt / dcnt],
                                                     'dvsb': [dcnt / bcnt]
                                                     }, index = [col.name])
                                       )

    return finalSet

data = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy1_close_h4_9year.csv")
result = pd.DataFrame()

for i in range(10, data.shape[1]):
    result = result.append(custom_func2(data.iloc[:,i],data.iloc[:, 4]))

result.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy1_close_h4_9year_result_new.csv")

data = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy2_close_h4_9year.csv")
result = pd.DataFrame()

for i in range(10, data.shape[1]):
    result = result.append(custom_func2(data.iloc[:,i],data.iloc[:, 4]))

result.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy2_close_h4_9year_result_new.csv")

data = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy3_close_h4_9year.csv")
result = pd.DataFrame()

for i in range(10, data.shape[1]):
    result = result.append(custom_func2(data.iloc[:,i],data.iloc[:, 4]))

result.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy3_close_h4_9year_result_new.csv")

data = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy4_close_h4_9year.csv")
result = pd.DataFrame()

for i in range(10, data.shape[1]):
    result = result.append(custom_func2(data.iloc[:,i],data.iloc[:, 4]))

result.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy4_close_h4_9year_result_new.csv")