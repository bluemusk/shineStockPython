#################################
# coding=utf-8
import sys
import pandas as pd
import datetime
import os
import pickle
import warnings

warnings.filterwarnings('ignore')

def run2(data, condDcnt, condDvsb):
    # data = pd.read_csv('C:/Users/Shine_anal/PycharmProjects/anlaysis/kbuy1_20210927_parallel_all.csv')
    data = data[(data['dvsb'] > condDcnt) & (data['dcnt'] > condDvsb)]
    data['conLevel'] = 0
    data['dupYn'] = 'N'

    data = data.drop_duplicates()
    data = data.reset_index(drop=True)

    for i in range(0, data.shape[0]):
        try:
            data['conLevel'][i] = pd.DataFrame(data['condi'][i].split(' AND ')).shape[0]
        except Exception as e:
            data['conLevel'][i] = 0
            pass

    data = data.sort_values(by=['conLevel', 'condi'], ascending=[True, True])
    data = data.reset_index(drop=True)

    for j in range(0, data.shape[0] - 1):
        for k in range(j + 1, data.shape[0]):
            print('j : ' + str(j) + ' k : ' + str(k))
            try:
                if (data['condi'][j] in data['condi'][k]) and (data['conLevel'][j] < data['conLevel'][k]):
                    # print("j : (" + str(j) + ")" + data['condi'][j] + " / k : (" + str(k) + ")" + data['condi'][k] )
                    data['dupYn'][j] = 'Y'
            except Exception as e:
                pass

    return data[data['dupYn'] == 'N']

###############################################################################################################################
# 상위레벨 조건 지우기 
###############################################################################################################################
fResult = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/dtree_inobuyt1.csv")
finalData = run2(fResult, 7, 5) # 데이터, condDcnt, condDvsb를 넣는다..
finalData.to_csv("C:/Users/Shine_anal/Desktop/inobuyt/dtree_" + name +"_final_{}.csv".format(datetime.datetime.today().strftime("%m%d%H%M")))