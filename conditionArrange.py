
###############################################################################################################################
# 중복 조건 지우기
###############################################################################################################################
# coding=utf-8
import sys
import pandas as pd
import datetime
import os
import pickle
import warnings

warnings.filterwarnings('ignore')

###############################################################################################################################
# dtree 결과를 읽어온다..
###############################################################################################################################
df = pd.read_csv('C:/Users/Shine_anal/Desktop/dtree_inobuyt3_final.csv')
df = df.sort_values('condi')
df = df.reset_index()
###############################################################################################################################
# 조건들을 and 로 연결되는 부분을 쪼갠다..
###############################################################################################################################
for i in range(0, df.shape[0]):
    print('i : ' + str(i) + ' / ' + df['condi'].iloc[i])
    tmpCon = ''
    tmpFin = pd.DataFrame()
    finCon = ''
    tmpCon = df['condi'].iloc[i].split(' AND ')
    tmpCon = pd.DataFrame(tmpCon, columns=['con'])
    tmpCon['con2'] = tmpCon['con']

    for j in range(0, tmpCon.shape[0]):
        tmpCon['con2'].iloc[j] = str(tmpCon['con2'].iloc[j]).upper()

    tmpFin = tmpCon.sort_values('con2')
    tmpFin = tmpFin.reset_index()

    for h in range(0, tmpFin.shape[0]):
        if h == 0:
            finCon = tmpFin.iloc[h]['con']
        else:
            finCon = finCon + ' AND ' + tmpFin.iloc[h]['con']

    df['condi'].iloc[i] = finCon
###############################################################################################################################
# 중복 조건을 삭제한다..
###############################################################################################################################
df = df.iloc[:, 2:df.shape[1]].drop_duplicates()
###############################################################################################################################
# csv파일을 생성한다..
###############################################################################################################################
df.to_csv('C:/Users/Shine_anal/Desktop/test.csv')