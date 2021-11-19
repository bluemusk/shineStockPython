# coding=utf-8
import pandas as pd
import warnings
import sys
import datetime
import os
import pickle

def checkCondition(data, condition):
    condition = pd.DataFrame(condition.split(' AND '))

    for i in range(0, condition.shape[0]):
        if condition.iloc[i, 0].find('<=') > 0:
            con = condition.iloc[i, 0].split('<=')[0]
            con = str(con).replace(' ', '')

            try:
                val = float(condition.iloc[i, 0].split('<=')[1])
                data = data[data[con] <= val]
            except Exception as e:
                val = condition.iloc[i, 0].split('<=')[1]
                val = str(val).replace(' ', '')
                data = data[data[con] <= data[val]]
                pass

        else:
            con = condition.iloc[i, 0].split('>')[0]
            con = str(con).replace(' ', '')

            try:
                val = float(condition.iloc[i, 0].split('>')[1])
                data = data[data[con] > val]
            except Exception as e:
                val = condition.iloc[i, 0].split('>')[1]
                val = str(val).replace(' ', '')
                data = data[data[con] > data[val]]
                pass

    return data, data.value_counts('pur_gubn5')

def checkDcnt(initData, initCond, cond, condDcnt, condDvsb):
    mod = sys.modules[__name__]

    if condDcnt > 0:
        cond = cond[cond['dcnt'] > condDcnt]

    if condDvsb > 0:
        cond = cond[cond['dvsb'] > condDvsb]

    cond = cond.sort_values(by='dvsb', ascending=False)

    if initCond != '':
        initData, datCnt = checkCondition(initData, initCond)

    setattr(mod, 'data_0', initData)

    finalConds = pd.DataFrame()
    for i in range(0, cond.shape[0]):
        # print(i)
        initBcnt = cond.iloc[i]['bcnt']
        initDcnt = cond.iloc[i]['dcnt']
        print(cond.iloc[i]['condi'])
        tmpData, tmpCnt =  checkCondition(getattr(mod, 'data_{}'.format(i)), cond.iloc[i]['condi'])

        try:
            tmpDcnt = tmpCnt[0]
        except Exception as e:
            tmpDcnt = 0

        try:
            tmpBcnt = tmpCnt[1]
        except Exception as e:
            tmpBcnt = 0

        if initDcnt <= tmpDcnt and initBcnt >= tmpBcnt:
            finalConds = finalConds.append(cond.iloc[i])
            setattr(mod, 'data_{}'.format(i+1), getattr(mod, 'data_{}'.format(i)).drop(tmpData.index))
        else:
            setattr(mod, 'data_{}'.format(i+1), getattr(mod, 'data_{}'.format(i)))

        if i > 3:
            delattr(mod, 'data_{}'.format(i-1))
    return finalConds



cond = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy1_close_h4_9year_10071037.csv")  # run을 통해 만들어진 조건 파일명
# initData = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inodel02_newcolm_close_h4_9year_202009.csv")
initData = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy1_close_h4_9year.csv")


# 초기 데이터 마트를 만들기 위한 조건
# initCond = 'momp_arc7 > 89'
initCond = ''
# 초기데이터, 초기데이터에 적용하는 첫 조건, 조건들, dcnt, dvsb
finalResult = checkDcnt(initData, initCond, cond, 7, 0.6)
# csv 파일 생성
finalResult.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy1_close_h4_9year_new_20211003_final.csv")
