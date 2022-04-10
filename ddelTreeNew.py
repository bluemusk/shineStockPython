# coding=utf-8
import pandas as pd
import warnings
import sys
import datetime
import os
import pickle
import numpy as np

warnings.filterwarnings('ignore')

def makeFinalDf(data):
    # data = result
    vals = data.columns
    finalSet = pd.DataFrame()

    for val in vals:
        # val='slow3D_fast5D'
        tmpData = data[val]

        num1 = tmpData.iloc[80]
        num2 = tmpData.iloc[81]
        num3 = tmpData.iloc[82]
        num4 = tmpData.iloc[83]
        step1 = tmpData.iloc[84]
        step2 = tmpData.iloc[85]
        step3 = tmpData.iloc[86]
        step4 = tmpData.iloc[87]

        for i in range(6, 74):
            if i % 2 == 0:
                bcnt = tmpData.iloc[i]
            else:
                dcnt = tmpData.iloc[i]

                if i % 10 == 1: j = 0

                if i % 10 == 3: j = 1

                if i % 10 == 5: j = 2

                if i % 10 == 7: j = 3

                if i % 10 == 9: j = 4

                if i // 10 == 0 or i // 10 == 4:
                    conValue = num1 + (step1 * j)
                elif i // 10 == 1 or i // 10 == 5:
                    conValue = num2 + (step2 * j)
                elif i // 10 == 2 or i // 10 == 6:
                    conValue = num3 + (step3 * j)
                elif i // 10 == 3 or i // 10 == 7:
                    conValue = num4 + (step4 * j)
                # print("i : " + str(i) + " / conValue : " + str(conValue))
                if i < 40:
                    finalSet = finalSet.append(
                        pd.DataFrame([(val, round(conValue, 2), bcnt, dcnt, bcnt / dcnt, dcnt / bcnt, 'GT')],
                                     columns=['condi', 'value', 'bcnt', 'dcnt', 'bvsd', 'dvsb', 'cal']),
                        ignore_index=False)
                else:
                    finalSet = finalSet.append(
                        pd.DataFrame([(val, round(conValue, 2), bcnt, dcnt, bcnt / dcnt, dcnt / bcnt, 'LT')],
                                     columns=['condi', 'value', 'bcnt', 'dcnt', 'bvsd', 'dvsb', 'cal']),
                        ignore_index=False)

                    # df[df['slow3D_fast5D'] <= 19.37]['pur_gubn5'].value_counts()
    finalSet = finalSet.dropna(axis=0)
    return finalSet

def custom_func(col, aggr_df):
    # col = df.iloc[:,20]

    num_0 = round(col.quantile(0), 1)
    num_1 = round(col.quantile(0.25), 1)
    num_2 = round(col.quantile(0.5), 1)
    num_3 = round(col.quantile(0.75), 1)
    num_4 = round(col.quantile(1), 1)

    step1 = round((num_0 + num_1) / 5, 1)
    step2 = round((num_1 + num_2) / 5, 1)
    step3 = round((num_2 + num_3) / 5, 1)
    step4 = round((num_3 + num_4) / 5, 1)

    cnt_1 = aggr_df[(col > num_0 + step1 * 0)]
    cnt_2 = aggr_df[(col > num_0 + step1 * 1)]
    cnt_3 = aggr_df[(col > num_0 + step1 * 2)]
    cnt_4 = aggr_df[(col > num_0 + step1 * 3)]
    cnt_5 = aggr_df[(col > num_0 + step1 * 4)]
    cnt_6 = aggr_df[(col > num_1 + step2 * 0)]
    cnt_7 = aggr_df[(col > num_1 + step2 * 1)]
    cnt_8 = aggr_df[(col > num_1 + step2 * 2)]
    cnt_9 = aggr_df[(col > num_1 + step2 * 3)]
    cnt_10 = aggr_df[(col > num_1 + step2 * 4)]
    cnt_11 = aggr_df[(col > num_2 + step3 * 0)]
    cnt_12 = aggr_df[(col > num_2 + step3 * 1)]
    cnt_13 = aggr_df[(col > num_2 + step3 * 2)]
    cnt_14 = aggr_df[(col > num_2 + step3 * 3)]
    cnt_15 = aggr_df[(col > num_2 + step3 * 4)]
    cnt_16 = aggr_df[(col > num_3 + step4 * 0)]
    cnt_17 = aggr_df[(col > num_3 + step4 * 1)]
    cnt_18 = aggr_df[(col > num_3 + step4 * 2)]
    cnt_19 = aggr_df[(col > num_3 + step4 * 3)]
    cnt_20 = aggr_df[(col > num_3 + step4 * 4)]

    cnt_21 = aggr_df[(col <= num_0 + step1 * 0)]
    cnt_22 = aggr_df[(col <= num_0 + step1 * 1)]
    cnt_23 = aggr_df[(col <= num_0 + step1 * 2)]
    cnt_24 = aggr_df[(col <= num_0 + step1 * 3)]
    cnt_25 = aggr_df[(col <= num_0 + step1 * 4)]
    cnt_26 = aggr_df[(col <= num_1 + step2 * 0)]
    cnt_27 = aggr_df[(col <= num_1 + step2 * 1)]
    cnt_28 = aggr_df[(col <= num_1 + step2 * 2)]
    cnt_29 = aggr_df[(col <= num_1 + step2 * 3)]
    cnt_30 = aggr_df[(col <= num_1 + step2 * 4)]
    cnt_31 = aggr_df[(col <= num_2 + step3 * 0)]
    cnt_32 = aggr_df[(col <= num_2 + step3 * 1)]
    cnt_33 = aggr_df[(col <= num_2 + step3 * 2)]
    cnt_34 = aggr_df[(col <= num_2 + step3 * 3)]
    cnt_35 = aggr_df[(col <= num_2 + step3 * 4)]
    cnt_36 = aggr_df[(col <= num_3 + step4 * 0)]
    cnt_37 = aggr_df[(col <= num_3 + step4 * 1)]
    cnt_38 = aggr_df[(col <= num_3 + step4 * 2)]
    cnt_39 = aggr_df[(col <= num_3 + step4 * 3)]
    cnt_40 = aggr_df[(col <= num_3 + step4 * 4)]

    try:
        cnt_1_b = cnt_1.value_counts()[1]
    except:
        cnt_1_b = 0.8
    try:
        cnt_1_d = cnt_1.value_counts()[0]
    except:
        cnt_1_d = 0.8
    try:
        cnt_2_b = cnt_2.value_counts()[1]
    except:
        cnt_2_b = 0.8
    try:
        cnt_2_d = cnt_2.value_counts()[0]
    except:
        cnt_2_d = 0.8
    try:
        cnt_3_b = cnt_3.value_counts()[1]
    except:
        cnt_3_b = 0.8
    try:
        cnt_3_d = cnt_3.value_counts()[0]
    except:
        cnt_3_d = 0.8
    try:
        cnt_4_b = cnt_4.value_counts()[1]
    except:
        cnt_4_b = 0.8
    try:
        cnt_4_d = cnt_4.value_counts()[0]
    except:
        cnt_4_d = 0.8
    try:
        cnt_5_b = cnt_5.value_counts()[1]
    except:
        cnt_5_b = 0.8
    try:
        cnt_5_d = cnt_5.value_counts()[0]
    except:
        cnt_5_d = 0.8
    try:
        cnt_6_b = cnt_6.value_counts()[1]
    except:
        cnt_6_b = 0.8
    try:
        cnt_6_d = cnt_6.value_counts()[0]
    except:
        cnt_6_d = 0.8
    try:
        cnt_7_b = cnt_7.value_counts()[1]
    except:
        cnt_7_b = 0.8
    try:
        cnt_7_d = cnt_7.value_counts()[0]
    except:
        cnt_7_d = 0.8
    try:
        cnt_8_b = cnt_8.value_counts()[1]
    except:
        cnt_8_b = 0.8
    try:
        cnt_8_d = cnt_8.value_counts()[0]
    except:
        cnt_8_d = 0.8
    try:
        cnt_9_b = cnt_9.value_counts()[1]
    except:
        cnt_9_b = 0.8
    try:
        cnt_9_d = cnt_9.value_counts()[0]
    except:
        cnt_9_d = 0.8
    try:
        cnt_10_b = cnt_10.value_counts()[1]
    except:
        cnt_10_b = 0.8
    try:
        cnt_10_d = cnt_10.value_counts()[0]
    except:
        cnt_10_d = 0.8
    try:
        cnt_11_b = cnt_11.value_counts()[1]
    except:
        cnt_11_b = 0.8
    try:
        cnt_11_d = cnt_11.value_counts()[0]
    except:
        cnt_11_d = 0.8
    try:
        cnt_12_b = cnt_12.value_counts()[1]
    except:
        cnt_12_b = 0.8
    try:
        cnt_12_d = cnt_12.value_counts()[0]
    except:
        cnt_12_d = 0.8
    try:
        cnt_13_b = cnt_13.value_counts()[1]
    except:
        cnt_13_b = 0.8
    try:
        cnt_13_d = cnt_13.value_counts()[0]
    except:
        cnt_13_d = 0.8
    try:
        cnt_14_b = cnt_14.value_counts()[1]
    except:
        cnt_14_b = 0.8
    try:
        cnt_14_d = cnt_14.value_counts()[0]
    except:
        cnt_14_d = 0.8
    try:
        cnt_15_b = cnt_15.value_counts()[1]
    except:
        cnt_15_b = 0.8
    try:
        cnt_15_d = cnt_15.value_counts()[0]
    except:
        cnt_15_d = 0.8
    try:
        cnt_16_b = cnt_16.value_counts()[1]
    except:
        cnt_16_b = 0.8
    try:
        cnt_16_d = cnt_16.value_counts()[0]
    except:
        cnt_16_d = 0.8
    try:
        cnt_17_b = cnt_17.value_counts()[1]
    except:
        cnt_17_b = 0.8
    try:
        cnt_17_d = cnt_17.value_counts()[0]
    except:
        cnt_17_d = 0.8
    try:
        cnt_18_b = cnt_18.value_counts()[1]
    except:
        cnt_18_b = 0.8
    try:
        cnt_18_d = cnt_18.value_counts()[0]
    except:
        cnt_18_d = 0.8
    try:
        cnt_19_b = cnt_19.value_counts()[1]
    except:
        cnt_19_b = 0.8
    try:
        cnt_19_d = cnt_19.value_counts()[0]
    except:
        cnt_19_d = 0.8
    try:
        cnt_20_b = cnt_20.value_counts()[1]
    except:
        cnt_20_b = 0.8
    try:
        cnt_20_d = cnt_20.value_counts()[0]
    except:
        cnt_20_d = 0.8
    try:
        cnt_21_b = cnt_21.value_counts()[1]
    except:
        cnt_21_b = 0.8
    try:
        cnt_21_d = cnt_21.value_counts()[0]
    except:
        cnt_21_d = 0.8
    try:
        cnt_22_b = cnt_22.value_counts()[1]
    except:
        cnt_22_b = 0.8
    try:
        cnt_22_d = cnt_22.value_counts()[0]
    except:
        cnt_22_d = 0.8
    try:
        cnt_23_b = cnt_23.value_counts()[1]
    except:
        cnt_23_b = 0.8
    try:
        cnt_23_d = cnt_23.value_counts()[0]
    except:
        cnt_23_d = 0.8
    try:
        cnt_24_b = cnt_24.value_counts()[1]
    except:
        cnt_24_b = 0.8
    try:
        cnt_24_d = cnt_24.value_counts()[0]
    except:
        cnt_24_d = 0.8
    try:
        cnt_25_b = cnt_25.value_counts()[1]
    except:
        cnt_25_b = 0.8
    try:
        cnt_25_d = cnt_25.value_counts()[0]
    except:
        cnt_25_d = 0.8
    try:
        cnt_26_b = cnt_26.value_counts()[1]
    except:
        cnt_26_b = 0.8
    try:
        cnt_26_d = cnt_26.value_counts()[0]
    except:
        cnt_26_d = 0.8
    try:
        cnt_27_b = cnt_27.value_counts()[1]
    except:
        cnt_27_b = 0.8
    try:
        cnt_27_d = cnt_27.value_counts()[0]
    except:
        cnt_27_d = 0.8
    try:
        cnt_28_b = cnt_28.value_counts()[1]
    except:
        cnt_28_b = 0.8
    try:
        cnt_28_d = cnt_28.value_counts()[0]
    except:
        cnt_28_d = 0.8
    try:
        cnt_29_b = cnt_29.value_counts()[1]
    except:
        cnt_29_b = 0.8
    try:
        cnt_29_d = cnt_29.value_counts()[0]
    except:
        cnt_29_d = 0.8
    try:
        cnt_30_b = cnt_30.value_counts()[1]
    except:
        cnt_30_b = 0.8
    try:
        cnt_30_d = cnt_30.value_counts()[0]
    except:
        cnt_30_d = 0.8
    try:
        cnt_31_b = cnt_31.value_counts()[1]
    except:
        cnt_31_b = 0.8
    try:
        cnt_31_d = cnt_31.value_counts()[0]
    except:
        cnt_31_d = 0.8
    try:
        cnt_32_b = cnt_32.value_counts()[1]
    except:
        cnt_32_b = 0.8
    try:
        cnt_32_d = cnt_32.value_counts()[0]
    except:
        cnt_32_d = 0.8
    try:
        cnt_33_b = cnt_33.value_counts()[1]
    except:
        cnt_33_b = 0.8
    try:
        cnt_33_d = cnt_33.value_counts()[0]
    except:
        cnt_33_d = 0.8
    try:
        cnt_34_b = cnt_34.value_counts()[1]
    except:
        cnt_34_b = 0.8
    try:
        cnt_34_d = cnt_34.value_counts()[0]
    except:
        cnt_34_d = 0.8
    try:
        cnt_35_b = cnt_35.value_counts()[1]
    except:
        cnt_35_b = 0.8
    try:
        cnt_35_d = cnt_35.value_counts()[0]
    except:
        cnt_35_d = 0.8
    try:
        cnt_36_b = cnt_36.value_counts()[1]
    except:
        cnt_36_b = 0.8
    try:
        cnt_36_d = cnt_36.value_counts()[0]
    except:
        cnt_36_d = 0.8
    try:
        cnt_37_b = cnt_37.value_counts()[1]
    except:
        cnt_37_b = 0.8
    try:
        cnt_37_d = cnt_37.value_counts()[0]
    except:
        cnt_37_d = 0.8
    try:
        cnt_38_b = cnt_38.value_counts()[1]
    except:
        cnt_38_b = 0.8
    try:
        cnt_38_d = cnt_38.value_counts()[0]
    except:
        cnt_38_d = 0.8
    try:
        cnt_39_b = cnt_39.value_counts()[1]
    except:
        cnt_39_b = 0.8
    try:
        cnt_39_d = cnt_39.value_counts()[0]
    except:
        cnt_39_d = 0.8
    try:
        cnt_40_b = cnt_40.value_counts()[1]
    except:
        cnt_40_b = 0.8
    try:
        cnt_40_d = cnt_40.value_counts()[0]
    except:
        cnt_40_d = 0.8

    return cnt_1_b, cnt_1_d, cnt_2_b, cnt_2_d, cnt_3_b, cnt_3_d, cnt_4_b, cnt_4_d, cnt_5_b, cnt_5_d \
        , cnt_6_b, cnt_6_d, cnt_7_b, cnt_7_d, cnt_8_b, cnt_8_d, cnt_9_b, cnt_9_d, cnt_10_b, cnt_10_d \
        , cnt_11_b, cnt_11_d, cnt_12_b, cnt_12_d, cnt_13_b, cnt_13_d, cnt_14_b, cnt_14_d, cnt_15_b, cnt_15_d \
        , cnt_16_b, cnt_16_d, cnt_17_b, cnt_17_d, cnt_18_b, cnt_18_d, cnt_19_b, cnt_19_d, cnt_20_b, cnt_20_d \
        , cnt_21_b, cnt_21_d, cnt_22_b, cnt_22_d, cnt_23_b, cnt_23_d, cnt_24_b, cnt_24_d, cnt_25_b, cnt_25_d \
        , cnt_26_b, cnt_26_d, cnt_27_b, cnt_27_d, cnt_28_b, cnt_28_d, cnt_29_b, cnt_29_d, cnt_30_b, cnt_30_d \
        , cnt_31_b, cnt_31_d, cnt_32_b, cnt_32_d, cnt_33_b, cnt_33_d, cnt_34_b, cnt_34_d, cnt_35_b, cnt_35_d \
        , cnt_36_b, cnt_36_d, cnt_37_b, cnt_37_d, cnt_38_b, cnt_38_d, cnt_39_b, cnt_39_d, cnt_40_b, cnt_40_d \
        , num_0, num_1, num_2, num_3, step1, step2, step3, step4

def antiJoin(ratioData, originData, ratio):
    # data = final
    # originData = df
    data = ratioData[ratioData['dvsb'] >= ratio]
    conditionBool = None
    for j in range(1, data.shape[0]):
        col = data['condi'].iloc[j]
        val = data['value'].iloc[j]

        if j == 1:
            condition = col + " > " + str(val)
            conditionBool = originData[col] > float(val)
        else:
            condition = condition + " | " + col + " > " + str(val)
            conditionBool = conditionBool | (originData[col] > float(val))

    return originData[~conditionBool]

def innerJoin(ratioData, originData, ratio):
    # data = final
    # originData = df
    data = ratioData[ratioData['bvsd'] >= ratio]
    conditionBool = None
    for j in range(1, data.shape[0]):
        col = data['condi'].iloc[j]
        val = data['value'].iloc[j]

        if j == 1:
            condition = col + " > " + str(val)
            conditionBool = originData[col] > float(val)
        else:
            condition = condition + " | " + col + " > " + str(val)
            conditionBool = conditionBool | (originData[col] > float(val))

    return originData[conditionBool]

def if_float(string):
    try:
        float(string)
        return True
    except ValueError:
        return False

def conditionMake(data, aggr_df, lvl, initCondition, prevBcnt, prevDcnt, branch, name, limitLvl, lvlNum, lastLimitRatio, limitCnt):
    # data, aggr_df, lvl, initCondition, prevBcnt, prevDcnt, branch, name, limitLvl, lvlNum = tmp[3], tmp[2], i, tmp[4], prevBcnt, prevDcnt, branch, name, i, j
    tmpFinal = pd.DataFrame()

    exceptCon = pd.DataFrame()
    fResultT = pd.DataFrame()

    for u in range(branch * (lvlNum - 1) + 1, branch * lvlNum + 1):
        try:
            # u=1
            tmpExec = pd.DataFrame()
            conH = ''
            valH = ''
            calH = ''
            tmpBool = pd.DataFrame()
            tmpAggr = pd.DataFrame()
            tmpData = pd.DataFrame()

            if u % branch == 1:
                result = data.iloc[:, 5:data.shape[1]].apply(lambda x: custom_func(x, aggr_df), axis=0)
                # 각 조건 별 bcnt, dcnt, bvsd, dvsb
                tmpFinal = makeFinalDf(result)
                tmpFinal['prevBcnt'] = prevBcnt
                tmpFinal['prevDcnt'] = prevDcnt
                tmpFinal = tmpFinal.assign(rGr=lambda x: x.dvsb - x.prevDcnt / x.prevBcnt)
                tmpFinal = tmpFinal.assign(bDc=lambda x: x.prevBcnt - x.bcnt)
                tmpFinal = tmpFinal.assign(dDc=lambda x: x.prevDcnt - x.dcnt)
                tmpFinal = tmpFinal.assign(rRt=lambda x: (x.prevBcnt - x.bcnt) / (x.prevDcnt - x.dcnt))
                tmpFinal = tmpFinal.assign(
                    sRt=lambda x: (x.prevBcnt - x.bcnt) * ((x.prevBcnt - x.bcnt) / (x.prevDcnt - x.dcnt)))
                tmpFinal = tmpFinal.assign(
                    pRt=lambda x: (x.prevBcnt - x.bcnt) * ((x.prevBcnt - x.bcnt) / (x.prevDcnt - x.dcnt)))
                tmpFinal = tmpFinal.assign(qRt=lambda x: ((x.prevBcnt - x.bcnt) * 100 / x.prevBcnt) * (
                        (x.prevBcnt - x.bcnt) / (x.prevDcnt - x.dcnt)))
                tmpFinal = tmpFinal.assign(fRt=lambda x: (x.dvsb - x.prevDcnt / x.prevBcnt) / (x.prevDcnt / x.prevBcnt) * (
                        (x.prevBcnt - x.bcnt) / (x.prevDcnt - x.dcnt)))
                tmpFinal = tmpFinal.assign(rRat=lambda x: (x.prevBcnt - x.bcnt) / x.prevBcnt * 100)
                tmpFinal = tmpFinal.assign(rrr=lambda x: (x.dvsb - x.prevDcnt / x.prevBcnt) / (x.prevDcnt / x.prevBcnt))

                # dvsb가 가장 큰거
                tmpExec = tmpFinal[(tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt)
                                 ].sort_values('dvsb', ascending=False).iloc[0]

            elif u % branch == 2:
                # dvsb가 두번째로 좋은 것
                tmpExec = tmpFinal[(tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt)
                                 ].sort_values('dvsb', ascending=False).iloc[1]
            elif u % branch == 3:
                #이전 레벨 전체건수 20% 이상 dvsb가 가장 좋은 것
                tmpExec = tmpFinal[(tmpFinal['dcnt'] + tmpFinal['bcnt'] >= (prevBcnt + prevDcnt) * 0.2)
                                 & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt)
                                  ].sort_values('dvsb', ascending=False).iloc[0]
            elif u % branch == 4:
                # 이전 레벨 전체건수 20% 이상 rrr이 가장 좋은 것
                tmpExec = tmpFinal[(tmpFinal['dcnt'] + tmpFinal['bcnt'] >= (prevBcnt + prevDcnt) * 0.2)
                                 & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt)
                                  ].sort_values('rrr', ascending=False).iloc[0]
            elif u % branch == 5:
                # 이전 레벨 전체건수 20% 이상 bcnt가 가장 적은 것
                tmpExec = tmpFinal[(tmpFinal['dcnt'] + tmpFinal['bcnt'] >= (prevBcnt + prevDcnt) * 0.2)
                                 & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt)
                                  ].sort_values('bcnt').iloc[0]
            elif u % branch == 6:
                # 이전 레벨 dvsb 보다 크면서 bcnt가 가장 적은 것
                tmpExec = tmpFinal[(tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt)
                                  ].sort_values('bcnt').iloc[0]
            elif u % branch == 7:
                # 이전 레벨 dvsb 보다 크면서 dcnt가 가장 많은 것
                tmpExec = tmpFinal[(tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt)
                                  ].sort_values('dcnt', ascending=False).iloc[0]
            elif u % branch == 8:
                # 이전 레벨 dvsb 보다 크면서 dcnt가 두번째로 많은 것
                tmpExec = tmpFinal[(tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt)
                                  ].sort_values('dcnt', ascending=False).iloc[1]
            elif u % branch == 9:
                # 이전 레벨 전체건수 40% 이상 dvsb가 가장 좋은 것
                tmpExec = tmpFinal[(tmpFinal['dcnt'] + tmpFinal['bcnt'] >= (prevBcnt + prevDcnt) * 0.4)
                                & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt)
                                  ].sort_values('dvsb', ascending=False).iloc[0]
            elif u % branch == 10:
                # 이전 레벨 전체건수 40% 이상 dvsb가 두번째로 좋은 것
                tmpExec = tmpFinal[(tmpFinal['dcnt'] + tmpFinal['bcnt'] >= (prevBcnt + prevDcnt) * 0.4)
                                 & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt)
                                  ].sort_values('dcnt', ascending=False).iloc[1]
            elif u % branch == 11:
                # rRt상위 50% 중에서 dcnt가 가장 많은 것
                tmpExec = tmpFinal[(tmpFinal['rRt'] > tmpFinal['rRt'].median())
                                 & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt)
                                  ].sort_values('dcnt', ascending=False).iloc[0]
            elif u % branch == 12:
                # rRat > 20 인 것 중에 rRt가 가장 큰것
                tmpExec = tmpFinal[(tmpFinal['rRat'] >= 20)
                                 & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt)
                                  ].sort_values('rRt', ascending=False).iloc[0]
            elif u % branch == 13:
                # lvl <= 2 rRat >= 30
                if lvl <= 2:
                    tmpExec = tmpFinal[(tmpFinal['rRat'] >= 30)
                                     & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt)
                                      ].sort_values('sRt', ascending=False).iloc[0]
                else:
                    tmpExec = tmpFinal[(tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt)
                                      ].sort_values('dvsb', ascending=False).iloc[0]
            elif u % branch == 0:
                # lvl <= 2 rRat >= 30
                if lvl <= 2:
                    tmpExec = tmpFinal[(tmpFinal['rRat'] >= 30)
                                     & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt)
                                      ].sort_values('rRt', ascending=False).iloc[0]
                else:
                    tmpExec = tmpFinal[(tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt)
                                      ].sort_values('dvsb', ascending=False).iloc[0]

            if len(tmpExec) > 0:
                conH = tmpExec['condi']
                valH = float(tmpExec['value'])
                calH = tmpExec['cal']

                if if_float(valH):
                    if calH == 'GT':
                        tmpBool = data[conH] > valH
                    else:
                        tmpBool = data[conH] <= valH
                else:
                    if calH == 'GT':
                        tmpBool = data[conH] > data[valH]
                    else:
                        tmpBool = data[conH] <= data[valH]

                if len(initCondition) == 0:
                    if calH == 'GT':
                        condition = conH + ' > ' + str(valH)
                    else:
                        condition = conH + ' <= ' + str(valH)
                else:
                    if calH == 'GT':
                        condition = initCondition + ' AND ' + conH + ' > ' + str(valH)
                    else:
                        condition = initCondition + ' AND ' + conH + ' <= ' + str(valH)

                tmpAggr = aggr_df[tmpBool]
                tmpData = data[tmpBool]

                print("[" + datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
                      + '] lvl'
                      + str(lvl) + "_" + str(u)
                      + "\ndvsb : " + str(round(tmpExec['dvsb'], 2))
                      + " / bcnt : " + str(int(tmpExec['bcnt']))
                      + " / dcnt : " + str(int(tmpExec['dcnt']))
                      + " / condition : " + condition)

                # 선택된 컬럼 삭제
                try:
                    tmpFinal = tmpFinal[~(tmpFinal['condi'] == conH)]
                except:
                    pass

                if (lvl < limitLvl - 1) or (lvl == limitLvl - 1 and tmpExec['dvsb'] > lastLimitRatio and tmpExec['dcnt'] > limitCnt):
                    with open('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/'
                              + name
                              + '/lvl_{}_{}.pkl'.format(lvl, u)
                            , 'wb') as f:
                        pickle.dump([tmpFinal, tmpBool, tmpAggr, tmpData, condition], f)

                try:
                    fResultT = fResultT.append(
                        pd.DataFrame([('lvl_' + str(lvl) + '_' + str(u), condition, str(int(tmpExec['bcnt'])),
                                       str(int(tmpExec['dcnt'])), tmpExec['bcnt'] / tmpExec['dcnt'],
                                       tmpExec['dcnt'] / tmpExec['bcnt'])],
                                     columns=['lvl', 'condi', 'bcnt', 'dcnt', 'bvsd', 'dvsb']))
                except Exception as e:
                    pass
        except:
            fResultT = fResultT.append(
                pd.DataFrame([('lvl_' + str(lvl) + '_' + str(u), '', '0',
                               '0', '0', '0')],
                             columns=['lvl', 'condi', 'bcnt', 'dcnt', 'bvsd', 'dvsb']))
            pass

    return fResultT

def conditionMakeInit(data, aggr_df, mode, leafType, lvl, condition, prevBcnt, prevDcnt, ratDf, exceptCon):
    # data = tmp[3]
    # aggr_df = tmp[2]
    # mode = 'del'
    # leafType = 'A'
    # lvl = 1
    # condition = tmp[4]
    # ratDf = tmp[5]
    tmpFinal = pd.DataFrame()

    if exceptCon != '':
        data = data.drop(exceptCon, axis=1)

    if leafType == 'A' and len(data) > 0:
        result = data.iloc[:, 8:data.shape[1] - 2].apply(lambda x: custom_func(x, aggr_df), axis=0)
        # 각 조건 별 bcnt, dcnt, bvsd, dvsb
        tmpFinal = makeFinalDf(result)
        # for i in range(10, data.shape[1]):
        #     tmpFinal = tmpFinal.append(custom_func2(data.iloc[:, i], data.iloc[:, 4]))

        ratList = tmpFinal
    else:
        tmpFinal = ratDf
        ratList = ratDf

    try:
        if len(tmpFinal) > 0:
            conH = tmpFinal.iloc[0]['condi']
            valH = float(tmpFinal.iloc[0]['value'])
            calH = tmpFinal.iloc[0]['cal']

            if if_float(valH):
                if calH == 'GT':
                    tmpBool = data[conH] > valH
                else:
                    tmpBool = data[conH] <= valH
            else:
                if calH == 'GT':
                    tmpBool = data[conH] > data[valH]
                else:
                    tmpBool = data[conH] <= data[valH]

            if len(condition) == 0:
                if calH == 'GT':
                    condition = conH + ' > ' + str(valH)
                else:
                    condition = conH + ' <= ' + str(valH)
            else:
                if calH == 'GT':
                    condition = condition + ' AND ' + conH + ' > ' + str(valH)
                else:
                    condition = condition + ' AND ' + conH + ' <= ' + str(valH)

            # data[data['slow3D_fast5D'] <= -53]
            # if mode == 'del':
            #     aggr_df = aggr_df[~tmpBool]
            #     data = data[~tmpBool]
            # else:
            aggr_df = aggr_df[tmpBool]
            data = data[tmpBool]

            tmpRt = tmpFinal.iloc[0]

            # 선택된 컬럼 삭제
            try:
                for i in range(0, len(data.columns)):
                    if conH[0:4] in data.columns[i]:
                        data = data.drop(data.columns[i], axis=1)
                        ratList = ratList[~(ratList['condi'] == data.columns[i])]
            except Exception as e:
                pass
        else:
            tmpBool = pd.DataFrame()
            tmpRt = pd.DataFrame()
    except Exception as e:
        tmpBool = pd.DataFrame()
        tmpRt = pd.DataFrame()

    return data, aggr_df, tmpBool, tmpFinal, condition, ratList

def drawCondiTree(data, level, limitCntRatio, name, lastLimitRatio):
    # initCondition = tmpCondition
    # data, order = tmpData, w
    # level = 3
    # limitCntRatio = 0.5
    # exceptCon = conH

    branch = 14

    initBcnt = data.iloc[:, 4].value_counts()[1]
    initDcnt = data.iloc[:, 4].value_counts()[0]
    limitCnt = initDcnt * limitCntRatio * 0.01
    realFinal = pd.DataFrame()
    tmpFF = pd.DataFrame()
    print('limitCnt : ' + str(limitCnt))

    for i in range(0, level + 1):
        if i == 0:
            ## Save pickle
            with open('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_0_1.pkl', 'wb') as f:
                pickle.dump(['', '', data.iloc[:, 4], data, '', ''], f)
            del (data)
        else:
            leaf = branch ** (i - 1)
            for j in range(1, leaf + 1):
                tmpNxtRat = pd.DataFrame()
                tmpRatList = pd.DataFrame()
                tmpDat = pd.DataFrame()
                tmpAggrDf = pd.DataFrame()
                tmpBool = pd.DataFrame()
                tmpFinal = pd.DataFrame()
                tmpCondition = ''

                prevBcnt = 0.8
                prevDcnt = 0.8
                tmp = pd.DataFrame()

                try:
                    with open(
                            "C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/"
                            + name + "/lvl_{}_{}.pkl".format(i - 1, j), "rb") as fr:
                        tmp = pickle.load(fr)
                except Exception as e:
                    tmp = pd.DataFrame()
                    pass

                try:
                    prevDcnt = tmp[3].iloc[:, 4].value_counts()[0]

                    try:
                        prevBcnt = tmp[3].iloc[:, 4].value_counts()[1]
                    except Exception as e:
                        prevBcnt = 0.8
                        pass
                    
                    print("[" + datetime.datetime.today().strftime(
                        "%Y-%m-%d %H:%M:%S") + '] sourceData : lvl_' + str(i - 1) + '_' + str(j)
                        + " / dvsb : " + str(round(prevDcnt / prevBcnt, 2))
                        + " / bvsd : " + str(round(prevBcnt / prevDcnt, 2))
                        + " / prevBcnt : "
                        + str(prevBcnt) + " / prevDcnt : " + str(prevDcnt)
                        )
                    # print('i : ' + str(i) + ' / prevDcnt : ' + str(prevDcnt) + ' / limitCnt : ' + str(limitCnt))
                    if (i < level and prevDcnt > limitCnt) or (i == level and prevDcnt / prevBcnt > lastLimitRatio and prevDcnt > limitCnt):
                        # print('here!!!')
                        tmpFF = conditionMake(tmp[3], tmp[2], i, tmp[4], prevBcnt, prevDcnt, branch, name, level, j, lastLimitRatio, limitCnt)
                        realFinal = realFinal.append(tmpFF)

                    try:
                        if i > 0:
                            print('delete pickle - lvl_{}_{}.pkl'.format(i - 1, j))
                            if os.path.isfile(
                                    'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(i - 1, j)):
                                os.remove(
                                    'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(i - 1, j))
                    except Exception as e:
                        print(e)
                        pass

                except Exception as e:
                    prevDcnt = 0.8
                    pass

    return realFinal

def drawCondiInitTree(path, level, limitCntRatio, name, lastLimitRatio):
    data = pd.read_csv(path + name + "_com.csv")
    mode = 'del'
    mod = sys.modules[__name__]
    fResultT = pd.DataFrame()
    tmpBoolSumm = pd.DataFrame()
    finalReturn = pd.DataFrame()
    # data = data.iloc[1:1000,]
    createFolder("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name)
    removeAllFile("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name)

    tmpNxtRat = pd.DataFrame()
    tmpRatList = pd.DataFrame()
    tmpDat = pd.DataFrame()
    tmpAggrDf = pd.DataFrame()
    tmpBool = pd.DataFrame()
    tmpFinal = pd.DataFrame()
    tmpCondition = ''

    prevBcnt = 0.8
    prevDcnt = 0.8
    tmp = pd.DataFrame()
    ## Load pickle
    tmp = ['', '', data.iloc[:, 4], data, '', '']

    try:
        prevDcnt = tmp[3].iloc[:, 4].value_counts()[0]
    except Exception as e:
        prevDcnt = 0.8
        pass

    try:
        prevBcnt = tmp[3].iloc[:, 4].value_counts()[1]
    except Exception as e:
        prevBcnt = 0.8
        pass

    # print("[" + datetime.datetime.today().strftime(
    #     "%Y-%m-%d %H:%M:%S") + "] initCondition Create : / Bcnt : " + str(
    #     prevBcnt) + " / Dcnt : " + str(prevDcnt))

    finalReturn = finalReturn.append(drawCondiTree(data, level, limitCntRatio, name, lastLimitRatio))
    finalReturn.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULT/" + name + "_result.csv")

    return finalReturn

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

    return print(data.value_counts('pur_gubn5'))

def exceptColumn(data, expCondi):
    try:
        for i in range(0, expCondi.shape[0]):
            if expCondi.iloc[i].find('>') > 0:
                expCol = expCondi.iloc[i].split('>')[0].replace(' ', '')
            else:
                expCol = expCondi.iloc[i].split('<=')[0].replace(' ', '')

            data = data.drop(expCol, axis=1)
    except Exception as e:
        pass

    return data

def removeAllFile(directory):
    if os.path.exists(directory):
        for file in os.scandir(directory):
            os.remove(file.path)

def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print('Error: Creating directory. ' + directory)
        pass

def makeFinalSet(path, name):
    # 결과 합치기
    fResultMid = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULT/" + name + "_result.csv")

    tmpL = fResultMid['condi'].drop_duplicates()  # 중복조건 제거
    fResultMid = fResultMid.iloc[tmpL.index]
    fResultMid = fResultMid.sort_values('dvsb', ascending=False)  # dvsb가 좋은 순서로 정렬
    fResultMid = fResultMid.iloc[:, 1:fResultMid.shape[1]].drop_duplicates()

    os.remove("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULT/" + name + "_result.csv")

    fResultMid.to_csv(path + name + "_dTreeNew_result_변경후.csv")

    return fResultMid

if __name__ == '__main__':
    #####################################################################################################
    # 파라미터 세팅   초기 조건 만들지 안고 계속 20개 가지 치기
    #####################################################################################################
    name = 'sjtabuy'                               # 사용자 지정 명
    path = "C:/Users/Shine_anal/Desktop/inott/"  # 사용자 지정명 + _com.csv 파일이 존재하는 폴더 (분석할 csv파일)
    paramLevel = 4                               # 돌릴LEVEL수
    paramLimitRatio = 0.4                        # 초기데이터 중 dcnt대비 x% 이상의 가지만 돌리게 하는 조건 (%)
    paramLastRatio = 2.5                           # 마지막 레벨에서 dvsb가 x:1 이상인 것만 돌리게 하는 조건
    #####################################################################################################

    # function Exec
    fResult = drawCondiInitTree(path, paramLevel, paramLimitRatio, name, paramLastRatio)
    fResultMid = makeFinalSet(path, name)

    #####################################################################################################
    # 최종결과파일명   사용자 지정 이름_allnew_ddelTreeNew_result.csv
    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    print('최종결과파일은 ' + path + name + '_TreeNew_result_변경후.csv  입니다.')
    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    #####################################################################################################
