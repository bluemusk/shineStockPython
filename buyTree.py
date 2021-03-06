# coding=utf-8
import pandas as pd
import warnings
import sys
import datetime
import os
import pickle

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

        for i in range(0, 80):
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
                        pd.DataFrame([(val, round(conValue, 1), bcnt, dcnt, bcnt / dcnt, dcnt / bcnt, 'GT')],
                                     columns=['condi', 'value', 'bcnt', 'dcnt', 'bvsd', 'dvsb', 'cal']),
                        ignore_index=False)
                else:
                    finalSet = finalSet.append(
                        pd.DataFrame([(val, round(conValue, 1), bcnt, dcnt, bcnt / dcnt, dcnt / bcnt, 'LT')],
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
        cnt_1_b = 0
    try:
        cnt_1_d = cnt_1.value_counts()[0]
    except:
        cnt_1_d = 0
    try:
        cnt_2_b = cnt_2.value_counts()[1]
    except:
        cnt_2_b = 0
    try:
        cnt_2_d = cnt_2.value_counts()[0]
    except:
        cnt_2_d = 0
    try:
        cnt_3_b = cnt_3.value_counts()[1]
    except:
        cnt_3_b = 0
    try:
        cnt_3_d = cnt_3.value_counts()[0]
    except:
        cnt_3_d = 0
    try:
        cnt_4_b = cnt_4.value_counts()[1]
    except:
        cnt_4_b = 0
    try:
        cnt_4_d = cnt_4.value_counts()[0]
    except:
        cnt_4_d = 0
    try:
        cnt_5_b = cnt_5.value_counts()[1]
    except:
        cnt_5_b = 0
    try:
        cnt_5_d = cnt_5.value_counts()[0]
    except:
        cnt_5_d = 0
    try:
        cnt_6_b = cnt_6.value_counts()[1]
    except:
        cnt_6_b = 0
    try:
        cnt_6_d = cnt_6.value_counts()[0]
    except:
        cnt_6_d = 0
    try:
        cnt_7_b = cnt_7.value_counts()[1]
    except:
        cnt_7_b = 0
    try:
        cnt_7_d = cnt_7.value_counts()[0]
    except:
        cnt_7_d = 0
    try:
        cnt_8_b = cnt_8.value_counts()[1]
    except:
        cnt_8_b = 0
    try:
        cnt_8_d = cnt_8.value_counts()[0]
    except:
        cnt_8_d = 0
    try:
        cnt_9_b = cnt_9.value_counts()[1]
    except:
        cnt_9_b = 0
    try:
        cnt_9_d = cnt_9.value_counts()[0]
    except:
        cnt_9_d = 0
    try:
        cnt_10_b = cnt_10.value_counts()[1]
    except:
        cnt_10_b = 0
    try:
        cnt_10_d = cnt_10.value_counts()[0]
    except:
        cnt_10_d = 0
    try:
        cnt_11_b = cnt_11.value_counts()[1]
    except:
        cnt_11_b = 0
    try:
        cnt_11_d = cnt_11.value_counts()[0]
    except:
        cnt_11_d = 0
    try:
        cnt_12_b = cnt_12.value_counts()[1]
    except:
        cnt_12_b = 0
    try:
        cnt_12_d = cnt_12.value_counts()[0]
    except:
        cnt_12_d = 0
    try:
        cnt_13_b = cnt_13.value_counts()[1]
    except:
        cnt_13_b = 0
    try:
        cnt_13_d = cnt_13.value_counts()[0]
    except:
        cnt_13_d = 0
    try:
        cnt_14_b = cnt_14.value_counts()[1]
    except:
        cnt_14_b = 0
    try:
        cnt_14_d = cnt_14.value_counts()[0]
    except:
        cnt_14_d = 0
    try:
        cnt_15_b = cnt_15.value_counts()[1]
    except:
        cnt_15_b = 0
    try:
        cnt_15_d = cnt_15.value_counts()[0]
    except:
        cnt_15_d = 0
    try:
        cnt_16_b = cnt_16.value_counts()[1]
    except:
        cnt_16_b = 0
    try:
        cnt_16_d = cnt_16.value_counts()[0]
    except:
        cnt_16_d = 0
    try:
        cnt_17_b = cnt_17.value_counts()[1]
    except:
        cnt_17_b = 0
    try:
        cnt_17_d = cnt_17.value_counts()[0]
    except:
        cnt_17_d = 0
    try:
        cnt_18_b = cnt_18.value_counts()[1]
    except:
        cnt_18_b = 0
    try:
        cnt_18_d = cnt_18.value_counts()[0]
    except:
        cnt_18_d = 0
    try:
        cnt_19_b = cnt_19.value_counts()[1]
    except:
        cnt_19_b = 0
    try:
        cnt_19_d = cnt_19.value_counts()[0]
    except:
        cnt_19_d = 0
    try:
        cnt_20_b = cnt_20.value_counts()[1]
    except:
        cnt_20_b = 0
    try:
        cnt_20_d = cnt_20.value_counts()[0]
    except:
        cnt_20_d = 0
    try:
        cnt_21_b = cnt_21.value_counts()[1]
    except:
        cnt_21_b = 0
    try:
        cnt_21_d = cnt_21.value_counts()[0]
    except:
        cnt_21_d = 0
    try:
        cnt_22_b = cnt_22.value_counts()[1]
    except:
        cnt_22_b = 0
    try:
        cnt_22_d = cnt_22.value_counts()[0]
    except:
        cnt_22_d = 0
    try:
        cnt_23_b = cnt_23.value_counts()[1]
    except:
        cnt_23_b = 0
    try:
        cnt_23_d = cnt_23.value_counts()[0]
    except:
        cnt_23_d = 0
    try:
        cnt_24_b = cnt_24.value_counts()[1]
    except:
        cnt_24_b = 0
    try:
        cnt_24_d = cnt_24.value_counts()[0]
    except:
        cnt_24_d = 0
    try:
        cnt_25_b = cnt_25.value_counts()[1]
    except:
        cnt_25_b = 0
    try:
        cnt_25_d = cnt_25.value_counts()[0]
    except:
        cnt_25_d = 0
    try:
        cnt_26_b = cnt_26.value_counts()[1]
    except:
        cnt_26_b = 0
    try:
        cnt_26_d = cnt_26.value_counts()[0]
    except:
        cnt_26_d = 0
    try:
        cnt_27_b = cnt_27.value_counts()[1]
    except:
        cnt_27_b = 0
    try:
        cnt_27_d = cnt_27.value_counts()[0]
    except:
        cnt_27_d = 0
    try:
        cnt_28_b = cnt_28.value_counts()[1]
    except:
        cnt_28_b = 0
    try:
        cnt_28_d = cnt_28.value_counts()[0]
    except:
        cnt_28_d = 0
    try:
        cnt_29_b = cnt_29.value_counts()[1]
    except:
        cnt_29_b = 0
    try:
        cnt_29_d = cnt_29.value_counts()[0]
    except:
        cnt_29_d = 0
    try:
        cnt_30_b = cnt_30.value_counts()[1]
    except:
        cnt_30_b = 0
    try:
        cnt_30_d = cnt_30.value_counts()[0]
    except:
        cnt_30_d = 0
    try:
        cnt_31_b = cnt_31.value_counts()[1]
    except:
        cnt_31_b = 0
    try:
        cnt_31_d = cnt_31.value_counts()[0]
    except:
        cnt_31_d = 0
    try:
        cnt_32_b = cnt_32.value_counts()[1]
    except:
        cnt_32_b = 0
    try:
        cnt_32_d = cnt_32.value_counts()[0]
    except:
        cnt_32_d = 0
    try:
        cnt_33_b = cnt_33.value_counts()[1]
    except:
        cnt_33_b = 0
    try:
        cnt_33_d = cnt_33.value_counts()[0]
    except:
        cnt_33_d = 0
    try:
        cnt_34_b = cnt_34.value_counts()[1]
    except:
        cnt_34_b = 0
    try:
        cnt_34_d = cnt_34.value_counts()[0]
    except:
        cnt_34_d = 0
    try:
        cnt_35_b = cnt_35.value_counts()[1]
    except:
        cnt_35_b = 0
    try:
        cnt_35_d = cnt_35.value_counts()[0]
    except:
        cnt_35_d = 0
    try:
        cnt_36_b = cnt_36.value_counts()[1]
    except:
        cnt_36_b = 0
    try:
        cnt_36_d = cnt_36.value_counts()[0]
    except:
        cnt_36_d = 0
    try:
        cnt_37_b = cnt_37.value_counts()[1]
    except:
        cnt_37_b = 0
    try:
        cnt_37_d = cnt_37.value_counts()[0]
    except:
        cnt_37_d = 0
    try:
        cnt_38_b = cnt_38.value_counts()[1]
    except:
        cnt_38_b = 0
    try:
        cnt_38_d = cnt_38.value_counts()[0]
    except:
        cnt_38_d = 0
    try:
        cnt_39_b = cnt_39.value_counts()[1]
    except:
        cnt_39_b = 0
    try:
        cnt_39_d = cnt_39.value_counts()[0]
    except:
        cnt_39_d = 0
    try:
        cnt_40_b = cnt_40.value_counts()[1]
    except:
        cnt_40_b = 0
    try:
        cnt_40_d = cnt_40.value_counts()[0]
    except:
        cnt_40_d = 0

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

def conditionMake(data, aggr_df, mode, leafType, lvl, condition, prevBcnt, prevDcnt, ratDf, exceptCon):
    # data = tmp[3]
    # aggr_df = tmp[2]
    # mode = 'buy'
    # leafType = 'A'
    # lvl = 1
    # condition = tmp[4]
    # ratDf = tmp[5]

    if exceptCon != '':
        data = data.drop(exceptCon, axis=1)

    if leafType == 'A' and len(data) > 0:
        result = data.iloc[:, 6:data.shape[1]].apply(lambda x: custom_func(x, aggr_df), axis=0)
        # ??? ?????? ??? bcnt, dcnt, bvsd, dvsb
        tmpFinal = makeFinalDf(result)
        ratList = tmpFinal
    else:
        tmpFinal = ratDf
        ratList = ratDf

    try:
        if leafType == 'A':
            # ?????? ?????? bcnt??? 20% ??????????????? bvsd??? ?????? ?????? ???
            tmpFinal = tmpFinal[(tmpFinal['bcnt'] >= prevBcnt * 0.2)].sort_values('bvsd', ascending=False)
        elif leafType == 'B':
            # ?????? ?????? dcnt??? 70% ?????? & ??????????????? ??????????????? 20% ?????? ????????? bvsd??? ?????? ?????? ???
            tmpFinal = tmpFinal[(tmpFinal['dcnt'] < prevDcnt * 0.7) & (
                        tmpFinal['bcnt'] + tmpFinal['dcnt'] > (prevBcnt + prevDcnt) * 0.2)].sort_values('bvsd',
                                                                                                        ascending=False)
        elif leafType == 'C':
            if lvl == 1 or lvl == 2:
                # bvsd??? 0.6 ?????? 1.8 ???????????? ??????????????? dcnt?????? 70% ????????? ??? ??? dcnt??? ?????? ?????? ???
                tmpFinal = tmpFinal[((tmpFinal['bvsd'] >= 0.6) | (tmpFinal['bvsd'] <= 1.8)) & (
                            tmpFinal['dcnt'] > prevDcnt * 0.4)].sort_values('dcnt')
            else:
                # ?????? ?????? bcnt??? 60% ??????????????? bvsd??? ?????? ?????? ???
                tmpFinal = tmpFinal[(tmpFinal['bcnt'] >= prevBcnt * 0.6)].sort_values('bvsd', ascending=False)
        elif leafType == 'D':
            if lvl == 1 or lvl == 2 or lvl == 3:
                # ?????? ?????? bcnt??? 40% ??????????????? bvsd??? ?????? ?????? ???
                tmpFinal = tmpFinal[(tmpFinal['bcnt'] >= prevBcnt * 0.4)].sort_values('bvsd', ascending=False)
            else:
                # bvsd??? 0.6 ?????? 1.8 ???????????? ??????????????? ???????????? 20% ????????? ??? ??? dcnt??? ?????? ?????? ???
                tmpFinal = tmpFinal[((tmpFinal['bvsd'] >= 0.6) | (tmpFinal['bvsd'] <= 1.8)) & (
                            tmpFinal['bcnt'] + tmpFinal['dcnt'] > (prevBcnt + prevDcnt) * 0.2)].sort_values('dcnt')
        elif leafType == 'E':
            if lvl == 1 or lvl == 2 or lvl == 3:
                # bvsd??? 0.6 ?????? 1.8 ???????????? ??????????????? dcnt?????? 50% ????????? ??? ??? dcnt??? ?????? ?????? ???
                tmpFinal = tmpFinal[((tmpFinal['bvsd'] >= 0.6) | (tmpFinal['bvsd'] <= 1.8)) & (
                            tmpFinal['dcnt'] < prevDcnt * 0.5)].sort_values('dcnt', ascending=False)
            else:
                # ?????? ?????? bcnt??? 60% ??????????????? bvsd??? ?????? ?????? ???
                tmpFinal = tmpFinal[(tmpFinal['bcnt'] >= prevBcnt * 0.6)].sort_values('bvsd', ascending=False)
        else:
            tmpFinal = pd.DataFrame()

        if len(tmpFinal) > 0:
            conH = tmpFinal.iloc[0]['condi']
            valH = tmpFinal.iloc[0]['value']
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

            if lvl == 1:
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
            if mode == 'del':
                aggr_df = aggr_df[~tmpBool]
                data = data[~tmpBool]
            else:
                aggr_df = aggr_df[tmpBool]
                data = data[tmpBool]

            tmpRt = tmpFinal.iloc[0]

            # ????????? ?????? ??????
            data = data.drop(conH, axis=1)
            ratList = ratList[~(ratList['condi'] == conH)]
        else:
            tmpBool = pd.DataFrame()
            tmpRt = pd.DataFrame()
    except Exception as e:
        tmpBool = pd.DataFrame()
        tmpRt = pd.DataFrame()

    return data, aggr_df, tmpBool, tmpRt, condition, ratList

def drawCondiTree(path, mode, level, limitCntRatio):
    # data = df
    # mode = 'buy'
    # level = 3
    # limitCntRatio = 10
    # tmpRatList[(tmpRatList['dcnt'] != 0) & (tmpRatList['bcnt'] > 200000)].sort_values('bvsd', ascending=False).drop_duplicates()
    # initConditions = tmpRatList[(tmpRatList['bvsd'] > 0.7) & (tmpRatList['bvsd'] < 1.3) & (tmpRatList['bcnt'] < tmpRatList['bcnt'].mean())].sort_values('bcnt', ascending=False).drop_duplicates()

    initConditions = drawCondiInitTree(path)
    initConditions = initConditions[(initConditions['bvsd'] > 1.4) & (initConditions['bcnt'] < tmpRatList['bcnt'].mean())].sort_values('bvsd', ascending=False).drop_duplicates()
    #?????? ????????? 1.4?????? ?????? bcnt??? ?????? ??????????????? ?????? ????????? bvsd??? ?????? ????????????
    initConditions.iloc[1:10,]
    
    
    for x in range(0, 11):
        # ?????? ??????????????? lvl1???????????? ?????????.
        data = pd.read_csv(path)
        data = exceptColumn(data, initConditions.iloc[x,0])

        conH = initConditions.iloc[x,0]
        valH = initConditions.iloc[x,1]
        calH = initConditions.iloc[x,6]

        if if_float(valH):
            if calH == 'GT':
                data = data[data[conH] > valH]
            else:
                data = data[data[conH] <= valH]
        else:
            if calH == 'GT':
                data = data[data[conH] > data[valH]]
            else:
                data = data[data[conH] <= data[valH]]

        branch = 5

        initBcnt = data.iloc[:, 4].value_counts()[1]
        limitCnt = initBcnt * limitCntRatio * 0.01
        fResultT = pd.DataFrame()
        print('limitCnt : ' + str(limitCnt))

        # i,j=1,1
        for i in range(0, level + 1):
            leaf = branch ** i
            if i == 0:
                # mod = sys.modules[__name__]
                # setattr(mod, 'lvl_0_1', ['', '', data.iloc[:, 4], data, '', ''])
                ## Save pickle
                with open('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/lvl_0_1.pkl', 'wb') as f:
                    pickle.dump(['', '', data.iloc[:, 4], data, '', ''], f)
                del (data)
            else:
                for j in range(1, leaf + 1):
                    if j % branch == 1:
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
                        with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/lvl_{}_{}.pkl".format(i - 1,
                                                                                                            j // branch + 1),
                                "rb") as fr:
                            tmp = pickle.load(fr)
                        # tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // branch + 1))

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

                        print("[" + datetime.datetime.today().strftime(
                            "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j) + " / prevBcnt : " + str(
                            prevBcnt) + " / prevDcnt : " + str(
                            prevDcnt) + ' / sourceData : lvl_' + str(i - 1) + '_' + str(j // branch + 1))
                        if prevBcnt > limitCnt:
                            tmpDat, tmpAggrDf, tmpBool, tmpFinal, tmpCondition, tmpRatList = conditionMake(tmp[3], tmp[2],
                                                                                                        mode, 'A', i,
                                                                                                        tmp[4], prevBcnt,
                                                                                                        prevDcnt, tmp[5],
                                                                                                        '')
                            tmpNxtRat = tmpRatList
                    elif j % branch == 2:
                        # tmp2 = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                        with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/lvl_{}_{}.pkl".format(i, j - 1),
                                "rb") as fr:
                            tmp2 = pickle.load(fr)
                        if tmp2[0].shape[0] > 0:
                            if prevBcnt > limitCnt:
                                tmpDat, tmpAggrDf, tmpBool, tmpFinal, tmpCondition, tmpRatList = conditionMake(tmp[3],
                                                                                                            tmp[2], mode,
                                                                                                            'B', i,
                                                                                                            tmp[4],
                                                                                                            prevBcnt,
                                                                                                            prevDcnt,
                                                                                                            tmpNxtRat,
                                                                                                            tmp2[
                                                                                                                0].condi)
                    elif j % branch == 3:
                        # tmp2 = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                        with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/lvl_{}_{}.pkl".format(i, j - 1),
                                "rb") as fr:
                            tmp2 = pickle.load(fr)
                        if tmp2[0].shape[0] > 0:
                            if prevBcnt > limitCnt:
                                tmpDat, tmpAggrDf, tmpBool, tmpFinal, tmpCondition, tmpRatList = conditionMake(tmp[3],
                                                                                                            tmp[2], mode,
                                                                                                            'C', i,
                                                                                                            tmp[4],
                                                                                                            prevBcnt,
                                                                                                            prevDcnt,
                                                                                                            tmpNxtRat,
                                                                                                            tmp2[
                                                                                                                0].condi)
                    elif j % branch == 4:
                        # tmp2 = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                        with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/lvl_{}_{}.pkl".format(i, j - 1),
                                "rb") as fr:
                            tmp2 = pickle.load(fr)
                        if tmp2[0].shape[0] > 0:
                            if prevBcnt > limitCnt:
                                tmpDat, tmpAggrDf, tmpBool, tmpFinal, tmpCondition, tmpRatList = conditionMake(tmp[3],
                                                                                                            tmp[2], mode,
                                                                                                            'D', i,
                                                                                                            tmp[4],
                                                                                                            prevBcnt,
                                                                                                            prevDcnt,
                                                                                                            tmpNxtRat,
                                                                                                            tmp2[
                                                                                                                0].condi)
                    elif j % branch == 0:
                        # tmp2 = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                        with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/lvl_{}_{}.pkl".format(i, j - 1),
                                "rb") as fr:
                            tmp2 = pickle.load(fr)
                        if tmp2[0].shape[0] > 0:
                            if prevBcnt > limitCnt:
                                tmpDat, tmpAggrDf, tmpBool, tmpFinal, tmpCondition, tmpRatList = conditionMake(tmp[3],
                                                                                                            tmp[2], mode,
                                                                                                            'E', i,
                                                                                                            tmp[4],
                                                                                                            prevBcnt,
                                                                                                            prevDcnt,
                                                                                                            tmpNxtRat,
                                                                                                            tmp2[
                                                                                                                0].condi)

                    # setattr(mod, 'lvl_{}_{}'.format(i, j), [tmpFinal, tmpBool, tmpAggrDf, tmpDat, tmpCondition, tmpRatList])
                    with open('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/lvl_{}_{}.pkl'.format(i, j), 'wb') as f:
                        pickle.dump([tmpFinal, tmpBool, tmpAggrDf, tmpDat, tmpCondition, tmpRatList], f)

                    if j % branch != 1:
                        tmp2 = pd.DataFrame()

                    try:
                        bcnt = tmpFinal['bcnt']
                    except Exception as e:
                        bcnt = 0.8
                        pass

                    try:
                        dcnt = tmpFinal['dcnt']
                    except Exception as e:
                        dcnt = 0.8
                        pass

                    print("[" + datetime.datetime.today().strftime(
                        "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j) + " / bcnt : " + str(bcnt) + " / dcnt : " + str(dcnt) + " / condition : " +  tmpCondition  )

                    try:
                        fResultT = fResultT.append(
                            pd.DataFrame([('lvl_' + str(i) + '_' + str(j), tmpCondition, bcnt, dcnt, bcnt / dcnt)],
                                        columns=['lvl', 'condi', 'bcnt', 'dcnt', 'bvsd']))
                    except Exception as e:
                        pass

                    if i >= 2:  # ?????? 1,2,3??????????????? ????????? ?????? ????????? ????????????
                        leaf2 = branch ** (i - 2)
                        try:
                            for l in range(1, leaf2 + 1):
                                # delattr(mod, 'lvl_{}_{}'.format(i-2, l))
                                if os.path.isfile(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/lvl_{}_{}.pkl'.format(i - 2,
                                                                                                                j)):
                                    os.remove(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/lvl_{}_{}.pkl'.format(i - 2,
                                                                                                                j))
                        except Exception as e:
                            pass

                    if j >= branch and j % branch == 0:
                        try:
                            # delattr(mod, 'lvl_{}_{}'.format(i - 1, j // branch))
                            if os.path.isfile(
                                    'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/lvl_{}_{}.pkl'.format(i - 1,
                                                                                                            j // branch)):
                                os.remove('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/lvl_{}_{}.pkl'.format(i - 1,
                                                                                                                    j // branch))
                        except Exception as e:
                            print(e)
                            pass

                    if i == level:
                        try:
                            # delattr(mod, 'lvl_{}_{}'.format(i, j - branch + 1))
                            if os.path.isfile('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/lvl_{}_{}.pkl'.format(i,
                                                                                                                        j - branch + 1)):
                                os.remove('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/lvl_{}_{}.pkl'.format(i,
                                                                                                                    j - branch + 1))
                        except Exception as e:
                            pass

    return fResultT

def drawCondiInitTree(path):
    data = pd.read_csv(path)
    fResultT = pd.DataFrame()

    for i in range(0, 2):
        if i == 0:
            mod = sys.modules[__name__]
            setattr(mod, 'lvl_0_1', ['', '', data.iloc[:, 4], data, '', ''])
        else:
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
            tmp = getattr(mod, 'lvl_0_1')

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

            tmpDat, tmpAggrDf, tmpBool, tmpFinal, tmpCondition, tmpRatList = conditionMake(tmp[3], tmp[2],
                                                                                            'buy', 'A', 1,
                                                                                            tmp[4], prevBcnt,
                                                                                            prevDcnt, tmp[5],
                                                                                            '')
    return tmpRatList

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
    for i in range(0, expCondi.shape[0]):
        if expCondi.iloc[i].find('>') > 0:
            expCol = expCondi.iloc[i].split('>')[0].replace(' ', '')
        else:
            expCol = expCondi.iloc[i].split('<=')[0].replace(' ', '')

        data = data.drop(expCol, axis=1)

    return data



# Data Loading
# path = "C:/Users/Shine_anal/PycharmProjects/anlaysis/trainning.csv"
path = "C:/Users/Shine_anal/PycharmProjects/anlaysis/inodel02_close_h4_9year_202009.csv"
# df = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/target.csv")

# function Exec
# fResult = drawCondiTree(df.iloc[:,0:50], 'buy', 2, 0.4)
fResult = drawCondiTree(path, 'buy', 5, 0.2, pd.DataFrame())
# fResult??? lvl1 ????????? ???????????? ?????? ?????? ???
fResult2 = drawCondiTree(path, 'buy', 5, 0.2, fResult)

fResult = fResult.append(fResult2)

fResult = fResult.sort_values('bvsd', ascending=False)
fResult = fResult.iloc[:, 1:fResult.shape[1]].drop_duplicates()
fResult.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/buy_8year_20210915.csv")
