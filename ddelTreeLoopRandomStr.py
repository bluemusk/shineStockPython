# coding=utf-8
import pandas as pd
# import modin.pandas as pd
import warnings
import sys
import datetime
import os
import pickle
import ray
import numpy as np
# import line_profiler

# from modin.config import Engine

# Engine.put("ray")  # Modin will use Ray

warnings.filterwarnings('ignore')

@ray.remote
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

@ray.remote
def colValueCalc(col, aggr_df):
    # import pandas as pd
    # name = 'ncbuy2'  # ????????? ?????? ???
    # path = "C:/Users/Shine_anal/Desktop/inott/"  # ????????? ????????? + _com.csv ????????? ???????????? ?????? (????????? csv??????)
    # data = pd.read_csv(path + name + '_com.csv')
    # col = data.iloc[:,9:data.shape[1]]
    # aggr_df = data.iloc[:,4]
    tmpTechData = col.describe()
    rtnDf = pd.DataFrame()

    for i in range(0, col.shape[1]):
        # num_1 = round(tmpTechData[col.columns[i]]['25%'], 1)
        # num_2 = round(tmpTechData[col.columns[i]]['50%'], 1)
        # num_3 = round(tmpTechData[col.columns[i]]['75%'], 1)
        #
        # IQR = num_3 - num_1
        #
        # num_0 = round(tmpTechData[col.columns[i]]['25%'], 1) - IQR * 1.5
        # num_4 = round(tmpTechData[col.columns[i]]['75%'], 1) + IQR * 1.5
        #
        # step1 = round((num_0 + num_1) / 5, 1)
        # step2 = round((num_1 + num_2) / 5, 1)
        # step3 = round((num_2 + num_3) / 5, 1)
        # step4 = round((num_3 + num_4) / 5, 1)

        num_0 = round(col.iloc[:, i].quantile(0), 1)
        num_1 = round(col.iloc[:, i].quantile(0.25), 1)
        num_2 = round(col.iloc[:, i].quantile(0.5), 1)
        num_3 = round(col.iloc[:, i].quantile(0.75), 1)
        num_4 = round(col.iloc[:, i].quantile(1), 1)

        step1 = round((num_0 + num_1) / 5, 1)
        step2 = round((num_1 + num_2) / 5, 1)
        step3 = round((num_2 + num_3) / 5, 1)
        step4 = round((num_3 + num_4) / 5, 1)

        cnt_1 = aggr_df[(col.iloc[:, i] > num_0 + step1 * 0)]
        cnt_2 = aggr_df[(col.iloc[:, i] > num_0 + step1 * 1)]
        cnt_3 = aggr_df[(col.iloc[:, i] > num_0 + step1 * 2)]
        cnt_4 = aggr_df[(col.iloc[:, i] > num_0 + step1 * 3)]
        cnt_5 = aggr_df[(col.iloc[:, i] > num_0 + step1 * 4)]
        cnt_6 = aggr_df[(col.iloc[:, i] > num_1 + step2 * 0)]
        cnt_7 = aggr_df[(col.iloc[:, i] > num_1 + step2 * 1)]
        cnt_8 = aggr_df[(col.iloc[:, i] > num_1 + step2 * 2)]
        cnt_9 = aggr_df[(col.iloc[:, i] > num_1 + step2 * 3)]
        cnt_10 = aggr_df[(col.iloc[:, i] > num_1 + step2 * 4)]
        cnt_11 = aggr_df[(col.iloc[:, i] > num_2 + step3 * 0)]
        cnt_12 = aggr_df[(col.iloc[:, i] > num_2 + step3 * 1)]
        cnt_13 = aggr_df[(col.iloc[:, i] > num_2 + step3 * 2)]
        cnt_14 = aggr_df[(col.iloc[:, i] > num_2 + step3 * 3)]
        cnt_15 = aggr_df[(col.iloc[:, i] > num_2 + step3 * 4)]
        cnt_16 = aggr_df[(col.iloc[:, i] > num_3 + step4 * 0)]
        cnt_17 = aggr_df[(col.iloc[:, i] > num_3 + step4 * 1)]
        cnt_18 = aggr_df[(col.iloc[:, i] > num_3 + step4 * 2)]
        cnt_19 = aggr_df[(col.iloc[:, i] > num_3 + step4 * 3)]
        cnt_20 = aggr_df[(col.iloc[:, i] > num_3 + step4 * 4)]

        cnt_21 = aggr_df[(col.iloc[:, i] <= num_0 + step1 * 0)]
        cnt_22 = aggr_df[(col.iloc[:, i] <= num_0 + step1 * 1)]
        cnt_23 = aggr_df[(col.iloc[:, i] <= num_0 + step1 * 2)]
        cnt_24 = aggr_df[(col.iloc[:, i] <= num_0 + step1 * 3)]
        cnt_25 = aggr_df[(col.iloc[:, i] <= num_0 + step1 * 4)]
        cnt_26 = aggr_df[(col.iloc[:, i] <= num_1 + step2 * 0)]
        cnt_27 = aggr_df[(col.iloc[:, i] <= num_1 + step2 * 1)]
        cnt_28 = aggr_df[(col.iloc[:, i] <= num_1 + step2 * 2)]
        cnt_29 = aggr_df[(col.iloc[:, i] <= num_1 + step2 * 3)]
        cnt_30 = aggr_df[(col.iloc[:, i] <= num_1 + step2 * 4)]
        cnt_31 = aggr_df[(col.iloc[:, i] <= num_2 + step3 * 0)]
        cnt_32 = aggr_df[(col.iloc[:, i] <= num_2 + step3 * 1)]
        cnt_33 = aggr_df[(col.iloc[:, i] <= num_2 + step3 * 2)]
        cnt_34 = aggr_df[(col.iloc[:, i] <= num_2 + step3 * 3)]
        cnt_35 = aggr_df[(col.iloc[:, i] <= num_2 + step3 * 4)]
        cnt_36 = aggr_df[(col.iloc[:, i] <= num_3 + step4 * 0)]
        cnt_37 = aggr_df[(col.iloc[:, i] <= num_3 + step4 * 1)]
        cnt_38 = aggr_df[(col.iloc[:, i] <= num_3 + step4 * 2)]
        cnt_39 = aggr_df[(col.iloc[:, i] <= num_3 + step4 * 3)]
        cnt_40 = aggr_df[(col.iloc[:, i] <= num_3 + step4 * 4)]

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

        rtnDf = pd.concat([rtnDf, pd.DataFrame(
            [cnt_1_b, cnt_1_d, cnt_2_b, cnt_2_d, cnt_3_b, cnt_3_d, cnt_4_b, cnt_4_d, cnt_5_b, cnt_5_d \
                , cnt_6_b, cnt_6_d, cnt_7_b, cnt_7_d, cnt_8_b, cnt_8_d, cnt_9_b, cnt_9_d, cnt_10_b, cnt_10_d \
                , cnt_11_b, cnt_11_d, cnt_12_b, cnt_12_d, cnt_13_b, cnt_13_d, cnt_14_b, cnt_14_d, cnt_15_b, cnt_15_d \
                , cnt_16_b, cnt_16_d, cnt_17_b, cnt_17_d, cnt_18_b, cnt_18_d, cnt_19_b, cnt_19_d, cnt_20_b, cnt_20_d \
                , cnt_21_b, cnt_21_d, cnt_22_b, cnt_22_d, cnt_23_b, cnt_23_d, cnt_24_b, cnt_24_d, cnt_25_b, cnt_25_d \
                , cnt_26_b, cnt_26_d, cnt_27_b, cnt_27_d, cnt_28_b, cnt_28_d, cnt_29_b, cnt_29_d, cnt_30_b, cnt_30_d \
                , cnt_31_b, cnt_31_d, cnt_32_b, cnt_32_d, cnt_33_b, cnt_33_d, cnt_34_b, cnt_34_d, cnt_35_b, cnt_35_d \
                , cnt_36_b, cnt_36_d, cnt_37_b, cnt_37_d, cnt_38_b, cnt_38_d, cnt_39_b, cnt_39_d, cnt_40_b, cnt_40_d \
                , num_0, num_1, num_2, num_3, step1, step2, step3, step4], columns=[col.columns[i]])], axis=1)

    return rtnDf

def entropy(target_col):
    elements, counts = np.unique(target_col, return_counts = True)
    entropy = -np.sum([(counts[i]/np.sum(counts))*np.log2(counts[i]/np.sum(counts)) for i in range(len(elements))])
    return entropy

def splitData(data, aggr_df, prevBcnt, prevDcnt, strs):
    # strs = initStr
    tmpFinal = pd.DataFrame()

    # result 20 split
    rslt_split = np.array_split(strs, 20, axis=0)
    tmpRslt = [checkConditionParallel.remote(data, y) for y in rslt_split]

    # data[data['rsi_sig6_ang3_1bd'] > 0].value_counts('pur_gubn5')

    tmpFinal = pd.concat(
        [ray.get(tmpRslt)[0], ray.get(tmpRslt)[1], ray.get(tmpRslt)[2], ray.get(tmpRslt)[3],
         ray.get(tmpRslt)[4],
         ray.get(tmpRslt)[5]
            , ray.get(tmpRslt)[6], ray.get(tmpRslt)[7], ray.get(tmpRslt)[8], ray.get(tmpRslt)[9],
         ray.get(tmpRslt)[10], ray.get(tmpRslt)[11]
            , ray.get(tmpRslt)[12], ray.get(tmpRslt)[13], ray.get(tmpRslt)[14], ray.get(tmpRslt)[15],
         ray.get(tmpRslt)[16], ray.get(tmpRslt)[17]
            , ray.get(tmpRslt)[18], ray.get(tmpRslt)[19]], axis=0)



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
    tmpFinal = tmpFinal.assign(
        fRt=lambda x: (x.dvsb - x.prevDcnt / x.prevBcnt) / (x.prevDcnt / x.prevBcnt) * (
                (x.prevBcnt - x.bcnt) / (x.prevDcnt - x.dcnt)))
    tmpFinal = tmpFinal.assign(rRat=lambda x: (x.prevBcnt - x.bcnt) / x.prevBcnt * 100)
    tmpFinal = tmpFinal.assign(rrr=lambda x: (x.dvsb - x.prevDcnt / x.prevBcnt) / (x.prevDcnt / x.prevBcnt))

    tmpFinal = tmpFinal[~(tmpFinal['condi'] == 'X')]

    return tmpFinal

#@profile
def conditionMake(data, aggr_df, lvl, initCondition, prevBcnt, prevDcnt, branch, name, limitLvl, lvlNum, lastLimitRatio,
                  limitCnt, lastYn, strs):
    # data, aggr_df, lvl, initCondition, prevBcnt, prevDcnt, branch, name, limitLvl, lvlNum = tmp[3], tmp[2], i, tmp[4], prevBcnt, prevDcnt, 11, name, i, j
    tmpFinal = pd.DataFrame()

    exceptCon = pd.DataFrame()
    fResultT = pd.DataFrame()

    # initCondition = 'momp_ang3 > -4.7 AND formula_arc_day5 > 0.0 AND dis60_ang3 > -4.8 AND momp_sig_ang1_momp5_sig4_ang1 > 4.8 AND roc_arc1 <= -82.5 AND m14_slow3k_ang1_m14_slow3k_ang2 > 0.1'
    if len(initCondition) > 0:
        exCon = pd.DataFrame()
        conds = initCondition.split(' AND ')
        for x in conds:
            tmp = ''
            if x.find('>') > 0:
                try:
                    tmp = x.split(' > ')[0]
                except:
                    pass

            else:
                try:
                    tmp = x.split(' <= ')[0]
                except:
                    pass

            tmp = tmp.replace(' ', '')

            # print(tmp)
            try:
                data = data.drop(tmp, axis=1)
            except:
                pass

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
                tmpFinal = splitData(data, aggr_df, prevBcnt, prevDcnt, strs)

                firstBcnt = 0
                firstDcnt = 0

                # ?????? ?????? ???????????? 20% ?????? dvsb??? ?????? ?????? ???
                tmpExec = tmpFinal[(tmpFinal['dcnt'] + tmpFinal['bcnt'] >= (prevBcnt + prevDcnt) * 0.2)
                                   & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt * 1.2)
                                   ].sort_values('dvsb', ascending=False).iloc[0]

                firstBcnt = tmpExec.bcnt
                firstDcnt = tmpExec.dcnt
                firstCnt = 0
                print('firstDcnt : ' + str(firstDcnt) + ' / firstBcnt : ' + str(firstBcnt))
            elif u % branch == 2:
                # lvl <= 2 ?????? dcnt??? 40%?????? ?????? ?????? ??? dvsb??? ?????? ??????
                # ????????? ??????????????? dvsb??? ?????? ??????
                if lvl <= 2:
                    tmpExec = tmpFinal[(tmpFinal['dcnt'] <= prevDcnt * 0.6)
                                       & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt * 1.2)
                                       ].sort_values('dcnt', ascending=False).iloc[0]
                else:
                    tmpExec = tmpFinal[(tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt * 1.2)
                    ].sort_values('dvsb', ascending=False).iloc[0]
            elif u % branch == 3:
                # ?????? ?????? ???????????? 20% ?????? rrr??? ?????? ?????? ???
                tmpExec = tmpFinal[(tmpFinal['dcnt'] + tmpFinal['bcnt'] >= (prevBcnt + prevDcnt) * 0.2)
                                   & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt * 1.2)
                                   ].sort_values('rrr', ascending=False).iloc[0]
            elif u % branch == 4:
                # ?????? ?????? dvsb ?????? ????????? bcnt??? ?????? ?????? ???
                #  ( lvl <= 2 ?????? ???????????? 30%?????? ?????? ?????????)
                if lvl < 2:
                    tmpExec = tmpFinal[(tmpFinal['dcnt'] + tmpFinal['bcnt'] >= (prevBcnt + prevDcnt) * 0.3)
                                      & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt * 1.2)
                                       ].sort_values('bcnt').iloc[0]
                else:
                    tmpExec = tmpFinal[(tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt * 1.2)
                    ].sort_values('bcnt').iloc[0]
            elif u % branch == 5:
                # ?????? ?????? bcnt 40%?????? ??????, ?????? dvsb?????? ??????, dcnt??? ?????? ?????? ???
                tmpExec = tmpFinal[(tmpFinal['bcnt'] <= prevBcnt * 0.6)
                                   & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt * 1.2)
                                   ].sort_values('dcnt', ascending=False).iloc[0]
            elif u % branch == 6:
                # bcnt??? ?????? ?????? ???????????? 25% ?????? dcnt??? ?????? ?????? ???
                tmpExec = tmpFinal[(tmpFinal['bcnt'] <= (prevBcnt + prevDcnt) * 0.25)
                                   & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt * 1.2)
                                   ].sort_values('dcnt', ascending=False).iloc[0]
            elif u % branch == 7:
                # ?????? ?????? ???????????? 30% ??????, dvsb??? ?????? ?????? ???
                tmpExec = tmpFinal[(tmpFinal['dcnt'] + tmpFinal['bcnt'] > (prevBcnt + prevDcnt) * 0.3)
                                   & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt * 1.2)
                                   ].sort_values('dvsb', ascending=False).iloc[0]
            elif u % branch == 8:
                # rRt?????? 60% ????????? dcnt??? ?????? ?????? ???
                tmpExec = tmpFinal[(tmpFinal['rRt'] > tmpFinal['rRt'].median())
                                   & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt * 1.2)
                                   ].sort_values('dcnt', ascending=False).iloc[0]
            elif u % branch == 9:
                # rRt??? ???????????? dvsb?????? ?????? sRt??? ?????? ??????
                # ????????? rRat > 20 ??? ??? ?????? rRt??? ?????? ??????
                tmpExec = tmpFinal[(tmpFinal['rRat'] >= 20)
                                   & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt * 1.2)
                                   ].sort_values('rRt', ascending=False).iloc[0]
            elif u % branch == 10:
                # lvl <= 2 rRat >= 30
                # ????????? sRt??? ?????? ??????
                if lvl <= 2:
                    tmpExec = tmpFinal[(tmpFinal['rRat'] >= 30)
                                       & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt * 1.2)
                                       ].sort_values('dcnt', ascending=False).iloc[0]
                else:
                    tmpExec = tmpFinal[(tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt * 1.2)
                                       ].sort_values('sRt', ascending=False).iloc[0]
            elif u % branch == 0:
                # lvl <= 2 rRat >= 30
                # ????????? rRt??? ?????? ??????
                if lvl <= 2:
                    tmpExec = tmpFinal[(tmpFinal['rRat'] >= 30)
                                       & (tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt * 1.2)
                                       ].sort_values('rRt', ascending=False).iloc[0]
                else:
                    tmpExec = tmpFinal[(tmpFinal['dcnt'] / tmpFinal['bcnt'] > prevDcnt / prevBcnt * 1.2)
                                       ].sort_values('rRt', ascending=False).iloc[0]

            # original - 14:3  / 1 - 12:0  / etc - 14:1
            if lastYn == 'Y':
                if (firstDcnt / firstBcnt <= tmpExec.dvsb):
                    pass
                elif tmpExec.dvsb > prevDcnt / prevBcnt and tmpExec.dcnt > firstDcnt and firstCnt == 0:
                    firstCnt = firstCnt + 1
                    pass
                else:
                    tmpExec = pd.DataFrame()

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

                tmpEntr = entropy(tmpAggr)

                print("[" + datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
                      + '] lvl'
                      + str(lvl) + "_" + str(u)
                      + "\ndvsb : " + str(round(tmpExec['dvsb'], 2))
                      + " / bcnt : " + str(int(tmpExec['bcnt']))
                      + " / dcnt : " + str(int(tmpExec['dcnt']))
                      + " / entropy : " + str(round(tmpEntr,3))
                      + " / condition : " + condition)

                # ????????? ?????? ??????
                try:
                    tmpFinal = tmpFinal[~(tmpFinal['condi'] == conH)]
                except:
                    pass

                # if (lvl < limitLvl - 1 and tmpExec['dcnt'] > limitCnt) or (
                #         lvl == limitLvl - 1 and tmpExec['dcnt'] > 10
                #         # and (tmpEntr >= 0.72 or tmpEntr <= 0.28) and tmpExec['dvsb'] > 0.3
                #    ):
                print('lvl_{}_{}.pkl'.format(lvl, u) + ' pickle file create')
                with open('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/'
                          + name + 'ReNew'
                          + '/lvl_{}_{}.pkl'.format(lvl, u)
                        , 'wb') as f:
                    pickle.dump([tmpFinal, tmpBool, tmpAggr, tmpData, condition, tmpEntr], f)

                try:
                    fResultT = fResultT.append(
                        pd.DataFrame([('lvl_' + str(lvl) + '_' + str(u), condition, str(int(tmpExec['bcnt'])),
                                       str(int(tmpExec['dcnt'])), tmpExec['bcnt'] / tmpExec['dcnt'],
                                       tmpExec['dcnt'] / tmpExec['bcnt'], tmpEntr)],
                                     columns=['lvl', 'condi', 'bcnt', 'dcnt', 'bvsd', 'dvsb', 'entr']))

                    # if u % branch == 0:
                    #     for p in range(0, tmpExec.shape[0]):
                    #         if tmpExec.iloc[p]['dvsb'] > lastLimitRatio + 2:
                    #             fResultT = fResultT.append(pd.DataFrame([('lvl_' + str(lvl) + '_' + str(u) + 'extra_' + str(p), condition, str(int(tmpExec.iloc[p]['bcnt'])),
                    #                            str(int(tmpExec.iloc[p]['dcnt'])), tmpExec.iloc[p]['bcnt'] / tmpExec.iloc[p]['dcnt'],
                    #                            tmpExec.iloc[p]['dcnt'] / tmpExec.iloc[p]['bcnt'], tmpEntr)],
                    #                          columns=['lvl', 'condi', 'bcnt', 'dcnt', 'bvsd', 'dvsb', 'entr']))


                except Exception as e:
                    pass
        except Exception as e:
            # print(e)
            fResultT = fResultT.append(
                pd.DataFrame([('lvl_' + str(lvl) + '_' + str(u), '', '0',
                               '0', '0', '0', '0')],
                             columns=['lvl', 'condi', 'bcnt', 'dcnt', 'bvsd', 'dvsb', 'entr']))
            pass

    return fResultT

# def checkCondition(data, condition):
#     # condition = strs.iloc[0]
#     try:
#         condition = pd.DataFrame(condition.split(' AND '))
#     except:
#         pass
#
#     for i in range(0, condition.shape[0]):
#         if condition.iloc[i, 0].find('<=') > 0:
#             con = condition.iloc[i, 0].split('<=')[0]
#             con = str(con).replace(' ', '')
#
#             try:
#                 val = float(condition.iloc[i, 0].split('<=')[1])
#                 data = data[data[con] <= val]
#             except Exception as e:
#                 val = condition.iloc[i, 0].split('<=')[1]
#                 val = str(val).replace(' ', '')
#                 data = data[data[con] <= data[val]]
#                 pass
#
#         else:
#             con = condition.iloc[i, 0].split('>')[0]
#             con = str(con).replace(' ', '')
#
#             try:
#                 val = float(condition.iloc[i, 0].split('>')[1])
#                 data = data[data[con] > val]
#             except Exception as e:
#                 val = condition.iloc[i, 0].split('>')[1]
#                 val = str(val).replace(' ', '')
#                 data = data[data[con] > data[val]]
#                 pass
#
#     return print(data.value_counts('pur_gubn5'))

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

@ray.remote
def chkEndBranch(condi, data):
    chkEnd = 0
    for h in range(0, data.shape[0]):
        try:
            if condi in data.iloc[h]['condi']:
                chkEnd = chkEnd + 1
        except Exception as e:
            print(e)
            pass

    return chkEnd

def makeFinalSet(path, name, lastRatio, realInitData):
    # ?????? ?????????
    lists = os.listdir("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTRENEW/")

    file_list_rslt = [file for file in lists if file.endswith("_result.csv") and file.startswith(name)]

    # ?????? ?????????
    fResultMid = pd.DataFrame()
    for i in range(0, len(file_list_rslt)):
        print("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTRENEW/" + file_list_rslt[i])
        tmp = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTRENEW/" + file_list_rslt[i])
        fResultMid = fResultMid.append(tmp)


    tmpL = fResultMid['condi'].drop_duplicates()  # ???????????? ??????
    fResultMid = fResultMid.iloc[tmpL.index]
    fResultMid = fResultMid.iloc[:, 2:fResultMid.shape[1]].drop_duplicates()

    fResultMid = fResultMid.sort_values('condi')  # dvsb??? ?????? ????????? ??????
    fResultMid = fResultMid.dropna(axis=0)
    fResultMid['chk'] = 0

    # fResultMid = fResultMid[1:100]
    for g in range(0, fResultMid.shape[0] - 1):
        try:
            print(str(g) + ' - ' + fResultMid.iloc[g]['condi'])
        except:
            pass

        try:
            fResultMid_split = np.array_split(fResultMid.iloc[g+1:], 20, axis=0)
        except:
            print('split error')
            pass

        tmp = [chkEndBranch.remote(fResultMid.iloc[g]['condi'], x) for x in fResultMid_split]

        last = ray.get(tmp)[0] + ray.get(tmp)[1] + ray.get(tmp)[2] + ray.get(tmp)[3] + ray.get(tmp)[4] + ray.get(tmp)[5]\
               + ray.get(tmp)[6] + ray.get(tmp)[7] + ray.get(tmp)[8] + ray.get(tmp)[9] + ray.get(tmp)[10] + ray.get(tmp)[11]\
               + ray.get(tmp)[12] + ray.get(tmp)[13] + ray.get(tmp)[14] + ray.get(tmp)[15] + ray.get(tmp)[16] + ray.get(tmp)[17]\
               + ray.get(tmp)[18] + ray.get(tmp)[19]

        if last > 0:
            fResultMid['chk'].iloc[g] = 1

    #?????? 100%???????????? ????????? ????????? ?????? ?????????.
    for m in range(0, len(fResultMid)):
        lvindex, lvdcnt, lvbcnt, lvdvsb, lvData = checkCondition(realInitData, fResultMid.iloc[m]['condi'])
        fResultMid['dcnt'].iloc[m] = lvdcnt
        fResultMid['bcnt'].iloc[m] = lvbcnt
        fResultMid['dvsb'].iloc[m] = lvdvsb


    fResultFin = fResultMid[fResultMid['chk'] == 0]
    fResultFin = fResultFin[fResultFin['dvsb'] >= lastRatio]
    fResultFin = fResultFin.sort_values('dvsb', ascending=False)  # dvsb??? ?????? ????????? ??????
    fResultFin.to_csv(path + name + "_ddelTreeLoopRandom_result_{}.csv".format(datetime.datetime.today().strftime(
                        "%Y%m%d%H%M%S")))

    # os.remove("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTRENEW/" + name + "_result.csv")

    return fResultFin

def checkCondition(ckData, ckCond):
    # ckData, ckCond = lvl4Data, realFinal.iloc[0]['condi']
    try:
        ckCond = pd.DataFrame(ckCond.split(' AND '))

        for i in range(0, ckCond.shape[0]):
            if ckCond.iloc[i, 0].find('<=') > 0:
                con = ckCond.iloc[i, 0].split('<=')[0]
                con = str(con).replace(' ', '')

                try:
                    val = float(ckCond.iloc[i, 0].split('<=')[1])
                    ckData = ckData[ckData[con] <= val]
                except Exception as e:
                    val = ckCond.iloc[i, 0].split('<=')[1]
                    val = str(val).replace(' ', '')
                    ckData = ckData[ckData[con] <= ckData[val]]
                    pass

            else:
                con = ckCond.iloc[i, 0].split('>')[0]
                con = str(con).replace(' ', '')

                try:
                    val = float(ckCond.iloc[i, 0].split('>')[1])
                    ckData = ckData[ckData[con] > val]
                except Exception as e:
                    val = ckCond.iloc[i, 0].split('>')[1]
                    val = str(val).replace(' ', '')
                    ckData = ckData[ckData[con] > ckData[val]]
                    pass
    except:
        ckData = pd.DataFrame()
        pass

    if len(ckData) > 0:
        try:
            dcnt = ckData.value_counts('pur_gubn5')[0]
        except:
            dcnt = 0.8

        try:
            bcnt = ckData.value_counts('pur_gubn5')[1]
        except:
            bcnt = 0.8
    else:
        dcnt = 0.8
        bcnt = 0.8

    return ckData.index, dcnt, bcnt, dcnt / bcnt, ckData

@ray.remote
def checkConditionParallel(ckData, ckCond):
    # ckData, ckCond = initData, rslt_split[0]
    try:
        finalSet = pd.DataFrame()
        tmpData = pd.DataFrame()

        for i in range(0, ckCond.shape[0]):
            if ckCond.iloc[i, 0].find('<=') > 0:
                con = ckCond.iloc[i, 0].split('<=')[0]
                con = str(con).replace(' ', '')
                cal = 'LT'

                try:
                    val = float(ckCond.iloc[i, 0].split('<=')[1])
                    tmpData = ckData[ckData[con] <= val]
                except Exception as e:
                    val = ckCond.iloc[i, 0].split('<=')[1]
                    val = str(val).replace(' ', '')
                    tmpData = ckData[ckData[con] <= ckData[val]]
                    pass

            else:
                con = ckCond.iloc[i, 0].split('>')[0]
                con = str(con).replace(' ', '')
                cal = 'GT'
                try:
                    if len(ckData[con]) > 0:
                        try:
                            val = float(ckCond.iloc[i, 0].split('>')[1])
                            tmpData = ckData[ckData[con] > val]
                        except Exception as e:
                            val = ckCond.iloc[i, 0].split('>')[1]
                            val = str(val).replace(' ', '')
                            tmpData = ckData[ckData[con] > ckData[val]]
                            pass
                except:
                    tmpData = pd.DataFrame()
                    con = 'X'
                    val = 0
                    cal = 'NO'
                    pass


            if len(tmpData) > 0:
                try:
                    dcnt = tmpData.value_counts('pur_gubn5')[0]
                except:
                    dcnt = 0.8

                try:
                    bcnt = tmpData.value_counts('pur_gubn5')[1]
                except:
                    bcnt = 0.8
            else:
                dcnt = 0.8
                bcnt = 0.8

            finalSet = finalSet.append(
                pd.DataFrame([(con, val, bcnt, dcnt, bcnt / dcnt, dcnt / bcnt, cal)],
                             columns=['condi', 'value', 'bcnt', 'dcnt', 'bvsd', 'dvsb', 'cal']),
                ignore_index=False)
    except:
        finalSet = finalSet.append(
            pd.DataFrame([('X', 0, 0, 0, 0, 0, 'NO')],
                         columns=['condi', 'value', 'bcnt', 'dcnt', 'bvsd', 'dvsb', 'cal']),
            ignore_index=False)
        pass

    return finalSet

def dropCol(ckData, ckCond):
    ckCond = pd.DataFrame(ckCond.split(' AND '))

    for i in range(0, ckCond.shape[0]):
        if ckCond.iloc[i, 0].find('<=') > 0:
            con = ckCond.iloc[i, 0].split('<=')[0]
            con = str(con).replace(' ', '')

            try:
                ckData = ckData.drop(con, axis=1)
            except Exception as e:
                pass

        else:
            con = ckCond.iloc[i, 0].split('>')[0]
            con = str(con).replace(' ', '')

            try:
                ckData = ckData.drop(con, axis=1)
            except Exception as e:
                pass

    return ckData

def makeLevel(vLoop, paramLevel, paramLastRatio, limitCnt, name, branch, data, initCond, lastYn, strs):
    # vLoop, paramLevel, paramLastRatio, limitCnt, name, branch, data, initCond, lastYn = vLoop, paramLevel, paramLastRatio, limitCnt, name, branch, data, '', 'N'
    # vLoop, paramLevel, paramLastRatio, limitCnt, name, branch, data, initCond = vLoop, 2, paramLastRatio, limitCnt, name, branch, checkCondition(data, lvl4Conds.iloc[u].condi)[4], lvl4Conds.iloc[u].condi
    tmpMkL = pd.DataFrame()
    # i,j=1,1
    for i in range(0, paramLevel + 1):
        if i == 0:
            ## Save pickle
            with open('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + 'ReNew' + '/lvl_0_1.pkl',
                      'wb') as f:
                pickle.dump(['', '', data['pur_gubn5'], data, initCond, '', entropy(data['pur_gubn5'])], f)
        else:
            leaf = branch ** (i - 1)
            for j in range(1, leaf + 1):
                tmpNxtRat = pd.DataFrame()
                tmpRatList = pd.DataFrame()
                tmpDat = pd.DataFrame()
                tmpAggrDf = pd.DataFrame()
                tmpBool = pd.DataFrame()
                tmpCondition = ''

                prevBcnt = 0.8
                prevDcnt = 0.8
                tmp = pd.DataFrame()

                try:
                    with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/"
                              + name + 'ReNew' + "/lvl_{}_{}.pkl".format(i - 1, j), "rb") as fr:
                        tmp = pickle.load(fr)

                        # tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j))
                except Exception as e:
                    tmp = pd.DataFrame()
                    # print(e)
                    pass

                try:
                    prevDcnt = tmp[3]['pur_gubn5'].value_counts()[0]

                    try:
                        prevBcnt = tmp[3]['pur_gubn5'].value_counts()[1]
                    except Exception as e:
                        prevBcnt = 0.8
                        pass
                    print(
                        '######################################################################################################################################')
                    print("[" + str(vLoop) + "][" + datetime.datetime.today().strftime(
                        "%Y-%m-%d %H:%M:%S") + '] sourceData : lvl_' + str(i - 1) + '_' + str(j)
                          + " / dvsb : " + str(round(prevDcnt / prevBcnt, 2))
                          + " / bvsd : " + str(round(prevBcnt / prevDcnt, 2))
                          + " / prevBcnt : "
                          + str(prevBcnt) + " / prevDcnt : " + str(prevDcnt) + " / entropy : " + str(
                        round(entropy(tmp[3]['pur_gubn5']), 2))
                          )
                    print(
                        '######################################################################################################################################')

                    if prevBcnt > 0.8:
                        tmpFF = conditionMake(tmp[3], tmp[2], i, tmp[4], prevBcnt, prevDcnt, branch, name,
                                              paramLevel, j,
                                              paramLastRatio, limitCnt, lastYn, strs)
                        tmpMkL = tmpMkL.append(tmpFF)

                    try:
                        if i > 1:
                            print('delete pickle - lvl_{}_{}.pkl'.format(i - 1, j))
                            # print(e)
                            if os.path.isfile(
                                    'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + 'ReNew' + '/lvl_{}_{}.pkl'.format(
                                        i - 1, j)):
                                os.remove(
                                    'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + 'ReNew' + '/lvl_{}_{}.pkl'.format(
                                        i - 1, j))


                    except Exception as e:
                        # print(e)
                        pass


                except Exception as e:
                    prevDcnt = 0.8
                    pass
    return tmpMkL

if __name__ == '__main__':
    #####################################################################################################
    # ???????????? ??????   ?????? ?????? ????????? ?????? ?????? 20??? ?????? ??????
    # ?????? ???????????? ?????? ?????? ??? ?????? ??????
    # ????????? ??????, ????????? -1 ???????????? 7??? ?????? ?????? ??? ????????? ???????????? + 2?????? ??? ?????? ?????? ?????? (2??? ??????)
    #####################################################################################################
    name = 'sjtabuy'  # ????????? ?????? ???
    path = "C:/Users/Shine_anal/Desktop/inott/"  # ????????? ????????? + _com.csv ????????? ???????????? ?????? (????????? csv??????)
    paramLevel = 4  # ?????? LEVEL???
    paramAftLevel = 2  # ?????? LEVEL ?????? ???????????? LEVEL???
    # paramLimitCnt = 7  # ??????????????? ??? dcnt?????? x??? ????????? ????????? ????????? ?????? ??????
    paramLimitRatio = 0.6 # ??????????????? ??? dcnt?????? ????????? x ??? ????????? ????????? ????????? ?????? ?????? (????????? ???!!)
    paramLastRatio = 2  # ????????? ???????????? dvsb??? x:1 ????????? ?????? ????????? ?????? ??????
    paramLoop = 3  # loop Count Setting
    #####################################################################################################
    ray.shutdown()
    ray.init(num_cpus=20, log_to_driver=False)
    # 5 - 13s - 45% / 10 - 8s - 74% / 15 - 8s - 72 ~ 90% / 20 - 5s - 78 ~ 100%
    branch = 11

    # initData = pd.read_csv(path + name + '_com.csv')

    initData = pd.read_csv("C:/Users/Shine_anal/Desktop/" + name + '_com.csv')
    initStr = pd.read_csv(path + name + '_com_str.csv', names=['condi'])

    for x in range(0, len(initStr)):
        initStr = initStr.append(pd.DataFrame([(initStr.iloc[x]['condi'].replace('>', '<='))], columns=['condi']))

    realInitData = initData
    initBcnt = initData['pur_gubn5'].value_counts()[1]
    initDcnt = initData['pur_gubn5'].value_counts()[0]
    limitCnt = initDcnt * paramLimitRatio * 0.01
    realFinal = pd.DataFrame()
    lastFinal = pd.DataFrame()
    realFinal['index'] = ''
    tmpFF = pd.DataFrame()
    tmpLoopFinal = pd.DataFrame()
    print('limitCnt : ' + str(limitCnt))
    mod = sys.modules[__name__]

    for vLoop in range(0, paramLoop):
        # vLoop = 0
        # random?????? 80% ??????
        data = initData.sample(frac=0.8, random_state=1004)

        # vLoop = 0
        tmpDcnt = data['pur_gubn5'].value_counts()[0]
        tmpBcnt = data['pur_gubn5'].value_counts()[1]

        ######################################################################################################################################
        # 4???????????? ?????? ?????? ??????
        ######################################################################################################################################
        # PICKLE FILE DELETE
        createFolder("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name + 'ReNew')
        removeAllFile("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name + 'ReNew')

        realFinal = realFinal.append(makeLevel(vLoop, paramLevel, paramLastRatio, limitCnt, name, branch, data, '', 'N', initStr))

        realFinal = realFinal.astype(
            {'dcnt': 'int', 'bcnt': 'int', 'bvsd': 'float', 'dvsb': 'float', 'entr': 'float'})
        # realFinal.to_csv(
        #         "C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTRENEW/" + name + "_" + str(
        #             vLoop) + "_result222.csv")
        # realFinal = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTRENEW/lastFinal_result_5_20.csv")

        # 4???????????? ??? ???????????? bcnt??? 0??? ???????????? ????????? ?????????.
        lastFinal = realFinal[(realFinal['bcnt'] < 1) & (realFinal['dcnt'] > 0.8)]
        # realFinal?????? bcnt??? 0??? ???????????? ?????????.
        realFinal = realFinal[~((realFinal['bcnt'] < 1) & (realFinal['dcnt'] > 0.8))]
        # realFinal?????? ?????? ????????? limitCnt dcnt??? ??? ???????????? ?????????.
        realFinal = realFinal[realFinal['dcnt'] >= limitCnt]

        realFinal = realFinal.sort_values('dvsb', ascending=False)

        # lastFinal[lastFinal['condi'] == 'dis5_ang1_dis20_ang1 > -5.7 AND atr_ang3_1bd > 11.7 AND dis5_ang1 <= 8.8 AND dis10_dis60 > -34.9']
        # checkCondition(initData, 'vr25_1bd_rate > 115.0 AND slow5D_ang1_fast5D_ang2 > -1.0 AND close_open_close_3bd_open_arc > 22.0 AND aroon > 92.0 AND momp5_sig4_ang1_momp5_ang2 > -7.0 AND nmind_ang2 > 8.0')
        lvl4Conds = pd.DataFrame()
        lvl4Data = data

        # ??? ?????? ?????? bcnt, dcnt, dvsb??? ?????? ??????
        for y in range(0, len(realFinal)):
            try:
                if realFinal.iloc[0].dcnt > 0:
                    pass
            except:
                break

            if realFinal.iloc[0].dcnt >= limitCnt:
                # realFinal??? dvsb ?????????????????? ??????
                realFinal = realFinal[realFinal['dvsb'] >= 0.5]
                realFinal = realFinal.sort_values('dvsb', ascending=False)

                for x in range(0, len(realFinal)):
                    # x=0
                    # limitCnt ?????? ??? ????????? ??? dvsb??? ?????? ??? ????????? ?????? ???????????? ?????????.
                    lvindex, lvdcnt, lvbcnt, lvdvsb, lvData = checkCondition(lvl4Data, realFinal.iloc[x]['condi'])
                    realFinal['index'].iloc[x] = lvindex
                    realFinal['dcnt'].iloc[x] = lvdcnt
                    realFinal['bcnt'].iloc[x] = lvbcnt
                    realFinal['dvsb'].iloc[x] = lvdvsb

                    print(str(y) + '-' + str(x) + ' - bcnt : ' + str(lvbcnt) + ' / dcnt : ' + str(lvdcnt) + ' / dvsb : ' + str(round(lvdcnt/lvbcnt,2)) + ' / ' + realFinal.iloc[x]['condi'])
                # kk = realFinal[realFinal['condi'] == 'dis5_ang1_dis20_ang1 > -5.7 AND atr_ang3_1bd > 11.7 AND dis5_ang1 <= 8.8 AND dis10_dis60 > -34.9']
                ################################################################################################################
                # realFinal??? dvsb ?????????????????? ??????
                realFinal = realFinal.sort_values('dvsb', ascending=False)

                # lvl4Conds??? ?????? ?????? ????????? ????????????
                lvl4Conds = lvl4Conds.append(
                                            pd.DataFrame([(realFinal.iloc[0]['condi'],realFinal.iloc[0]['bcnt'],realFinal.iloc[0]['dcnt'],realFinal.iloc[0]['dvsb'])]
                                                         ,columns=['condi','bcnt','dcnt','dvsb'])
                                            )

                # ?????? ?????? ????????? ??????
                lastFinal = lastFinal.append(realFinal.iloc[0])

                # dvsb??? ?????? ?????? ????????? ???????????? ?????????
                try:
                  lvl4Data = lvl4Data.drop(index=realFinal.iloc[0]['index'])
                except:
                    pass
                ################################################################################################################
                # lvl4Cond??? ????????? dvsb?????? ?????? dcnt??? ?????? ??? ????????? ????????????..
                realFinal = realFinal[realFinal['dvsb'] >= paramLastRatio]

                if len(realFinal) > 0:
                    # realFinal??? dcnt ?????????????????? ??????
                    realFinal = realFinal.sort_values(['dcnt','bcnt'], ascending=[False, True])

                    lvl4Conds = lvl4Conds.append(
                        pd.DataFrame([(realFinal.iloc[0]['condi'], realFinal.iloc[0]['bcnt'], realFinal.iloc[0]['dcnt'],
                                       realFinal.iloc[0]['dvsb'])]
                                     , columns=['condi', 'bcnt', 'dcnt', 'dvsb'])
                    )

                    # ?????? ?????? ????????? ??????
                    lastFinal = lastFinal.append(realFinal.iloc[0])

                    # realFinal?????? ?????? ?????? ?????? ??????
                    realFinal = realFinal[~(realFinal['condi'] == realFinal.iloc[0]['condi'])]
                ################################################################################################################

            else:
                break
        ######################################################################################################################################
        # 4?????? ?????? 2??? ?????? ??? ?????? ??????
        ######################################################################################################################################
        # vLoop = 0
        for u in range(0, len(lvl4Conds)):
            # PICKLE FILE DELETE
            createFolder("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name + 'ReNew')
            removeAllFile("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name + 'ReNew')
            print(str(u) + ' - ' + lvl4Conds.iloc[u].condi)
            tempGG = makeLevel(vLoop, paramAftLevel, paramLastRatio, limitCnt, name, branch, checkCondition(data, lvl4Conds.iloc[u].condi)[4], lvl4Conds.iloc[u].condi, 'Y', initStr)
            lastFinal = lastFinal.append(tempGG)

        lastFinal = lastFinal.astype(
            {'dcnt': 'int', 'bcnt': 'int', 'bvsd': 'float', 'dvsb': 'float', 'entr': 'float'})

        lastFinal = lastFinal.sort_values('dvsb', ascending=False)
        lastFinal = lastFinal[lastFinal['dvsb'] >= paramLastRatio]

        tmpLoopFinal = tmpLoopFinal.append(lastFinal)

        lastFinal['rDcnt'] = 0
        lastFinal['rBcnt'] = 0
        lastFinal['rDvsb'] = 0

        # 80% ???????????? ????????? ?????? ????????? ???????????? ???????????? 100% ???????????? ???????????????.
        for m in range(0, len(lastFinal)):
            lvindex, lvdcnt, lvbcnt, lvdvsb, lvData = checkCondition(initData, lastFinal.iloc[m]['condi'])
            lastFinal['rDcnt'].iloc[m] = lvdcnt
            lastFinal['rBcnt'].iloc[m] = lvbcnt
            lastFinal['rDvsb'].iloc[m] = lvdvsb

        try:
            lastFinal.to_csv(
                "C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTRENEW/" + name + "_" + str(
                    vLoop) + "_result.csv")
        except:
            pass

        # loop ?????? ??? ????????? ?????? ??? ?????? ??? dcnt & bcnt??? 0??? ????????? ???????????? data??? ?????????.
        lastFinal = lastFinal[(lastFinal['rDcnt'] >= limitCnt) & (lastFinal['rBcnt'] < 1)]

        for z in range(0, len(lastFinal)):
            try:
                initData = initData.drop(index=checkCondition(initData, lastFinal.iloc[z].condi)[0])
            except:
                pass

        # data.value_counts('pur_gubn5')
    tmpLoopFinal.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTRENEW/" + name + "_result.csv")
    fResultMid = makeFinalSet(path, name, paramLastRatio, realInitData)
    ray.shutdown()
    #####################################################################################################
    # ?????????????????????   ????????? ?????? ??????_allnew_ddelTreeNew_result.csv
    #####################################################################################################
    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    print('????????????????????? ' + path + name + '_ddelTreeLoopRandom_result.csv  ?????????.')
    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    #####################################################################################################
