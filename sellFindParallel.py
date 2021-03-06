# coding=utf-8
import pandas as pd
import warnings
import sys
import datetime
import os
import pickle
import numpy as np
import ray

# COMMIT TEST
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
    finalSet = finalSet[(finalSet['value'] >= -1000) & (finalSet['value'] <= 1000)]
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

@ray.remote
def colValueCalc(col, aggr_df):
    # import pandas as pd
    # name = 'ncbuy2'  # ????????? ?????? ???
    # path = "C:/Users/Shine_anal/Desktop/inott/"  # ????????? ????????? + _com.csv ????????? ???????????? ?????? (????????? csv??????)
    # data = pd.read_csv(path + name + '_com.csv')
    # col = data.iloc[:,9:data.shape[1]]
    # aggr_df = data.iloc[:,4]
    rtnDf = pd.DataFrame()

    for i in range(0, col.shape[1]):
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

def if_float(string):
    try:
        float(string)
        return True
    except ValueError:
        return False


def conditionMake(data, aggr_df, lvl, initCondition, prevBcnt, prevDcnt, branch, name, limitLvl, lvlNum, lastLimitRatio,
                  limitCnt, mode):
    # data, aggr_df, lvl, initCondition, prevBcnt, prevDcnt, branch, name, limitLvl, lvlNum = tmp[3], tmp[2], i, tmp[4], prevBcnt, prevDcnt, branch, name, i, j
    tmpFinal = pd.DataFrame()

    exceptCon = pd.DataFrame()
    fResultT = pd.DataFrame()
    if mode == 'init':
        df_split = np.array_split(data.iloc[:, 9:data.shape[1] - 2], 20, axis=1)
        tmp = [colValueCalc.remote(x, aggr_df) for x in df_split]

        result = pd.concat([ray.get(tmp)[0], ray.get(tmp)[1], ray.get(tmp)[2], ray.get(tmp)[3], ray.get(tmp)[4],
                            ray.get(tmp)[5]
                               , ray.get(tmp)[6], ray.get(tmp)[7], ray.get(tmp)[8], ray.get(tmp)[9],
                            ray.get(tmp)[10], ray.get(tmp)[11]
                               , ray.get(tmp)[12], ray.get(tmp)[13], ray.get(tmp)[14], ray.get(tmp)[15],
                            ray.get(tmp)[16], ray.get(tmp)[17]
                               , ray.get(tmp)[18], ray.get(tmp)[19]], axis=1)
        # result = ray.get(colValueCalc.remote(df_split[k], aggr_df) for k in range(12))
        # print(result)
        # ??? ?????? ??? bcnt, dcnt, bvsd, dvsb

        rslt_split = np.array_split(result, 20, axis=1)
        tmpRslt = [makeFinalDf.remote(y) for y in rslt_split]

        tmpFinal = pd.concat(
            [ray.get(tmpRslt)[0], ray.get(tmpRslt)[1], ray.get(tmpRslt)[2], ray.get(tmpRslt)[3],
             ray.get(tmpRslt)[4],
             ray.get(tmpRslt)[5]
                , ray.get(tmpRslt)[6], ray.get(tmpRslt)[7], ray.get(tmpRslt)[8], ray.get(tmpRslt)[9],
             ray.get(tmpRslt)[10], ray.get(tmpRslt)[11]
                , ray.get(tmpRslt)[12], ray.get(tmpRslt)[13], ray.get(tmpRslt)[14], ray.get(tmpRslt)[15],
             ray.get(tmpRslt)[16], ray.get(tmpRslt)[17]
                , ray.get(tmpRslt)[18], ray.get(tmpRslt)[19]], axis=0)

        # ?????? ?????? bcnt??? 20% ??????????????? bvsd??? ?????? ?????? ???
        tmpExec = tmpFinal[(tmpFinal['dcnt'] >= prevDcnt * 0.8)
                         & (tmpFinal['bcnt'] <= prevBcnt * 0.8)].sort_values('bvsd', ascending=False)

        fResultT = tmpExec
    else:
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
                    # result = colValueCalc(data.iloc[:, 5:data.shape[1]], aggr_df)

                    # result = ray.get(colValueCalc.remote(data.iloc[:, 9:data.shape[1] - 2], aggr_df))

                    df_split = np.array_split(data.iloc[:, 9:data.shape[1] - 2], 20, axis=1)
                    tmp = [colValueCalc.remote(x, aggr_df) for x in df_split]
                    # print(tmp)
                    # print('----------------')
                    # print(ray.get(tmp)[0])
                    # print('----------------')
                    # print(ray.get(tmp)[1])
                    # print('----------------')
                    result = pd.concat([ray.get(tmp)[0], ray.get(tmp)[1], ray.get(tmp)[2], ray.get(tmp)[3], ray.get(tmp)[4],
                                        ray.get(tmp)[5]
                                           , ray.get(tmp)[6], ray.get(tmp)[7], ray.get(tmp)[8], ray.get(tmp)[9],
                                        ray.get(tmp)[10], ray.get(tmp)[11]
                                           , ray.get(tmp)[12], ray.get(tmp)[13], ray.get(tmp)[14], ray.get(tmp)[15],
                                        ray.get(tmp)[16], ray.get(tmp)[17]
                                           , ray.get(tmp)[18], ray.get(tmp)[19]], axis=1)
                    # result = ray.get(colValueCalc.remote(df_split[k], aggr_df) for k in range(12))
                    # print(result)
                    # ??? ?????? ??? bcnt, dcnt, bvsd, dvsb

                    rslt_split = np.array_split(result, 20, axis=1)
                    tmpRslt = [makeFinalDf.remote(y) for y in rslt_split]

                    tmpFinal = pd.concat(
                        [ray.get(tmpRslt)[0], ray.get(tmpRslt)[1], ray.get(tmpRslt)[2], ray.get(tmpRslt)[3],
                         ray.get(tmpRslt)[4],
                         ray.get(tmpRslt)[5]
                            , ray.get(tmpRslt)[6], ray.get(tmpRslt)[7], ray.get(tmpRslt)[8], ray.get(tmpRslt)[9],
                         ray.get(tmpRslt)[10], ray.get(tmpRslt)[11]
                            , ray.get(tmpRslt)[12], ray.get(tmpRslt)[13], ray.get(tmpRslt)[14], ray.get(tmpRslt)[15],
                         ray.get(tmpRslt)[16], ray.get(tmpRslt)[17]
                            , ray.get(tmpRslt)[18], ray.get(tmpRslt)[19]], axis=0)

                    # tmpFinal = ray.get(makeFinalDf.remote(result))
                    # tmpFinal = makeFinalDf(result)

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

                    tmpExec = tmpFinal[(tmpFinal['bcnt'] >= prevBcnt * 0.9)
                             & (tmpFinal['bcnt'] / tmpFinal['dcnt'] > prevBcnt / prevDcnt)
                             ].sort_values('bvsd', ascending=False).iloc[0]
                elif u % branch == 2:
                    tmpExec = tmpFinal[(tmpFinal['bcnt'] >= prevBcnt * 0.8)
                             & (tmpFinal['bcnt'] / tmpFinal['dcnt'] > prevBcnt / prevDcnt)
                             ].sort_values('bvsd', ascending=False).iloc[0]
                elif u % branch == 3:
                    tmpExec = tmpFinal[(tmpFinal['bcnt'] >= prevBcnt * 0.7)
                             & (tmpFinal['bcnt'] / tmpFinal['dcnt'] > prevBcnt / prevDcnt)
                             ].sort_values('bvsd', ascending=False).iloc[0]
                elif u % branch == 4:
                    tmpExec = tmpFinal[(tmpFinal['bcnt'] >= prevBcnt * 0.6)
                             & (tmpFinal['bcnt'] / tmpFinal['dcnt'] > prevBcnt / prevDcnt)
                             ].sort_values('bvsd', ascending=False).iloc[0]
                elif u % branch == 0:
                    tmpExec = tmpFinal[(tmpFinal['bcnt'] >= prevBcnt * 0.5)
                             & (tmpFinal['bcnt'] / tmpFinal['dcnt'] > prevBcnt / prevDcnt)
                             ].sort_values('bvsd', ascending=False).iloc[0]
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
                          + "\nbvsd : " + str(round(tmpExec['bvsd'], 2))
                          + " / bcnt : " + str(int(tmpExec['bcnt']))
                          + " / dcnt : " + str(int(tmpExec['dcnt']))
                          + " / condition : " + condition)

                    # ????????? ?????? ??????
                    try:
                        tmpFinal = tmpFinal[~(tmpFinal['condi'] == conH)]
                    except:
                        pass

                    if (lvl < limitLvl - 1 and tmpExec['bcnt'] > limitCnt) or (
                            lvl == limitLvl - 1 and tmpExec['bvsd'] > lastLimitRatio and tmpExec['bcnt'] > limitCnt):
                        print('lvl_{}_{}.pkl'.format(lvl, u) + ' pickle file create')
                        with open('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/'
                                  + name
                                  + '/lvl_{}_{}.pkl'.format(lvl, u)
                                , 'wb') as f:
                            pickle.dump([tmpFinal, tmpBool, tmpAggr, tmpData, condition], f)
                        # setattr(mod,'lvl_{}_{}'.format(lvl, u), [tmpFinal, tmpBool, tmpAggr, tmpData, condition])

                    try:
                        fResultT = fResultT.append(
                            pd.DataFrame([('lvl_' + str(lvl) + '_' + str(u), condition, str(int(tmpExec['bcnt'])),
                                           str(int(tmpExec['dcnt'])), tmpExec['bcnt'] / tmpExec['dcnt'],
                                           tmpExec['dcnt'] / tmpExec['bcnt'])],
                                         columns=['lvl', 'condi', 'bcnt', 'dcnt', 'bvsd', 'dvsb']))
                    except Exception as e:
                        pass
            except Exception as e:
                # print(e)
                fResultT = fResultT.append(
                    pd.DataFrame([('lvl_' + str(lvl) + '_' + str(u), '', '0',
                                   '0', '0', '0')],
                                 columns=['lvl', 'condi', 'bcnt', 'dcnt', 'bvsd', 'dvsb']))
                pass

    return fResultT

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

if __name__ == '__main__':
    #####################################################################################################
    # ???????????? ??????   ?????? ?????? ????????? ?????? ?????? 20??? ?????? ??????
    #####################################################################################################
    name = 'sjtabuy'  # ????????? ?????? ???
    path = "C:/Users/Shine_anal/Desktop/inott/"  # ????????? ????????? + _com.csv ????????? ???????????? ?????? (????????? csv??????)
    paramLevel = 5  # ??????LEVEL???
    paramLimitRatio = 0.4  # ??????????????? ??? dcnt?????? x% ????????? ????????? ????????? ?????? ?????? (%)
    paramLastRatio = 2.5  # ????????? ???????????? dvsb??? x:1 ????????? ?????? ????????? ?????? ??????
    #####################################################################################################

    createFolder("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name)
    removeAllFile("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name)

    ray.init(num_cpus=20, log_to_driver=False)

    branch = 5

    data = pd.read_csv(path + name + '_com.csv')

    # data[
    #     (data['momp_ang2_momp5_sig4_ang2'] > -3.7)
    #     & (data['nmind_ang2_nmind_sig_ang2'] <= -19.7)
    #     & (data['fast3D_ang1_slow3D_ang1'] > 5.3)
    #     & (data['momp5_sig4_ang1_momp5_sig4_ang2'] <= -1.5)
    #     & (data['DIn'] > 46.5)
    #     & (data['rsi_high5d_low5d_rate'] <= 43.2)
    #     ].value_counts('pur_gubn5')

    initBcnt = data.iloc[:, 4].value_counts()[1]
    initDcnt = data.iloc[:, 4].value_counts()[0]
    limitCnt = initDcnt * paramLimitRatio * 0.01
    realFinal = pd.DataFrame()
    tmpFF = pd.DataFrame()
    print('limitCnt : ' + str(limitCnt))

    initConds = conditionMake(data, data.iloc[:, 4], 0, '', initBcnt, initDcnt, branch, name,
                          paramLevel, 0,
                          paramLastRatio, limitCnt, 'init')

    initTmp = pd.DataFrame()

    for o in range(0, 15):
        initTmp = initTmp.append(initConds.iloc[0])
        tmpCond = initConds.iloc[0].condi
        initConds = initConds[~(initConds['condi'] == tmpCond)]


    for w in range(0, initTmp.shape[0]):
        conH = initTmp.iloc[w]['condi']
        valH = initTmp.iloc[w]['value']
        calH = initTmp.iloc[w]['cal']

        if calH == 'GT':
            tmpData = data[data[conH] > float(valH)]
            tmpCondition = conH + ' > ' + str(valH)
        else:
            tmpData = data[data[conH] <= float(valH)]
            tmpCondition = conH + ' <= ' + str(valH)

        for i in range(0, paramLevel + 1):
            if i == 0:
                with open('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_0_1.pkl', 'wb') as f:
                    pickle.dump(['', '', tmpData.iloc[:, 4], tmpData, tmpCondition, ''], f)
            else:
                leaf = branch ** (i - 1)
                for j in range(1, leaf + 1):
                    tmpNxtRat = pd.DataFrame()
                    tmpRatList = pd.DataFrame()
                    tmpDat = pd.DataFrame()
                    tmpAggrDf = pd.DataFrame()
                    tmpBool = pd.DataFrame()
                    tmpFinal = pd.DataFrame()

                    prevBcnt = 0.8
                    prevDcnt = 0.8
                    tmp = pd.DataFrame()

                    try:
                        with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/"
                                  + name + "/lvl_{}_{}.pkl".format(i - 1, j), "rb") as fr:
                            tmp = pickle.load(fr)

                            # tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j))
                    except Exception as e:
                        tmp = pd.DataFrame()
                        # print(e)
                        pass

                    try:
                        prevDcnt = tmp[3].iloc[:, 4].value_counts()[0]

                        try:
                            prevBcnt = tmp[3].iloc[:, 4].value_counts()[1]
                        except Exception as e:
                            prevBcnt = 0.8
                            pass

                        print('[' + str(w) + "][" + datetime.datetime.today().strftime(
                            "%Y-%m-%d %H:%M:%S") + '] sourceData : lvl_' + str(i - 1) + '_' + str(j)
                              + " / dvsb : " + str(round(prevDcnt / prevBcnt, 2))
                              + " / bvsd : " + str(round(prevBcnt / prevDcnt, 2))
                              + " / prevBcnt : "
                              + str(prevBcnt) + " / prevDcnt : " + str(prevDcnt)
                              )

                        if (i < paramLevel and prevBcnt > limitCnt) or (
                                i == paramLevel and prevBcnt / prevDcnt > paramLastRatio and prevBcnt > limitCnt):
                            tmpFF = conditionMake(tmp[3], tmp[2], i, tmp[4], prevBcnt, prevDcnt, branch, name,
                                                  paramLevel, j,
                                                  paramLastRatio, limitCnt, 'br')
                            realFinal = realFinal.append(tmpFF)

                        try:
                            if i > 1:
                                print('delete pickle - lvl_{}_{}.pkl'.format(i - 1, j))
                                if os.path.isfile(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                            i - 1, j)):
                                    os.remove(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                            i - 1, j))
                        except Exception as e:
                            # print(e)
                            pass

                    except Exception as e:
                        prevDcnt = 0.8
                        # print(e)
                        pass

        realFinal.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULT/" + name + "_result_" + str(w) + ".csv")
    fResultMid = makeFinalSet(path, name)

    #####################################################################################################
    # ?????????????????????   ????????? ?????? ??????_allnew_ddelTreeNew_result.csv
    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    print('????????????????????? ' + path + name + '_sellFindParallel_result.csv  ?????????.')
    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    #####################################################################################################




##########################################################################################
# Data Loading
path = "C:/Users/Shine_anal/PycharmProjects/anlaysis/SELLMART_ALL_NEW_202201_ADD/SELLMART_ALL_NEW_05.csv"
additionalCond = "((data['R_CCIRSI13'] > -163.9) & (data['R_DIS5DIS6020'] <= 0.0) & (data['R_DIS5DIS1018'] > -439.3) & (data['R_CCI8'] > -24.5) & (data['R_HL2'] <= 0.0)) " \
                 "| ((data['R_DIS5DIS601'] <= 0.2) & (data['R_CCIRSI5'] > -102.2) & (data['R_DIS5DIS6024'] <= 0.0) & (data['R_LH9'] > -0.1) & (data['R_HIH3'] <= 0.0)) " \
                 "| ((data['R_HIH7'] > -0.4) & (data['R_DIS5DIS6023'] <= 0.0) & (data['R_LH9'] > -0.2) & (data['R_RSI10'] <= 3.0) & (data['R_HL3'] <= 0.0))"

name = 'SELLMART_ALL_NEW_05_7_all'
# function Exec
fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'all')

name = 'SELLMART_ALL_NEW_05_7_fsjtabuy'
# function Exec
fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'fsjtabuy')

name = 'SELLMART_ALL_NEW_05_7_fncbuy3'
# function Exec
fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'fncbuy3')

name = 'SELLMART_ALL_NEW_05_7_finobuy1'
# function Exec
fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'finobuy1')

name = 'SELLMART_ALL_NEW_05_7_finobuy2'
# function Exec
fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'finobuy2')

name = 'SELLMART_ALL_NEW_05_7_finobuyt8'
# function Exec
fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'finobuyt8')
# ##########################################################################################
#
# ##########################################################################################
# # Data Loading
path = "C:/Users/Shine_anal/PycharmProjects/anlaysis/SELLMART_ALL_NEW_202201_ADD/SELLMART_ALL_NEW_510.csv"
additionalCond = "((data['R_LH7'] > -0.9) & (data['R_RSICCI18'] > -89.3) & (data['R_MACDMACSSIG13'] > -48.6) & (data['R_CCIRSI1'] > 3.0) & (data['R_HIH13'] > 0.5))       " \
                 " | ((data['R_LH7'] > -0.9) & (data['R_OPN7'] > 0.2) & (data['R_MACD7'] > -69.5) & (data['R_HIH6'] > 0.4) & (data['R_HIH4'] > 0.5))                      " \
                 " | ((data['R_LH7'] > -0.9) & (data['R_HIH13'] > 0.0) & (data['R_MACDMACSSIG7'] > -70.2) & (data['R_HIH6'] > 0.3) & (data['R_HIH4'] > 0.4))              " \
                 " | ((data['R_DIS10DIS601'] <= 0.5) & (data['R_LH7'] > -0.5) & (data['R_MACDMACSSIG7'] > -69.0) & (data['R_HIH7'] > 0.5) & (data['R_HIH6'] > 0.7))       " \
                 " | ((data['R_DIS10DIS601'] <= 0.5) & (data['R_LH7'] > -0.5) & (data['R_MACDMACSSIG7'] > -69.0) & (data['R_HIH7'] > 0.5) & (data['R_CCI13'] > -74.1))    " \
                 " | ((data['R_DIS10DIS601'] <= 0.5) & (data['R_LH7'] > -0.5) & (data['R_MACDMACSSIG7'] > -69.0) & (data['R_CCIRSI12'] > -39.7) & (data['R_HIH7'] > 0.5)) "

name = 'SELLMART_ALL_NEW_510_3_all'
# function Exec
fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'all')

# name = 'SELLMART_ALL_NEW_510_3_fsjtabuy'
# # function Exec
# fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'fsjtabuy')

# name = 'SELLMART_ALL_NEW_510_3_fncbuy3'
# # function Exec
# fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'fncbuy3')

# name = 'SELLMART_ALL_NEW_510_3_finobuy1'
# # function Exec
# fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'finobuy1')

# name = 'SELLMART_ALL_NEW_510_3_finobuy2'
# # function Exec
# fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'finobuy2')

# name = 'SELLMART_ALL_NEW_510_3_finobuyt8'
# # function Exec
# fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'finobuyt8')
# ##########################################################################################
#
# ##########################################################################################
# # Data Loading
path = "C:/Users/Shine_anal/PycharmProjects/anlaysis/SELLMART_ALL_NEW_202201_ADD/SELLMART_ALL_NEW_1015.csv"
additionalCond = ''

name = 'SELLMART_ALL_NEW_1015_3_all'
# function Exec
fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'all')

# name = 'SELLMART_ALL_NEW_1015_3_fsjtabuy'
# # function Exec
# fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'fsjtabuy')

# name = 'SELLMART_ALL_NEW_1015_3_fncbuy3'
# # function Exec
# fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'fncbuy3')

# name = 'SELLMART_ALL_NEW_1015_3_finobuy1'
# # function Exec
# fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'finobuy1')

# name = 'SELLMART_ALL_NEW_1015_3_finobuy2'
# # function Exec
# fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'finobuy2')

# name = 'SELLMART_ALL_NEW_1015_3_finobuyt8'
# # function Exec
# fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'finobuyt8')
# ##########################################################################################
# # Data Loading
path = "C:/Users/Shine_anal/PycharmProjects/anlaysis/SELLMART_ALL_NEW_202201_ADD/SELLMART_ALL_NEW_15AB.csv"
additionalCond = ''

name = 'SELLMART_ALL_NEW_15AB_3_all'
# function Exec
fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'all')

# name = 'SELLMART_ALL_NEW_15AB_3_fsjtabuy'
# # function Exec
# fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'fsjtabuy')

# name = 'SELLMART_ALL_NEW_15AB_3_fncbuy3'
# # function Exec
# fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'fncbuy3')

# name = 'SELLMART_ALL_NEW_15AB_3_finobuy1'
# # function Exec
# fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'finobuy1')

# name = 'SELLMART_ALL_NEW_15AB_3_finobuy2'
# # function Exec
# fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'finobuy2')

# name = 'SELLMART_ALL_NEW_15AB_3_finobuyt8'
# # function Exec
# fResult = drawCondiInitTree(path, 4, 0.2, additionalCond, name, 'finobuyt8')