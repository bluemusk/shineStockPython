# coding=utf-8
import re
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
    # name = 'ncbuy2'  # 사용자 지정 명
    # path = "C:/Users/Shine_anal/Desktop/inott/"  # 사용자 지정명 + _com.csv 파일이 존재하는 폴더 (분석할 csv파일)
    # data = pd.read_csv(path + name + '_com.csv')
    # col = data.iloc[:,9:data.shape[1]]
    # aggr_df = data.iloc[:,4]
    tmpTechData = col.describe()
    rtnDf = pd.DataFrame()

    for i in range(0, col.shape[1]):
        num_1 = round(tmpTechData[col.columns[i]]['25%'], 1)
        num_2 = round(tmpTechData[col.columns[i]]['50%'], 1)
        num_3 = round(tmpTechData[col.columns[i]]['75%'], 1)

        IQR = num_3 - num_1

        num_0 = round(tmpTechData[col.columns[i]]['25%'], 1) - IQR * 1.5
        num_4 = round(tmpTechData[col.columns[i]]['75%'], 1) + IQR * 1.5

        step1 = round((num_0 + num_1) / 5, 1)
        step2 = round((num_1 + num_2) / 5, 1)
        step3 = round((num_2 + num_3) / 5, 1)
        step4 = round((num_3 + num_4) / 5, 1)

        # num_0 = round(col.iloc[:, i].quantile(0), 1)
        # num_1 = round(col.iloc[:, i].quantile(0.25), 1)
        # num_2 = round(col.iloc[:, i].quantile(0.5), 1)
        # num_3 = round(col.iloc[:, i].quantile(0.75), 1)
        # num_4 = round(col.iloc[:, i].quantile(1), 1)
        #
        # step1 = round((num_0 + num_1) / 5, 1)
        # step2 = round((num_1 + num_2) / 5, 1)
        # step3 = round((num_2 + num_3) / 5, 1)
        # step4 = round((num_3 + num_4) / 5, 1)

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
    elements, counts = np.unique(target_col, return_counts=True)
    entropy = -np.sum(
        [(counts[i] / np.sum(counts)) * np.log2(counts[i] / np.sum(counts)) for i in range(len(elements))])
    return entropy

def splitData(data, aggr_df, prevBcnt, prevDcnt, limitCnt):
    # data = data[data.columns.difference(['pur_gubn5', 'delYn', 'yyyymmdd', 'stock_code', 'pur_gubn'])]
    df_split = np.array_split(data[data.columns.difference(['pur_gubn5', 'delYn', 'yyyymmdd', 'stock_code', 'pur_gubn'])], 20, axis=1)
    tmp = [colValueCalc.remote(x, aggr_df) for x in df_split]

    result = pd.concat([ray.get(tmp)[0], ray.get(tmp)[1], ray.get(tmp)[2], ray.get(tmp)[3], ray.get(tmp)[4],
                        ray.get(tmp)[5]
                           , ray.get(tmp)[6], ray.get(tmp)[7], ray.get(tmp)[8], ray.get(tmp)[9],
                        ray.get(tmp)[10], ray.get(tmp)[11]
                           , ray.get(tmp)[12], ray.get(tmp)[13], ray.get(tmp)[14], ray.get(tmp)[15],
                        ray.get(tmp)[16], ray.get(tmp)[17]
                           , ray.get(tmp)[18], ray.get(tmp)[19]], axis=1)

    # result 20 split
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

    tmpFinal = tmpFinal[tmpFinal['dcnt'] > limitCnt]
    tmpFinal = tmpFinal.drop_duplicates()

    return tmpFinal

def changeCondition(origin):
    origin = origin.split(' ')
    mod = ''
    for o in range(0, len(origin)):
        if origin[o] == '>':
            origin[o] = '<='
        elif origin[o] == '<=':
            origin[o] = '>'
        elif origin[o] == 'AND':
            origin[o] = 'OR'
        elif origin[o] == 'OR':
            origin[o] = 'AND'

    for x in range(0, len(origin)):
        if x == 0:
            mod = origin[x]
        else:
            mod = mod + ' ' + origin[x]
    return mod

def checkConditionCnt(data, condition):
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

def calcRatioDf(paramData):
    df = paramData

    if df.value_counts('pur_gubn5')[0] >= 8000:
        paramLimitRatio = 0.1  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 3500:
        paramLimitRatio = 0.2  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 2500 and df.value_counts('pur_gubn5')[0] <= 3499:
        paramLimitRatio = 0.3  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 1500 and df.value_counts('pur_gubn5')[0] <= 2499:
        paramLimitRatio = 0.4  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 900 and df.value_counts('pur_gubn5')[0] <= 1499:
        paramLimitRatio = 0.7  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 600 and df.value_counts('pur_gubn5')[0] <= 899:
        paramLimitRatio = 0.9  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 400 and df.value_counts('pur_gubn5')[0] <= 599:
        paramLimitRatio = 1.2  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 250 and df.value_counts('pur_gubn5')[0] <= 399:
        paramLimitRatio = 1.5  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 50 and df.value_counts('pur_gubn5')[0] <= 249:
        paramLimitRatio = 2.2  # 초기데이터 중 dcnt대비

    return paramLimitRatio

def calcRatioLoop(data):
    df = data

    if df.value_counts('pur_gubn5')[0] >= 8000:
        paramLimitRatio = 0.1  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 3500:
        paramLimitRatio = 0.2  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 2500 and df.value_counts('pur_gubn5')[0] <= 3499:
        paramLimitRatio = 0.3  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 1500 and df.value_counts('pur_gubn5')[0] <= 2499:
        paramLimitRatio = 0.4  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 900 and df.value_counts('pur_gubn5')[0] <= 1499:
        paramLimitRatio = 0.7  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 600 and df.value_counts('pur_gubn5')[0] <= 899:
        paramLimitRatio = 0.9  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 400 and df.value_counts('pur_gubn5')[0] <= 599:
        paramLimitRatio = 1.2  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 250 and df.value_counts('pur_gubn5')[0] <= 399:
        paramLimitRatio = 1.5  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 50 and df.value_counts('pur_gubn5')[0] <= 249:
        paramLimitRatio = 2.2  # 초기데이터 중 dcnt대비

    return paramLimitRatio

def makeFinalSet(path, name, paramLoop):
    # 결과 합치기
    # import pandas as pd
    # import os
    # name = 'kbuy2'
    # paramLoop = 0
    # path = "C:/Users/Shine_anal/Desktop/inott/"
    if paramLoop == 0:
        data = pd.read_csv(path + name + '_com.csv')
    else:
        data = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/" + name + "_com_loop" + str(paramLoop) + ".csv")
    
    # 검증할 데이터
    initData = pd.read_csv(path + name + '_11year.csv')
    
    lists = os.listdir("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/")
    file_list_rslt = [file for file in lists if file.endswith("_result.csv") and file.find(name) > 0 and file.find(str(paramLoop) + ']') > 0]

    # 결과 합치기
    fResultMid = pd.DataFrame()
    fResultFin = pd.DataFrame()
    for i in range(0, len(file_list_rslt)):
        # print("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/" + file_list_rslt[i])
        tmp = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/" + file_list_rslt[i])
        fResultMid = fResultMid.append(tmp)
        print('------------------------------- / ' + file_list_rslt[i])
        print(fResultMid.value_counts('tree'))

    fResultMid = fResultMid.sort_values('dvsb', ascending=False)  # dvsb가 좋은 순서로 정렬

    limitCnt = data.value_counts('pur_gubn5')[0] * calcRatioLoop(data) * 0.01
    fResultMid = fResultMid[fResultMid['dvsb'] > limitCnt - 2]
    fResultMid = fResultMid.sort_values('dvsb', ascending=False)
    fResultMid = fResultMid.drop_duplicates()

    fResultMid['rDcnt'] = 0
    fResultMid['rBcnt'] = 0
    fResultMid['rDvsb'] = 0
    fResultMid['vIndex'] = ''
    
    # 최종 11년 데이터에 넣어서 비율을 다시 구한다.
    for m in range(0, len(fResultMid)):
        lvindex, lvdcnt, lvbcnt, lvdvsb, lvData = checkCondition(initData, fResultMid.iloc[m]['condi'])
        print(str(m) + ' / ' + str(len(fResultMid)) + ' / originalDvsb : ' + str(fResultMid['dvsb'].iloc[m]) + ' / dvsb : ' + str(lvdvsb) + ' / dcnt : ' + str(lvdcnt) + ' / bcnt : ' + str(lvbcnt))
        fResultMid['rDcnt'].iloc[m] = lvdcnt
        fResultMid['rBcnt'].iloc[m] = lvbcnt
        fResultMid['rDvsb'].iloc[m] = lvdvsb
        fResultMid['vIndex'].iloc[m] = lvindex

        # lvindex, lvdcnt, lvbcnt, lvdvsb, lvData = checkCondition(data, fResultMid.iloc[m]['condi'])
        # fResultMid['index'].iloc[m] = lvindex
    
    # limitCnt = 10
    # 결과 중 rBcnt 0인 애들을 모은 후 삭제
    fResultFin = fResultFin.append(fResultMid[(fResultMid['rBcnt'] < 1) & (fResultMid['rDcnt'] >= limitCnt)])

    data['delYn'] = 'N'

    if len(fResultFin) > 0:
        for z in range(0, len(fResultFin)):
            # z=0
            try:
                data['delYn'].iloc[fResultFin['index'].iloc[z]] = 'Y'
            except:
                pass
    # data.value_counts('pur_gubn5')
    data = data[data['delYn'] == 'N']

    initData['delYn'] = 'N'

    for l in range(0, len(fResultFin)):
        print(str(l) + ' / ' + str(len(fResultFin)))
        initData['delYn'].iloc[fResultFin['vIndex'].iloc[l]] = 'Y'

    initData = initData[initData['delYn'] == 'N']

    data.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/" + name + "_com_loop" + str(paramLoop) + ".csv")

    fResultMid = fResultMid[(fResultMid['rBcnt'] > 0.8) & (fResultMid['rDcnt'] >= limitCnt)]

    # 마지막 루프 돌때 마지막 루프에서 계산된 리미트 카운트보다 큰 녀석들 중 바이가 0인 애들을 모아서 원래 11년 데이터에서 빼낸 후
    # 바이가 0인 애들을 제외한 조건들을 전부다 다시 대입하여 dvsb가 3 이상인 녀석들은 추가시켜서 최종 결과를 만들어낸다..
    for h in range(0, len(fResultMid)):
        lvindex, lvdcnt, lvbcnt, lvdvsb, lvData = checkCondition(initData, fResultMid.iloc[h]['condi'])
        print(str(h) + ' / ' + str(len(fResultMid)) + ' / originalDvsb : ' + str(
            fResultMid['dvsb'].iloc[h]) + ' / dvsb : ' + str(lvdvsb) + ' / dcnt : ' + str(lvdcnt) + ' / bcnt : ' + str(
            lvbcnt))
        if lvdvsb > 2:
            fResultFin = fResultFin.append(fResultMid.iloc[h])

    fResultFin = fResultFin.sort_values('rDvsb', ascending=False)  # dvsb가 좋은 순서로 정렬
    fResultFin.to_csv(path + '[Loop - {}]' + name + "_ddelTreeLoopFinal_result.csv".format(paramLoop))

    return fResultFin

def checkCondition(ckData, ckCond):
    # ckData, ckCond = lvl4Data, realFinal.iloc[0]['condi']
    try:
        ckCond = ckCond.replace('(','')
        ckCond = ckCond.replace(')', '')
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

def conditionMake(data, aggr_df, lvl, initCondition, prevBcnt, prevDcnt, branch, name, limitLvl, lvlNum, lastLimitRatio,
                  limitCnt, lastYn, initBranch, treeNm, loopCnt, paramMemoryYn):
    # data, aggr_df, lvl, initCondition, prevBcnt, prevDcnt, branch, name, limitLvl, lvlNum, lastLimitRatio, limitCnt, lastYn, initBranch, treeNm = \
    #     tmp[3], tmp[2], i, tmp[4], prevBcnt, prevDcnt, trees.iloc[row].branch, name, paramLevel, j, paramLastRatio, limitCnt, lastYn, tmp[6], trees.iloc[row].treeNm
    mod = sys.modules[__name__]
    initData = data
    initAggr = aggr_df
    tmpInvCon = ''

    tmpFinal = pd.DataFrame()

    exceptCon = pd.DataFrame()
    fResultT = pd.DataFrame()

    for u in range(branch * (lvlNum - 1) + 1, branch * lvlNum + 1):
        # for u in range(branch * (lvlNum - 1) + 1, 2):
        try:
            tmpExec = pd.DataFrame()
            conH = ''
            valH = ''
            calH = ''
            tmpBool = pd.DataFrame()
            tmpAggr = pd.DataFrame()
            tmpData = pd.DataFrame()
            tmpCompare = pd.DataFrame()
            
            # print(treeNm + ' / ' + str(u))
            if treeNm == 'tree1':
                if u % branch == 1:
                    tmpFinal = splitData(data, aggr_df, prevBcnt, prevDcnt, limitCnt)
                    firstBcnt, firstDcnt, tmpZeroCnt = 0, 0, 0

                    tmpExec = tmpFinal[(tmpFinal['bcnt'] < tmpFinal['prevBcnt'] * 0.78)].sort_values('dvsb', ascending=False).iloc[0]

                    firstBcnt = tmpExec.bcnt
                    firstDcnt = tmpExec.dcnt
                    firstCnt = 0

                    print('firstDcnt : ' + str(firstDcnt) + ' / firstBcnt : ' + str(firstBcnt))
                elif u % branch == 2:
                    if lvl == 1:
                        try:
                            if paramMemoryYn == 'N':
                                with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/"
                                        + name + 'New' + "/lvl_{}_{}.pkl".format(lvl, u - 1), "rb") as fr:
                                    tmpInv = pickle.load(fr)
                            else:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(lvl, u - 1))

                        except Exception as e:
                            tmpInv = pd.DataFrame()
                            pass

                        data = data.drop(tmpInv[3].index)
                        aggr_df = data['pur_gubn5']

                        tmpInvCon = ''
                        tmpInvCon = changeCondition(tmpInv[4])
                        tmpInvCon = '(' + tmpInvCon + ')'

                        prevInvDcnt = data['pur_gubn5'].value_counts()[0]
                        prevInvBcnt = data['pur_gubn5'].value_counts()[1]

                        tmpInvFinal = splitData(data, data['pur_gubn5'], prevInvBcnt, prevInvDcnt, limitCnt)

                        tmpExec = tmpInvFinal[tmpInvFinal['dcnt'] > tmpInvFinal['prevDcnt'] * 0.7 \
                                              ].sort_values('bcnt').iloc[0]
                    else:
                        tmpExec = tmpFinal[tmpFinal['dcnt'] > tmpFinal['prevDcnt'] * 0.7 \
                                           ].sort_values('bcnt').iloc[0]
                elif u % branch == 3:
                    if lvl == 1:
                        try:
                            if paramMemoryYn == 'N':
                                with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/"
                                        + name + 'New' + "/lvl_{}_{}.pkl".format(lvl, u - 1), "rb") as fr:
                                    tmpInv = pickle.load(fr)
                            else:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(lvl, u - 1))

                        except Exception as e:
                            tmpInv = pd.DataFrame()
                            pass

                        data = data.drop(tmpInv[3].index)
                        aggr_df = data['pur_gubn5']

                        tmpInvCon = ''
                        tmpInvCon = changeCondition(tmpInv[4])
                        tmpInvCon = '(' + tmpInvCon + ')'

                        prevInvDcnt = data['pur_gubn5'].value_counts()[0]
                        prevInvBcnt = data['pur_gubn5'].value_counts()[1]

                        tmpInvFinal = splitData(data, data['pur_gubn5'], prevInvBcnt, prevInvDcnt, limitCnt)

                        tmpExec = tmpInvFinal[(tmpInvFinal['rRat'] > 20) & (tmpInvFinal['bcnt'] < tmpInvFinal['prevBcnt'] * 0.8) \
                            ].sort_values('dcnt', ascending=False).iloc[0]
                    else:
                        tmpExec = tmpFinal[(tmpFinal['rRat'] > 20) & (tmpInvFinal['bcnt'] < tmpInvFinal['prevBcnt'] * 0.8)\
                            ].sort_values('dcnt', ascending=False).iloc[0]
                elif u % branch == 0:
                    tmpExec = \
                    tmpFinal[tmpFinal['bcnt'] <= (tmpFinal['prevBcnt'] + tmpFinal['prevDcnt']) * 0.25].sort_values(
                        'dcnt', ascending=False).iloc[0]

            elif treeNm == 'tree2':
                if u % branch == 1:
                    tmpFinal = splitData(data, aggr_df, prevBcnt, prevDcnt, limitCnt)
                    firstBcnt, firstDcnt, tmpZeroCnt = 0, 0, 0

                    tmpExec = tmpFinal[tmpFinal['bcnt'] + tmpFinal['dcnt']
                                       > (tmpFinal['prevBcnt'] + tmpFinal['prevDcnt']) * 0.3 \
                                       ].sort_values('dvsb', ascending=False).iloc[0]

                    firstBcnt = tmpExec.bcnt
                    firstDcnt = tmpExec.dcnt
                    firstCnt = 0
                    print('firstDcnt : ' + str(firstDcnt) + ' / firstBcnt : ' + str(firstBcnt))
                elif u % branch == 2:
                    if lvl == 1:
                        try:
                            if paramMemoryYn == 'N':
                                with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/"
                                        + name + 'New' + "/lvl_{}_{}.pkl".format(lvl, u - 1), "rb") as fr:
                                    tmpInv = pickle.load(fr)
                            else:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(lvl, u - 1))

                        except Exception as e:
                            tmpInv = pd.DataFrame()
                            pass

                        data = data.drop(tmpInv[3].index)
                        aggr_df = data['pur_gubn5']

                        tmpInvCon = ''
                        tmpInvCon = changeCondition(tmpInv[4])
                        tmpInvCon = '(' + tmpInvCon + ')'

                        prevInvDcnt = data['pur_gubn5'].value_counts()[0]
                        prevInvBcnt = data['pur_gubn5'].value_counts()[1]

                        tmpInvFinal = splitData(data, data['pur_gubn5'], prevInvBcnt, prevInvDcnt, limitCnt)

                        tmpExec = tmpInvFinal.sort_values('dcnt', ascending=False).iloc[0]
                    else:
                        tmpExec = tmpFinal[tmpFinal['dcnt'] > tmpFinal['prevDcnt'] * 0.8].sort_values('dvsb',
                                                                                                      ascending=False).iloc[
                            0]
                elif u % branch == 3:
                    tmpExec = tmpFinal[tmpFinal['bcnt'] + tmpFinal['dcnt']
                                       < (tmpFinal['prevBcnt'] + tmpFinal['prevDcnt']) * 0.8 \
                                       ].sort_values('dvsb', ascending=False).iloc[2]
                elif u % branch == 0:
                    if lvl == 1:
                        try:
                            if paramMemoryYn == 'N':
                                with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/"
                                        + name + 'New' + "/lvl_{}_{}.pkl".format(lvl, u - 1), "rb") as fr:
                                    tmpInv = pickle.load(fr)
                            else:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(lvl, u - 1))

                        except Exception as e:
                            tmpInv = pd.DataFrame()
                            pass

                        data = data.drop(tmpInv[3].index)
                        aggr_df = data['pur_gubn5']

                        tmpInvCon = ''
                        tmpInvCon = changeCondition(tmpInv[4])
                        tmpInvCon = '(' + tmpInvCon + ')'

                        prevInvDcnt = data['pur_gubn5'].value_counts()[0]
                        prevInvBcnt = data['pur_gubn5'].value_counts()[1]

                        tmpInvFinal = splitData(data, data['pur_gubn5'], prevInvBcnt, prevInvDcnt, limitCnt)

                        tmpExec = tmpInvFinal[tmpInvFinal['prevBcnt'] > 0.8 \
                                              ].sort_values('dcnt', ascending=False).iloc[0]
                    else:
                        tmpExec = tmpFinal[tmpFinal['dcnt'] > limitCnt \
                                           ].sort_values('dvsb', ascending=False).iloc[0]
            elif treeNm == 'tree3':
                if u % branch == 1:
                    tmpFinal = splitData(data, aggr_df, prevBcnt, prevDcnt, limitCnt)
                    firstBcnt, firstDcnt, tmpZeroCnt = 0, 0, 0

                    tmpExec = tmpFinal[tmpFinal['bcnt'] + tmpFinal['dcnt']
                                       > (tmpFinal['prevBcnt'] + tmpFinal['prevDcnt']) * 0.4 \
                                       ].sort_values('dvsb', ascending=False).iloc[0]

                    firstBcnt = tmpExec.bcnt
                    firstDcnt = tmpExec.dcnt
                    firstCnt = 0
                    print('firstDcnt : ' + str(firstDcnt) + ' / firstBcnt : ' + str(firstBcnt))
                elif u % branch == 2:
                    tmpExec = \
                    tmpFinal[(tmpFinal['dcnt'] > tmpFinal['prevDcnt'] * 0.8)].sort_values('rRat', ascending=False).iloc[
                        0]
                elif u % branch == 3:
                    tmpExec = tmpFinal[(tmpFinal['rRat'] > 20) & \
                                       (tmpFinal['dcnt'] > tmpFinal['prevDcnt'] * 0.2)].sort_values('dvsb',
                                                                                                    ascending=False).iloc[
                        2]
                elif u % branch == 0:
                    tmpExec = \
                    tmpFinal[(tmpFinal['bcnt'] < tmpFinal['prevBcnt'] * 0.9)].sort_values('rGr', ascending=False).iloc[
                        0]
            elif treeNm == 'tree4':
                if u % branch == 1:
                    tmpFinal = splitData(data, aggr_df, prevBcnt, prevDcnt, limitCnt)
                    firstBcnt, firstDcnt, tmpZeroCnt = 0, 0, 0
                    

                    tmpExec = tmpFinal[(tmpFinal['bcnt'] < tmpFinal['prevBcnt'] * 0.8) & (
                                tmpFinal['dcnt'] > tmpFinal['prevDcnt'] * 0.6) \
                                       ].sort_values('dvsb', ascending=False).iloc[1]

                    firstBcnt = tmpExec.bcnt
                    firstDcnt = tmpExec.dcnt
                    firstCnt = 0
                    print('firstDcnt : ' + str(firstDcnt) + ' / firstBcnt : ' + str(firstBcnt))
                elif u % branch == 2:
                    tmpExec = tmpFinal[(tmpFinal['dcnt'] > tmpFinal['prevDcnt'] * 0.5)].sort_values('rrr', ascending=False).iloc[2]
                elif u % branch == 3:
                    tmpExec = tmpFinal[(tmpFinal['bcnt'] < tmpFinal['prevBcnt'] * 0.95) & (
                                tmpFinal['dcnt'] > tmpFinal['prevDcnt'] * 0.7)].sort_values('rRat',
                                                                                            ascending=False).iloc[1]
                elif u % branch == 0:
                    if lvl == 1:
                        try:
                            if paramMemoryYn == 'N':
                                with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/"
                                        + name + 'New' + "/lvl_{}_{}.pkl".format(lvl, u - 1), "rb") as fr:
                                    tmpInv = pickle.load(fr)
                            else:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(lvl, u - 1))

                        except Exception as e:
                            tmpInv = pd.DataFrame()
                            pass

                        data = data.drop(tmpInv[3].index)
                        aggr_df = data['pur_gubn5']

                        tmpInvCon = ''
                        tmpInvCon = changeCondition(tmpInv[4])
                        tmpInvCon = '(' + tmpInvCon + ')'

                        prevInvDcnt = data['pur_gubn5'].value_counts()[0]
                        prevInvBcnt = data['pur_gubn5'].value_counts()[1]

                        tmpInvFinal = splitData(data, data['pur_gubn5'], prevInvBcnt, prevInvDcnt, limitCnt)

                        tmpExec = tmpInvFinal[(tmpInvFinal['dcnt'] > tmpInvFinal['prevDcnt'] * 0.4)].sort_values('dcnt',
                                                                                                                 ascending=False).iloc[
                            0]
                    else:
                        tmpExec = tmpFinal[(tmpFinal['dcnt'] > tmpFinal['prevDcnt'] * 0.4)].sort_values('rGr',
                                                                                                        ascending=False).iloc[
                            0]
            elif treeNm == 'tree5':
                if u % branch == 1:
                    tmpFinal = splitData(data, aggr_df, prevBcnt, prevDcnt, limitCnt)
                    firstBcnt, firstDcnt, tmpZeroCnt = 0, 0, 0

                    tmpExec = tmpFinal[tmpFinal['rRat'] > 20]

                    tmpFinalChk = tmpFinal[tmpFinal['rRt'] > tmpFinal['dvsb']]

                    if tmpFinalChk['dvsb'].count() > 0:
                        tmpExec = \
                        tmpFinal[tmpFinal['bcnt'] < tmpFinal['prevBcnt'] * 0.9].sort_values('rRt',
                                                                                                               ascending=False).iloc[
                            0]
                    else:
                        tmpExec = \
                        tmpFinal[tmpFinal['bcnt'] < tmpFinal['prevBcnt'] * 0.9].sort_values('sRt',
                                                                                                               ascending=False).iloc[
                            0]

                    firstBcnt = tmpExec.bcnt
                    firstDcnt = tmpExec.dcnt
                    firstCnt = 0
                    print('firstDcnt : ' + str(firstDcnt) + ' / firstBcnt : ' + str(firstBcnt))
                elif u % branch == 2:
                    tmpExec = tmpFinal[tmpFinal['dcnt']
                                       > tmpFinal['prevDcnt'] * 0.7 \
                                       ].sort_values('dvsb', ascending=False).iloc[2]
                elif u % branch == 3:
                    tmpExec = tmpFinal[tmpFinal['bcnt'] + tmpFinal['dcnt']
                                       > (tmpFinal['prevBcnt'] + tmpFinal['prevDcnt']) * 0.23].sort_values('bcnt').iloc[
                        0]
                elif u % branch == 0:
                    tmpExec = \
                    tmpFinal[(tmpFinal['bcnt'] < tmpFinal['prevBcnt'] * 0.8)].sort_values('fRt', ascending=False).iloc[
                        1]
            elif treeNm == 'tree6':
                if u % branch == 1:
                    tmpFinal = splitData(data, aggr_df, prevBcnt, prevDcnt, limitCnt)
                    firstBcnt, firstDcnt, tmpZeroCnt = 0, 0, 0

                    if lvl > 0 and lvl <= 3:
                        tmpFinal = tmpFinal[tmpFinal['rRat'] > 45]
                    elif lvl > 3 and lvl <= 6:
                        tmpFinal = tmpFinal[tmpFinal['rRat'] > 35]
                    elif lvl > 6 and lvl <= 9:
                        tmpFinal = tmpFinal[tmpFinal['rRat'] > 25]

                    tmpExec = \
                    tmpFinal[(tmpFinal['bcnt'] < tmpFinal['prevBcnt'] * 0.8)].sort_values('rRt', ascending=False).iloc[
                        0]

                    firstBcnt = tmpExec.bcnt
                    firstDcnt = tmpExec.dcnt
                elif u % branch == 0:
                    if lvl == 1:
                        try:
                            if paramMemoryYn == 'N':
                                with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/"
                                        + name + 'New' + "/lvl_{}_{}.pkl".format(lvl, u - 1), "rb") as fr:
                                    tmpInv = pickle.load(fr)
                            else:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(lvl, u - 1))

                        except Exception as e:
                            tmpInv = pd.DataFrame()
                            pass

                        data = data.drop(tmpInv[3].index)
                        aggr_df = data['pur_gubn5']

                        tmpInvCon = ''
                        tmpInvCon = changeCondition(tmpInv[4])
                        tmpInvCon = '(' + tmpInvCon + ')'

                        prevInvDcnt = data['pur_gubn5'].value_counts()[0]
                        prevInvBcnt = data['pur_gubn5'].value_counts()[1]

                        tmpInvFinal = splitData(data, data['pur_gubn5'], prevInvBcnt, prevInvDcnt, limitCnt)

                        if lvl > 0 and lvl <= 3:
                            tmpInvFinal = tmpInvFinal[tmpInvFinal['rRat'] > 45]
                        elif lvl > 3 and lvl <= 6:
                            tmpInvFinal = tmpInvFinal[tmpInvFinal['rRat'] > 35]
                        elif lvl > 6 and lvl <= 9:
                            tmpInvFinal = tmpInvFinal[tmpInvFinal['rRat'] > 25]

                        tmpExec = tmpInvFinal[(tmpInvFinal['bcnt'] < tmpInvFinal['prevBcnt'] * 0.8)].sort_values('sRt',
                                                                                                           ascending=False).iloc[
                            0]
                    else:
                        tmpExec = tmpFinal[(tmpFinal['bcnt'] < tmpFinal['prevBcnt'] * 0.8)].sort_values('sRt',
                                                                                                        ascending=False).iloc[
                            0]
            elif treeNm == 'tree7':
                if u % branch == 1:
                    tmpFinal = splitData(data, aggr_df, prevBcnt, prevDcnt, limitCnt)
                    firstBcnt, firstDcnt, tmpZeroCnt = 0, 0, 0

                    if lvl > 0 and lvl <= 2:
                        tmpFinal = tmpFinal[tmpFinal['rRat'] > 40]
                    elif lvl > 2 and lvl <= 4:
                        tmpFinal = tmpFinal[tmpFinal['rRat'] > 30]
                    elif lvl > 4 and lvl <= 6:
                        tmpFinal = tmpFinal[tmpFinal['rRat'] > 20]
                    elif lvl > 6:
                        tmpFinal = tmpFinal[tmpFinal['rRat'] > 7]

                    tmpExec = \
                    tmpFinal[(tmpFinal['bcnt'] < tmpFinal['prevBcnt'] * 0.7)].sort_values('dcnt', ascending=False).iloc[
                        0]

                    firstBcnt = tmpExec.bcnt
                    firstDcnt = tmpExec.dcnt

                    print('firstDcnt : ' + str(firstDcnt) + ' / firstBcnt : ' + str(firstBcnt))
                elif u % branch == 0:
                    if lvl == 1:
                        try:
                            if paramMemoryYn == 'N':
                                with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/"
                                        + name + 'New' + "/lvl_{}_{}.pkl".format(lvl, u - 1), "rb") as fr:
                                    tmpInv = pickle.load(fr)
                            else:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(lvl, u - 1))

                        except Exception as e:
                            tmpInv = pd.DataFrame()
                            pass

                        data = data.drop(tmpInv[3].index)
                        aggr_df = data['pur_gubn5']

                        tmpInvCon = ''
                        tmpInvCon = changeCondition(tmpInv[4])
                        tmpInvCon = '(' + tmpInvCon + ')'

                        prevInvDcnt = data['pur_gubn5'].value_counts()[0]
                        prevInvBcnt = data['pur_gubn5'].value_counts()[1]

                        tmpInvFinal = splitData(data, data['pur_gubn5'], prevInvBcnt, prevInvDcnt, limitCnt)

                        if lvl > 0 and lvl <= 2:
                            tmpInvFinal = tmpInvFinal[tmpInvFinal['rRat'] > 35]
                        elif lvl > 2 and lvl <= 4:
                            tmpInvFinal = tmpInvFinal[tmpInvFinal['rRat'] > 25]
                        elif lvl > 4 and lvl <= 6:
                            tmpInvFinal = tmpInvFinal[tmpInvFinal['rRat'] > 15]
                        elif lvl > 6:
                            tmpInvFinal = tmpInvFinal[tmpInvFinal['rRat'] > 5]

                        tmpExec = tmpInvFinal[(tmpInvFinal['bcnt'] < tmpInvFinal['prevBcnt'] * 0.75)].sort_values('rGr', ascending=False).iloc[0]
                    else:
                        tmpExec = tmpFinal[(tmpFinal['bcnt'] < tmpFinal['prevBcnt'] * 0.75)].sort_values('rGr', ascending=False).iloc[0]
            elif treeNm == 'tree8':
                if u % branch == 1:
                    tmpFinal = splitData(data, aggr_df, prevBcnt, prevDcnt, limitCnt)
                    firstBcnt, firstDcnt, tmpZeroCnt = 0, 0, 0

                    tmpExec = tmpFinal[tmpFinal['bcnt']
                                       < tmpFinal['prevBcnt'] * 0.8].sort_values('dcnt', ascending=False).iloc[4]

                    firstBcnt = tmpExec.bcnt
                    firstDcnt = tmpExec.dcnt
                    firstCnt = 0
                    print('firstDcnt : ' + str(firstDcnt) + ' / firstBcnt : ' + str(firstBcnt))
                elif u % branch == 2:
                    if lvl > 0 and lvl <= 2:
                        tmpFinal = tmpFinal[tmpFinal['rRat'] > 38]
                    elif lvl > 2 and lvl <= 4:
                        tmpFinal = tmpFinal[tmpFinal['rRat'] > 28]
                    elif lvl > 4 and lvl <= 6:
                        tmpFinal = tmpFinal[tmpFinal['rRat'] > 18]
                    elif lvl > 6:
                        tmpFinal = tmpFinal[tmpFinal['rRat'] > 5]

                    tmpExec = tmpFinal.sort_values('fRt', ascending=False).iloc[3]
                elif u % branch == 0:
                    tmpFinal = tmpFinal[tmpFinal['rRat'] > 10]

                    tmpFinalChk = tmpFinal[tmpFinal['rRt'] > tmpFinal['dvsb']]

                    if tmpFinalChk['dvsb'].count() > 0:
                        tmpExec = tmpFinal.sort_values('rGr', ascending=False).iloc[4]
                    else:
                        tmpExec = tmpFinal.sort_values('sRt', ascending=False).iloc[4]
            elif treeNm == 'tree9':
                if u % branch == 1:
                    tmpFinal = splitData(data, aggr_df, prevBcnt, prevDcnt, limitCnt)
                    firstBcnt, firstDcnt, tmpZeroCnt = 0, 0, 0

                    tmpExec = tmpFinal[tmpFinal['bcnt'] + tmpFinal['dcnt']
                                       > (tmpFinal['prevBcnt'] + tmpFinal['prevDcnt']) * 0.1].sort_values('dvsb',
                                                                                                          ascending=False).iloc[
                        0]

                    firstBcnt = tmpExec.bcnt
                    firstDcnt = tmpExec.dcnt
                    firstCnt = 0
                    print('firstDcnt : ' + str(firstDcnt) + ' / firstBcnt : ' + str(firstBcnt))
                elif u % branch == 2:
                    tmpFinal = tmpFinal[tmpFinal['rRat'] > 10]

                    tmpFinalChk = tmpFinal[tmpFinal['rRt'] > tmpFinal['dvsb']]
                    #
                    if tmpFinalChk['dvsb'].count() > 0:
                        tmpExec = tmpFinal[tmpFinal['dcnt'] > tmpFinal['prevDcnt'] * 0.7].sort_values('rRt', ascending=False).iloc[4]
                    else:
                        tmpExec = tmpFinal[tmpFinal['dcnt'] > tmpFinal['prevDcnt'] * 0.7].sort_values('sRt', ascending=False).iloc[4]
                elif u % branch == 3:
                    if lvl == 1:
                        try:
                            if paramMemoryYn == 'N':
                                with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/"
                                        + name + 'New' + "/lvl_{}_{}.pkl".format(lvl, u - 1), "rb") as fr:
                                    tmpInv = pickle.load(fr)
                            else:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(lvl, u - 1))

                        except Exception as e:
                            tmpInv = pd.DataFrame()
                            pass

                        data = data.drop(tmpInv[3].index)
                        aggr_df = data['pur_gubn5']

                        tmpInvCon = ''
                        tmpInvCon = changeCondition(tmpInv[4])
                        tmpInvCon = '(' + tmpInvCon + ')'

                        prevInvDcnt = data['pur_gubn5'].value_counts()[0]
                        prevInvBcnt = data['pur_gubn5'].value_counts()[1]

                        tmpInvFinal = splitData(data, data['pur_gubn5'], prevInvBcnt, prevInvDcnt, limitCnt)

                        if lvl > 0 and lvl <= 2:
                            tmpInvFinal = tmpInvFinal[tmpInvFinal['rRat'] > 35]
                        elif lvl > 2 and lvl <= 4:
                            tmpInvFinal = tmpInvFinal[tmpInvFinal['rRat'] > 25]
                        elif lvl > 4 and lvl <= 6:
                            tmpInvFinal = tmpInvFinal[tmpInvFinal['rRat'] > 15]
                        elif lvl > 6:
                            tmpInvFinal = tmpInvFinal[tmpInvFinal['rRat'] > 5]

                        tmpExec = tmpInvFinal[tmpInvFinal['sRt'] > tmpInvFinal['sRt'].mean()].sort_values('dcnt',
                                                                                                          ascending=False).iloc[
                            0]
                    else:
                        tmpExec = \
                        tmpFinal[tmpFinal['sRt'] > tmpFinal['sRt'].mean()].sort_values('dcnt', ascending=False).iloc[0]
                elif u % branch == 0:
                    tmpExec = tmpFinal[tmpFinal['bcnt'] < tmpFinal['prevBcnt'] * 0.9].sort_values('rGr', ascending=False).iloc[0]

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

                if len(tmpInvCon) > 0:
                    condition = condition + ' AND ' + tmpInvCon

                tmpInvCon = ''

                fBranch = str(initBranch) + '/' + str(u % branch)

                tmpAggr = aggr_df[tmpBool]
                tmpData = data[tmpBool]
                tmpEntr = entropy(tmpAggr)

                print("[" + datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
                      + '] lvl'
                      + str(lvl) + "_" + str(u)
                      + "\ndvsb : " + str(round(tmpExec['dvsb'], 2))
                      + " / dcnt : " + str(int(tmpExec['dcnt']))
                      + " / bcnt : " + str(int(tmpExec['bcnt']))
                      + " / entropy : " + str(round(tmpEntr, 2))
                      + " / condition : " + condition)

                # 선택된 컬럼 삭제
                try:
                    tmpFinal = tmpFinal[~(tmpFinal['condi'] == conH)]
                except:
                    pass

                if (lvl <= 2 and int(tmpExec['bcnt']) > 0.8) or \
                    (  # lvl3 ~ lvl5
                    lvl > 2 and lvl <= limitLvl - 3 and int(tmpExec['bcnt']) > 0.8
                    and tmpExec['dvsb'] > (prevDcnt / prevBcnt) and tmpExec['dcnt'] >= limitCnt
                    ) or (
                    lvl == limitLvl - 2 and int(tmpExec['bcnt']) > 0.8
                    and tmpExec['dvsb'] > (prevDcnt / prevBcnt) and tmpExec['dcnt'] >= limitCnt and tmpExec[
                        'bcnt'] <= 40
                    ) or (
                    lvl == limitLvl - 1 and int(tmpExec['bcnt']) > 0.8
                    and tmpExec['dvsb'] >= 2.5 and tmpExec['dcnt'] >= limitCnt
                    ):
                    print('lvl_{}_{}.pkl'.format(lvl, u) + ' pickle file create')

                    if paramMemoryYn == 'N':
                        with open('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/'
                                + name + 'New'
                                + '/lvl_{}_{}.pkl'.format(lvl, u)
                                , 'wb') as f:
                            pickle.dump([tmpFinal, tmpBool, tmpAggr, tmpData, condition, tmpEntr, fBranch, tmpData.index],
                                        f)
                    else:
                        setattr(mod, 'lvl_{}_{}'.format(lvl, u), [tmpFinal, tmpBool, tmpAggr, tmpData, condition, tmpEntr, fBranch, tmpData.index])

                try:
                    fResultT = fResultT.append(
                        pd.DataFrame(
                            [(loopCnt, treeNm, 'lvl_' + str(lvl) + '_' + str(u), condition, str(int(tmpExec['dcnt'])),
                              str(int(tmpExec['bcnt'])), tmpExec['dcnt'] / tmpExec['bcnt'],
                              tmpExec['bcnt'] / tmpExec['dcnt'], str(round(tmpEntr, 2)), fBranch, tmpData.index)],
                            columns=['loop', 'tree', 'lvl', 'condi', 'dcnt', 'bcnt', 'dvsb', 'bvsd', 'entr', 'branch', 'index']))

                except Exception as e:
                    pass
                
                # 전개한 가지내에 bcnt가 0인 녀석이 있는지 카운트함
                if tmpExec['bcnt'] < 1:
                    tmpZeroCnt = tmpZeroCnt + 1
                
                # original - 14:3  / 1 - 12:0  / etc - 14:1
                # 전개한 가지 내에 dcnt가 0인 녀석이 존재하면 tmpCompare에 넣고 다른 가지들을 체크한다..
                if u % branch == 0 and tmpZeroCnt > 0:
                    tmpCountsDf = pd.DataFrame()
                    for p in range(branch * (lvlNum - 1) + 1, branch * lvlNum + 1):
                        print('lvl_{}_{}'.format(lvl, p) + '- Delete')
                        if paramMemoryYn == 'N':
                            if os.path.isfile('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/'
                                + name + 'New'
                                + '/lvl_{}_{}.pkl'.format(lvl, p)):
                                os.remove('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/'
                                + name + 'New'
                                + '/lvl_{}_{}.pkl'.format(lvl, p))
                        else:
                            delattr(mod, 'lvl_{}_{}'.format(lvl, p))
                
                # 6레벨부터 전개한 가지 내에 dcnt의 변화가 없을 경우 bcnt가 가장 적은 것들만 남긴다. 
                if u % branch == 0 and lvl >= 5:
                    tmpDcntDf = pd.DataFrame()

                    tmpDcntDf = fResultT

                    tmpDcntDf = tmpDcntDf.astype(
                        {'dcnt': 'int', 'bcnt': 'int', 'bvsd': 'float', 'dvsb': 'float', 'entr': 'float'})

                    tmpDcntDf = tmpDcntDf[(tmpDcntDf['bcnt'] > 0) & (tmpDcntDf['dcnt'] > 0)]
                    tmpDcntDf['dcnt'][tmpDcntDf['dcnt'] == 0] = 0.8
                    tmpDcntDf['dvsb'] = tmpDcntDf['dcnt'] / tmpDcntDf['bcnt']

                    if int(tmpDcntDf.sort_values('dcnt', ascending=False).iloc[0]['dcnt']) == prevDcnt:
                        # 상위 레벨의 dcnt 감소 없을 경우 상위레벨의 dcnt보다 감소한 가지나 상위레벨의 dcnt와 같고 bcnt가 더 큰 녀석들은 tmpDcntDf에 넣고 삭제
                        tmpDcntDf = tmpDcntDf[(tmpDcntDf['dvsb'] <= tmpDcntDf.sort_values('dvsb', ascending=False).iloc[0]['dvsb'])]

                        tmpDcntDf = tmpDcntDf.sort_values('dvsb', ascending=False)
                        tmpDcntDf = tmpDcntDf.iloc[1:]

                        for q in tmpDcntDf['lvl']:
                            print('{}'.format(q) + ' - Delete')
                            if paramMemoryYn == 'N':
                                if os.path.isfile('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/'
                                    + name + 'New'
                                    + '/{}.pkl'.format(q)):
                                    os.remove('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/'
                                    + name + 'New'
                                    + '/{}.pkl'.format(q))
                            else:
                                delattr(mod, '{}'.format(q))

        except Exception as e:
            # print(e)
            fResultT = fResultT.append(
                pd.DataFrame([(loopCnt, treeNm, 'lvl_' + str(lvl) + '_' + str(u), '', '0',
                               '0', '0', '0', '0', 'X', '')],
                             columns=['loop', 'tree', 'lvl', 'condi', 'dcnt', 'bcnt', 'dvsb', 'bvsd', 'entr',
                                      'branch', 'index']))
            pass

    return fResultT

def makeLevel(vLoop, paramLevel, paramLastRatio, limitCnt, name, data, initCond, lastYn, paramMemoryYn):
    # vLoop, paramLevel, paramLastRatio, limitCnt, name, data, initCond, lastYn = 0, paramLevel, paramLastRatio, limitCnt, name, data, '', 'N'
    tmpMkL = pd.DataFrame()

    trees = pd.DataFrame([('tree1', 4)], columns=('treeNm', 'branch'))
    # trees = pd.DataFrame([('tree4', 4)], columns=('treeNm', 'branch'))
    trees = trees.append(pd.DataFrame([('tree2', 4)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree3', 4)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree4', 4)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree5', 4)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree6', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree7', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree8', 3)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree9', 4)], columns=('treeNm', 'branch')))
    # row,i,j = 0,1,1
    for row in range(0, trees.shape[0]):
        for i in range(0, paramLevel + 1):
            if i == 0:
                if paramMemoryYn == 'N':
                    ## Save pickle
                    with open('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + 'New' + '/lvl_0_1.pkl',
                            'wb') as f:
                        pickle.dump(['', '', data['pur_gubn5'], data, initCond, entropy(data['pur_gubn5']), '', data.index], f)
                else:
                    mod = sys.modules[__name__]
                    setattr(mod, 'lvl_0_1', ['', '', data['pur_gubn5'], data, initCond, entropy(data['pur_gubn5']), '', data.index])

            else:
                leaf = trees.iloc[row].branch ** (i - 1)
                # i,j = 1,1
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
                        if paramMemoryYn == 'N':
                            with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/"
                                    + name + 'New' + "/lvl_{}_{}.pkl".format(i - 1, j), "rb") as fr:
                                tmp = pickle.load(fr)
                        else:
                            tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j))
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
                        print("[Loop - " + str(vLoop) + "][" + name + "][MD][" + trees.iloc[
                            row].treeNm + "][" + datetime.datetime.today().strftime(
                            "%Y-%m-%d %H:%M:%S") + '] sourceData : lvl_' + str(i - 1) + '_' + str(j)
                              + " / dvsb : " + str(round(prevDcnt / prevBcnt, 2))
                              + " / bvsd : " + str(round(prevBcnt / prevDcnt, 2))
                              + " / prevBcnt : "
                              + str(prevBcnt) + " / prevDcnt : " + str(prevDcnt) + " / entropy : " + str(
                            round(entropy(tmp[3]['pur_gubn5']), 2))
                              )
                        print(
                            '######################################################################################################################################')

                        if (i <= 6 and prevBcnt > 0.8 and prevDcnt > limitCnt) or \
                                (i > 6 and prevBcnt > 0.8 and prevDcnt > limitCnt and prevDcnt / prevBcnt > 1.2):
                            tmpFF = conditionMake(tmp[3], tmp[2], i, tmp[4], prevBcnt, prevDcnt, trees.iloc[row].branch,
                                                  name,
                                                  paramLevel, j,
                                                  paramLastRatio, limitCnt, lastYn, tmp[6], trees.iloc[row].treeNm,
                                                  vLoop, paramMemoryYn)
                            tmpMkL = tmpMkL.append(tmpFF)

                        try:
                            if i > 1:
                                print('delete pickle - lvl_{}_{}.pkl'.format(i - 1, j))
                                # print(e)
                                if paramMemoryYn == 'N':
                                    if os.path.isfile(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + 'New' + '/lvl_{}_{}.pkl'.format(
                                                i - 1, j)):
                                        os.remove(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + 'New' + '/lvl_{}_{}.pkl'.format(
                                                i - 1, j))
                                else:
                                    delattr(mod,'/lvl_{}_{}.pkl'.format(i - 1, j))


                        except Exception as e:
                            # print(e)
                            pass


                    except Exception as e:
                        prevDcnt = 0.8
                        pass

        try:
            tmpMkL.to_csv(
                "C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/[Loop - " + str(vLoop) + "][MD][" +
                trees.iloc[row].treeNm + "]" + name + "_result.csv")
        except:
            pass
    return tmpMkL
