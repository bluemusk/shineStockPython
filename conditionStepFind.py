# coding=utf-8
import pandas as pd
import sys
import datetime
import warnings

warnings.filterwarnings('ignore')


def stepFunction(col, aggr_df):
    # col = data.iloc[:,20]
    # aggr_df = data.iloc[:,4]

    mod = sys.modules[__name__]

    setattr(mod, 'num_0', round(col.quantile(0), 1))
    setattr(mod, 'num_1', round(col.quantile(0.25), 1))
    setattr(mod, 'num_2', round(col.quantile(0.5), 1))
    setattr(mod, 'num_3', round(col.quantile(0.75), 1))
    setattr(mod, 'num_4', round(col.quantile(1), 1))

    num_0 = round(col.quantile(0), 1)
    num_1 = round(col.quantile(0.25), 1)
    num_2 = round(col.quantile(0.5), 1)
    num_3 = round(col.quantile(0.75), 1)
    num_4 = round(col.quantile(1), 1)

    setattr(mod, 'step1', round((num_1 - num_0) / 10, 1))
    setattr(mod, 'step2', round((num_2 - num_1) / 10, 1))
    setattr(mod, 'step3', round((num_3 - num_2) / 10, 1))
    setattr(mod, 'step4', round((num_4 - num_3) / 10, 1))

    finalSet = pd.DataFrame()

    for j in range(0, 4):
        for k in range(0, 10):
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
                                                     'dvsb': [dcnt / bcnt],
                                                     'cal':['GT'],
                                                     'col':[str(col.name)],
                                                     'chkYn':[''],
                                                     'chkNum':[0]
                                                     }, index=[col.name])
                                       )

    for j in range(0, 4):
        for k in range(0, 10):
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
                                                     'dvsb': [dcnt / bcnt],
                                                     'cal': ['LT'],
                                                     'col': [str(col.name)],
                                                     'chkYn': [''],
                                                     'chkNum': [0]
                                                     }, index=[col.name])
                                       )

    return finalSet

def similarDel(result):
    resultGt = result[(result['cal'] == 'GT')].sort_values(by=["col", "bcnt"],
                                                           ascending=[False, False]).drop_duplicates()
    resultLt = result[(result['cal'] == 'LT')].sort_values(by=["col", "bcnt"],
                                                           ascending=[False, False]).drop_duplicates()

    finalSet = pd.DataFrame()

    for y in data.iloc[:, 14:data.shape[1]].columns:
        # for y in data.iloc[:, 14:50].columns:
        x = resultGt[(resultGt['col'] == y)]
        # x = resultGt[(resultGt['col'] == 'macd')]
        chkNum = 0
        print("[" + datetime.datetime.today().strftime(
            "%Y-%m-%d %H:%M:%S") + "]condi : " + y)
        for j in range(0, x.shape[0] - 1):
            if chkNum > 5 or j == 0:
                chkNum = 0

            chkNum = chkNum + (x.iloc[j].bcnt - x.iloc[j + 1].bcnt) / x.iloc[j].bcnt * 100

            if chkNum > 5:
                x['chkYn'][j] = 'Y'
            else:
                x['chkYn'][j] = 'N'

            finalSet = finalSet.append(x.iloc[j])

    for d in data.iloc[:, 14:data.shape[1]].columns:
        # for d in data.iloc[:, 14:50].columns:
        z = resultLt[(resultLt['col'] == d)]
        print("[" + datetime.datetime.today().strftime(
            "%Y-%m-%d %H:%M:%S") + "]condi : " + d)
        chkNum = 0

        for j in range(0, z.shape[0] - 1):
            if chkNum > 5 or j == 0:
                chkNum = 0

            chkNum = chkNum + (z.iloc[j].bcnt - z.iloc[j + 1].bcnt) / z.iloc[j].bcnt * 100

            if chkNum > 5:
                z['chkYn'][j] = 'Y'
            else:
                z['chkYn'][j] = 'N'
            # print((z.iloc[j].bcnt - z.iloc[j+1].bcnt) / z.iloc[j].bcnt * 100)
            # print(z['condi'][j])
            finalSet = finalSet.append(z.iloc[j])

    finalSet = finalSet[finalSet['chkYn'] == 'Y']

    return finalSet


######################################################################################################
# data Loading
data = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy1_close_h4_9year.csv")
result = pd.DataFrame()
# 컬럼 별 구간 찾기 및 비슷한 구간은 병합 +-5%
for i in range(14, data.shape[1]):
    result = result.append(stepFunction(data.iloc[:, i], data.iloc[:, 4]))
# 컬럼 별 구간 찾기 및 비슷한 구간은 병합 +-5%
final = similarDel(result)
# csv파일 생성
final.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy1_close_h4_9year_new_20211003.csv")
######################################################################################################
# data Loading
data = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy2_close_h4_9year.csv")
result = pd.DataFrame()
# 컬럼 별 구간 찾기 및 비슷한 구간은 병합 +-5%
for i in range(14, data.shape[1]):
    result = result.append(stepFunction(data.iloc[:, i], data.iloc[:, 4]))
# 컬럼 별 구간 찾기 및 비슷한 구간은 병합 +-5%
final = similarDel(result)
# csv파일 생성
final.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy2_close_h4_9year_new_20211003.csv")
######################################################################################################
# data Loading
data = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy3_close_h4_9year.csv")
result = pd.DataFrame()
for i in range(14, data.shape[1]):
    result = result.append(stepFunction(data.iloc[:, i], data.iloc[:, 4]))
# 컬럼 별 구간 찾기 및 비슷한 구간은 병합 +-5%
final = similarDel(result)
# csv파일 생성
final.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy3_close_h4_9year_new_20211003.csv")
######################################################################################################
# data Loading
data = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy4_close_h4_9year.csv")
result = pd.DataFrame()
for i in range(14, data.shape[1]):
    result = result.append(stepFunction(data.iloc[:, i], data.iloc[:, 4]))
# 컬럼 별 구간 찾기 및 비슷한 구간은 병합 +-5%
final = similarDel(result)
# csv파일 생성
final.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy4_close_h4_9year_new_20211003.csv")