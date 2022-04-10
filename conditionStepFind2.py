

######################################################################################################
# coding=utf-8
import pandas as pd
import sys
import datetime
import warnings
######################################################################################################

######################################################################################################
warnings.filterwarnings('ignore')
######################################################################################################
# 데이터를 각 컬럼별로 구간을 구한다음 컬럼별로 +-5% 인 녀석들은 합쳐주는 프로그램입니다..
######################################################################################################

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

def similarDel(result, data):
    resultGt = result[(result['cal'] == 'GT')].sort_values(by=["col", "bcnt"],
                                                           ascending=[False, False]).drop_duplicates()
    resultLt = result[(result['cal'] == 'LT')].sort_values(by=["col", "bcnt"],
                                                           ascending=[False, False]).drop_duplicates()

    finalSet = pd.DataFrame()

    for y in data.iloc[:, 5:data.shape[1]].columns:
        # for y in data.iloc[:, 5:50].columns:
        x = resultGt[(resultGt['col'] == y)]
        # x = resultGt[(resultGt['col'] == 'macd')]
        chkNum = 0
        print("[" + datetime.datetime.today().strftime(
            "%Y-%m-%d %H:%M:%S") + "] condi : " + y)
        for j in range(0, x.shape[0] - 1):
            if chkNum > 5 or j == 0:
                chkNum = 0

            chkNum = chkNum + (x.iloc[j].bcnt - x.iloc[j + 1].bcnt) / x.iloc[j].bcnt * 100

            try:
                if chkNum > 5:
                    x['chkYn'][j] = 'Y'
                else:
                    x['chkYn'][j] = 'N'
            except Exception as e:
                x['chkYn'][j] = 'N'

            finalSet = finalSet.append(x.iloc[j])

    for d in data.iloc[:, 5:data.shape[1]].columns:
        # for d in data.iloc[:, 5:50].columns:
        z = resultLt[(resultLt['col'] == d)]
        print("[" + datetime.datetime.today().strftime(
            "%Y-%m-%d %H:%M:%S") + "] condi : " + d)
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

def run(dataPath, resultCsvFileName):
    # data Loading
    # dataPath = "C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy1_close_h4_9year.csv"
    # resultCsvFileName = "C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy1_close_h4_9year_new_20211003.csv"
    data = pd.read_csv(dataPath)
    # data = data.iloc[:,1:50]
    result = pd.DataFrame()

    for i in range(5, data.shape[1]):
        try:
            result = result.append(stepFunction(data.iloc[:, i], data.iloc[:, 4]))
        except Exception as e:
            pass
    # 컬럼 별 구간 찾기 및 비슷한 구간은 병합 +-5%
    final = similarDel(result, data)
    final.to_csv(resultCsvFileName)
    # csv파일 생성
    return final, data

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

def checkDcnt(initData, cond, condDcnt, condDvsb):
    # initData = data
    # condDcnt = 6
    # condDvsb = 0.6
    mod = sys.modules[__name__]

    if condDcnt > 0:
        cond = cond[cond['dcnt'] >= condDcnt]

    if condDvsb > 0:
        cond = cond[cond['dvsb'] >= condDvsb]

    cond = cond.sort_values(by='dvsb', ascending=False)
    cond = cond.reset_index(drop=True)

    setattr(mod, 'data_0', initData)

    finalConds = pd.DataFrame()
    for i in range(0, cond.shape[0]):
        # print(i)
        initBcnt = cond.iloc[i]['bcnt']
        initDcnt = cond.iloc[i]['dcnt']
        # print(cond.iloc[i]['condi'])
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

def run2(initData, cond, condDcnt, condDvsb, fileName):
    finalResult = checkDcnt(initData, cond, condDcnt, condDvsb)
    finalResult = finalResult.reset_index(drop=True)
    finalCond = ''

    for i in range(0,finalResult.shape[0]):
        if i == 0:
            finalCond = finalResult['condi'][i]
        else:
            finalCond = finalCond + ' | ' +  finalResult['condi'][i]
    
    finalResult = finalResult.append(pd.DataFrame([(finalCond, 0, 0,0,0,'X','X','N',0)],
                                    columns=finalResult.columns),
                                    ignore_index=False)
    finalResult.to_csv(fileName)
    

######################################################################################################
# R에서 inobuy1 <- inodel02 %>% filter( momp_arc7>=89) 데이터를 csv로 저장
# write.csv(inobuy1, 'inobuy1_close_h4_9year.csv')
# 실행할 부분을 선택한 후 shift + Enter 
cond, data = run("C:/Users/Shine_anal/Desktop/inott/allnew_adnl_d.csv"               # 로딩할 Data 경로와 파일 명
   ,"C:/Users/Shine_anal/Desktop/inott/allnew_adnl_d_{}.csv".format(datetime.datetime.today().strftime("%m%d%H%M"))) # 결과파일 저장할 경로와 파일 명
######################################################################################################
# 초기데이터, 조건들, dcnt, dvsb
run2(data, cond, 1, 0.6, 
     "C:/Users/Shine_anal/Desktop/inott/kbuy2_com_allnew_final.csv")




