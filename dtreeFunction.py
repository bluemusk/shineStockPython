# coding=utf-8
import sys
import pandas as pd
import datetime

def makeMatrix(data, aggr_df, conData, leafType, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum, condition, datBool, lvl, ratDf):
    # data = tmp[8]
    # aggr_df = tmp[9]
    # conData = tmp[1]
    # leafType = 'A'
    # rRatYn = 'Y'
    # rRatNum = 1
    # rRtYn = 'Y'
    # rRtLvl = 3
    # rRtNum = 1
    # serial = 2
    # branch = 4
    # lvl = 2
    # condition = tmp[6]
    # datBool = tmp[0]
    # ratDf = tmp[7]
    prevDcnt = data.iloc[:, 2].value_counts()[0]
    prevBcnt = data.iloc[:, 2].value_counts()[1]

    try:
        if leafType == 'A' and len(data) > 0 and prevDcnt >= 80:
            meltDt = data.iloc[:, 2:data.shape[1]].melt(id_vars='pur_gubn5')
            meltSummary = pd.DataFrame(meltDt.value_counts().reset_index(name='counts')).sort_values(
                ['variable', 'value'])
            tmpFinal = meltSummary.pivot(index=['variable', 'value'], columns='pur_gubn5', values='counts')

            tmpFinal.columns = tmpFinal.columns.values
            tmpFinal.reset_index(level=0, inplace=True)
            tmpFinal.reset_index(level=0, inplace=True)

            tmpFinal.columns = ['cal', 'tree', 'dcnt', 'bcnt']

            tmpFinal[tmpFinal['bcnt'] == 0]['bcnt'] = 0.8
            tmpFinal[tmpFinal['dcnt'] == 0]['dcnt'] = 0.8

            tmpFinal['bvsd'] = tmpFinal['bcnt'] / tmpFinal['dcnt']
            tmpFinal['dvsb'] = tmpFinal['dcnt'] / tmpFinal['bcnt']
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

            if rRatYn == 'Y':
                tmpFinal = tmpFinal[tmpFinal['rRat'] >= rRatNum]

            if rRtYn == 'Y' and lvl >= rRtLvl:
                tmpFinal = tmpFinal[tmpFinal['rRat'] >= rRtNum]

            pos = pd.merge(tmpFinal[tmpFinal['cal'] == 1], conData[conData['cal'] == 'GT'], left_on='tree',
                           right_on='colname', how='inner').sort_values('tree')
            neg = pd.merge(tmpFinal[tmpFinal['cal'] == 0], conData[conData['cal'] == 'LT'], left_on='tree',
                           right_on='colname', how='inner').sort_values('tree')
            midSet = pos
            midSet = midSet.append(neg)
        else:
            midSet = ratDf

    except Exception as e:
        midSet = ratDf
        pass

    try:
        if len(midSet) > 0:
            # 가지별로 계산하는 것을 makeMatrix에서 처리하겠음
            if leafType == 'A' and prevDcnt >= 80:
                midSet = midSet[midSet['rRat'] > 20]
                midSetChk = midSet[midSet['rRt'] > midSet['dvsb']]

                if midSetChk['dvsb'].count() > 0 :
                    midSet = midSet.sort_values('rRt', ascending=False)
                else:
                    midSet = midSet.sort_values('sRt', ascending=False)
            elif leafType == 'B' and prevDcnt >= 80:
                midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                midSet = midSet.sort_values('dvsb', ascending=False)
            elif leafType == 'C' and prevDcnt >= 80:
                midSet = midSet.sort_values('bcnt')
            elif leafType == 'D' and prevDcnt >= 80:
                midSet = midSet[midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt']]
                midSet = midSet.sort_values('dcnt', ascending=False)
            # A,B,C,D 공통 시작
            conH = midSet.iloc[0]['tree']
            rConH = midSet.iloc[0]['condi']
            valH = midSet.iloc[0]['value']
            calX = midSet.iloc[0]['cal_x']
            calH = midSet.iloc[0]['cal_y']

            tmpBool = (data[conH] == calX)

            if lvl == 1:
                if calH == 'GT':
                    condition = rConH + ' > ' + str(valH)
                else:
                    condition = rConH + ' <= ' + str(valH)
            else:
                if calH == 'GT':
                    condition = condition + ' AND ' + rConH + ' > ' + str(valH)
                else:
                    condition = condition + ' AND ' + rConH + ' <= ' + str(valH)
            # A,B,C,D 공통부분 종료
            aggr_df = aggr_df[tmpBool]
            data = data[tmpBool]
            tmpRt = midSet.iloc[0]
            conTmp = conData[~((conData['colname'] == midSet.iloc[0]['tree']) & (conData['cal'] == midSet.iloc[0]['cal_y']))]
            # data = data.drop(conH, axis=1)

            # 중복조건 있는지 확인
            if leafType == 'B' or leafType == 'C' or leafType == 'D':
                condition, tmpBool = chkDupCond(data, midSet, condition, tmpBool)

        else:
            tmpBool = pd.DataFrame()
            tmpRt = pd.DataFrame([(0,0,0,0)], columns=['bcnt','dcnt','bvsd','dvsb'])
            conTmp = conData
    except Exception as e:
        tmpBool = pd.DataFrame()
        tmpRt = pd.DataFrame([(0,0,0,0)], columns=['bcnt','dcnt','bvsd','dvsb'])
        conTmp = conData

    return data, aggr_df, tmpRt, tmpBool, condition, conTmp, midSet

def chkDupCond(data, midSet, condition, tmpBool):
    # 중복조건 있는지 확인
    chkAddiCon = midSet['dvsb'] >= midSet.iloc[0]['dvsb']
    tfCnt = chkAddiCon.value_counts()

    if tfCnt.loc[True] > 1 and tfCnt.loc[True] < 10:
        addiCon = midSet[chkAddiCon]

        for x in range(0, addiCon.shape[0]):
            conD = addiCon.iloc[x]['tree']
            rConD = addiCon.iloc[x]['condi']
            valD = addiCon.iloc[x]['value']
            calD = addiCon.iloc[x]['cal_y']
            calX = addiCon.iloc[x]['cal_x']

            dupBool = (data[conD] == calX)

            if x == 0:
                if calD == 'GT':
                    dupCondition = '(' + rConD + ' > ' + str(valD)
                else:
                    dupCondition = '(' + rConD + ' <= ' + str(valD)
            elif x != addiCon.shape[0] - 1:
                if calD == 'GT':
                    dupCondition = dupCondition + ' OR ' + rConD + ' > ' + str(valD)
                else:
                    dupCondition = dupCondition + ' OR ' + rConD + ' <= ' + str(valD)

            dupCondition = dupCondition + ' OR ' + condition + ')'

            condition = dupCondition
            tmpBool = tmpBool | dupBool
    else:
        condition = condition
        tmpBool = tmpBool

    return condition, tmpBool

def initCond(df, dfCon):
    cond = dfCon.iloc[:, 1]
    condDf = pd.DataFrame()
    newDf = pd.DataFrame()

    newDf['ymd'] = df['yyyymmdd']
    newDf['stCd'] = df['stock_code']
    newDf['pur_gubn5'] = df['pur_gubn5']

    for i in range(0, cond.shape[0]):
        con = cond.iloc[i].split('>')[0]
        try:
            val = float(cond.iloc[i].split('>')[1])
        except Exception as e:
            val = cond.iloc[i].split('>')[1]
            pass
        colname = 'tree' + str(i)
        condDf = condDf.append(pd.DataFrame([(con, val, 'GT', colname)], columns=['condi', 'value', 'cal', 'colname']))
        condDf = condDf.append(pd.DataFrame([(con, val, 'LT', colname)], columns=['condi', 'value', 'cal', 'colname']))
        condDf = condDf[~condDf['condi'].str.contains('-')]

        if '-' not in str(con):
            # 조건을 만족하면(condi > value) 값에 1을 넣고 아니면 0
            if if_float(val):
                tmpBoolP = df[con] > val
            else:
                tmpBoolP = df[con] > df[val]

            newDf[colname] = tmpBoolP.apply(lambda x: 1 if x == True else 0)
            # newDf.append(tmpBoolP.apply(lambda x: 1 if x == True else 0), colname=['tree' + str(i)], ignore_index=True)

    return newDf,condDf

def if_float(string):
    try:
        float(string)
        return True
    except ValueError:
        return False

def drawTree(data, level, limitRatio, initConds, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum ):
    #####################################################################################
    #  트리를 그리는 로직
    # level = 2
    # data = newDff
    # initConds = dfConF
    # rRatYn = 'Y'
    # rRatNum = 1
    # rRtYn = 'Y'
    # rRtLvl = 3
    # rRtNum = 1
    #####################################################################################
    mod = sys.modules[__name__]
    branch = 4
    # i = 1 j = 1
    try:
        for i in range(0, level + 1):
            leaf = branch ** i

            if i == 0:
                # cnt = data['pur_gubn5'].value_counts()
                # dcnt = int(cnt.loc[0])
                # bcnt = int(cnt.loc[1])
                # data = data.assign(bool = lambda x : True)
                # datBool = data['bool']
                data['x'] = True
                datBool = data['x']
                data = data.drop('x', axis=1)
                setattr(mod, 'lvl_0_1', [datBool, initConds, '', '', '', '', '', '',data,data.iloc[:, 2]])
                #                              0              1             2     3       4          5            6    7[result condition]]
                # setattr(mod, 'lvl_0_1', [pd.DataFrame(), pd.DataFrame(), bcnt, dcnt, bcnt / dcnt, dcnt / bcnt, '', ''])
            else:
                # i = 1 j = 1
                for j in range(1, leaf + 1):
                    print("[" + datetime.datetime.today().strftime(
                        "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j) + " - Start")
                    if j % branch == 1:
                        tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // branch + 1))

                        try:
                            prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                        except Exception as e:
                            prevDcnt = 0
                            pass

                        try:
                            prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                        except Exception as e:
                            prevBcnt = 0
                            pass

                        print("[" + datetime.datetime.today().strftime(
                            "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j) + " - Start / prevBcnt : " + str(
                            prevBcnt) + " / prevDcnt : " + str(
                            prevDcnt))
                        tmpNxtRat = pd.DataFrame()
                        tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                                                       rRtNum, tmp[6], tmp[0], i, tmp[7])
                        tmpNxtRat = tmpRatList
                    elif j % branch == 2:
                        tmpDat, tmpAggrDf,tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                                                       rRtNum, tmp[6], tmp[0], i, tmpNxtRat)
                    elif j % branch == 3:
                        tmpDat, tmpAggrDf,tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                                                       rRtNum, tmp[6], tmp[0], i, tmpNxtRat)
                    elif j % branch == 0:
                        tmpDat, tmpAggrDf,tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                                                       rRtNum, tmp[6], tmp[0], i, tmpNxtRat)


                    if i >= 3: # 초기 1,2,3레벨일때는 데이터 셋이 크니까 없애버려
                        setattr(mod, 'lvl_{}_{}'.format(i, j),
                                [pd.DataFrame(), pd.DataFrame(), tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb,
                                 tmpCon,
                                 tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # print("here1!!")

                    if i != level:
                        # print("here2!!")
                        setattr(mod, 'lvl_{}_{}'.format(i, j),
                                [tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon,
                                 tmpRatList, tmpDat, tmpAggrDf])
                    else: # 마지막 레벨일때는 램을 많이 먹는 데이터셋은 저장하지 않음
                        # print("here3!!")
                        setattr(mod, 'lvl_{}_{}'.format(i, j),
                                [pd.DataFrame(), pd.DataFrame(), tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon,
                                 tmpRatList, pd.DataFrame(), pd.DataFrame()])
    except Exception as e:
        setattr(mod, 'lvl_{}_{}'.format(i, j),
                [tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon, tmpRatList, tmpDat,
                 tmpAggrDf])

    fResult = pd.DataFrame()
    # mod = sys.modules[__name__]

    for i in range(0, level + 1):
        leaf = branch ** i
        for j in range(1, leaf + 1):
            tmp = getattr(mod, 'lvl_{}_{}'.format(i, j))
            # print('lvl_'+str(i)+'_'+str(j))
            # print(tmp[3].value_counts('pur_gubn5')[0])
            try:
                bcnt = int(tmp[2])
            except Exception as e:
                bcnt = 1
                pass

            try:
                dcnt = int(tmp[3])
            except Exception as e:
                dcnt = 1
                pass

            fResult = fResult.append(pd.DataFrame([('lvl_' + str(i) + '_' + str(j), tmp[6], bcnt, dcnt, bcnt / dcnt, dcnt / bcnt)],
                                                  columns=['lvl', 'condi', 'bcnt', 'dcnt', 'bvsd', 'dvsb']))

    return fResult


# fast5D > macd_slow3k AND sma5_ang1 <= stdbal_cl_ang1
# data[lvl_2_5[0]]
# lvl_2_5[2]
# data[(data['fast5D'] <= data['macd_slow3k']) & (data['sma5_ang1'] > data['fast5D'])]['pur_gubn5'].value_counts()



# Data Loading
# df = pd.read_csv('C:/Users/Shine_anal/PycharmProjects/anlaysis/target.csv')
# df = pd.read_csv('C:/Users/Shine_anal/PycharmProjects/anlaysis/tr2.csv')
# dfCon = pd.read_csv('C:/Users/Shine_anal/PycharmProjects/anlaysis/dtreeCon.csv')

# df = pd.read_csv('C:/Users/Shine_anal/Desktop/gkosbuy4.csv')
df = pd.read_csv('C:/Users/Shine_anal/Desktop/kbuy1.csv')
dfCon = pd.read_csv('C:/Users/Shine_anal/Desktop/dtreeCon.csv')

# 조건 이니셜라이즈
newDff, dfConF = initCond(df, dfCon)

# newDff.value_counts('pur_gubn5')
#data, level, limitRatio, initConds, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum ):
fResult = drawTree(newDff, 2, 4, dfConF, 'Y', 1, 'Y', 3, 1)
# [2021-08-26 16:58:16] Lvl_1_1 - Start
# [2021-08-26 17:06:20] Lvl_6_4096 - Start


fResult = fResult.sort_values('bvsd', ascending=False)
fResult.to_csv("C:/Users/Shine_anal/Desktop/kbuy1_20210827_dtree.csv")

# sma5_ang1__sma10_ang2 > 0.0 AND atan_fast5D_slow5D_gap > 51.0 AND slow3D_ang1 > slow10D_ang1 AND nmind_sig_ang2 > 0.0 AND slow3D <= rsi AND slow10D_ang3_1bd > 0.0
# df[
# (
#   (df['sma5_ang1__sma10_ang2'] > 0.0)
# & (df['atan_fast5D_slow5D_gap'] > 51.0 )
# & (df['slow3D_ang1'] > df['slow10D_ang1'])
# & (df['nmind_sig_ang2'] > 0.0)
# & (df['slow3D'] <= df['rsi'])
# & (df['slow10D_ang3_1bd'] > 0.0)
# )
# ].value_counts('pur_gubn5')
#
# tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
#                                                                        rRtNum, tmp[6], tmp[0], i, tmp[7])
#
# meltDt = data.iloc[:, 2:data.shape[1]].melt(id_vars='pur_gubn5')
#             meltSummary = pd.DataFrame(meltDt.value_counts().reset_index(name='counts')).sort_values(
#                 ['variable', 'value'])
#             tmpFinal = meltSummary.pivot(index=['variable', 'value'], columns='pur_gubn5', values='counts')
# meltDt = lvl_6_342[8].iloc[:, 2:data.shape[1]].melt(id_vars='pur_gubn5')