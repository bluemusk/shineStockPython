

#################################
# coding=utf-8
import sys
import pandas as pd
import datetime
import os
import pickle
import warnings

warnings.filterwarnings('ignore')

##################################

def makeMatrix(data, aggr_df, conData, leafType, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum, condition, datBool, lvl, ratDf, invYn, treeNm):
    # data = tmp[8]
    # aggr_df = tmp[9]
    # conData = tmp[1]
    # leafType = 'D'
    # rRatYn = 'Y'
    # rRatNum = 1
    # rRtYn = 'Y'
    # rRtLvl = 3
    # rRtNum = 1
    # serial = 2
    # branch = 4
    # lvl = 2
    # condition = tmp[6]
    # datBool = tilemp[0]
    # # ratDf = tmp[7]
    # invYn = 'N'
    # treeNm = 'tree1'
    # -----------------------------------------
    # data = newDff
    # data['x'] = True
    # datBool = data['x']
    # data = data.drop('x', axis=1)
    # data = data.drop(tmpInv[8].index)
    # aggr_df = data['pur_gubn5']
    # conData = tmpInv[1]
    # leafType = 'C'
    # rRatYn = 'Y'
    # rRatNum = 1
    # rRtYn = 'Y'
    # rRtLvl = 3
    # rRtNum = 1
    # serial = 2
    # branch = 4
    # lvl = 1
    # condition = tmpInvCon
    # datBool = ~tmpInv[0]
    # ratDf = ''
    # invYn = 'Y'
    # treeNm = trees.iloc[row].treeNm
    try:
        prevDcnt = data.iloc[:, 2].value_counts()[0]
    except Exception as e:
        prevDcnt = 0.8

    try:
        prevBcnt = data.iloc[:, 2].value_counts()[1]
    except Exception as e:
        prevBcnt = 0.8

    try:
        if (leafType == 'A' and len(data) > 0) or invYn == 'Y':
            meltDt = data.iloc[:, 2:data.shape[1]].melt(id_vars='pur_gubn5')
            # meltDt = data.iloc[1:10, 2:5].melt(id_vars='pur_gubn5')
            meltSummary = pd.DataFrame(meltDt.value_counts().reset_index(name='counts')).sort_values(
                ['variable', 'value'])
            tmpFinal = meltSummary.pivot(index=['variable', 'value'], columns='pur_gubn5', values='counts')

            tmpFinal.columns = tmpFinal.columns.values
            tmpFinal.reset_index(level=0, inplace=True)
            tmpFinal.reset_index(level=0, inplace=True)
            
            tmpFinal.columns = ['cal', 'tree', 'dcnt', 'bcnt']
            # x = pd.DataFrame(tmpFinal.value_counts('tree'))
            # x.columns = ['cnt']
            # x[x['cnt'] == 2]

            tmpFinal = tmpFinal.fillna(0.8)
            # tmpFinal[tmpFinal['bcnt'] == 0]['bcnt'] = 0.8
            # tmpFinal[tmpFinal['dcnt'] == 0]['dcnt'] = 0.8

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
            if treeNm == 'tree1':
                if leafType == 'A':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / bcnt asc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('bcnt')
                elif leafType == 'C':
                    midSet = midSet[(midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt'])]
                    midSet = midSet[midSet['rRat'] > 20]
                    midSet = midSet.sort_values('dcnt', ascending=False)
                # elif leafType == 'D':
                #     midSet = midSet[(midSet['dvsb'] > 0)]
                #     midSet = midSet[(midSet['prevDcnt'] > 0)]
                #     midSet = midSet.sort_values('bcnt', ascending=False)
                elif leafType == 'D':
                    # bcnt != 0 & dcnt != 0 & bcnt <= totCnt * 0.25 /	dcnt desc
                    midSet = midSet[midSet['bcnt'] <= (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.25]
                    midSet = midSet.sort_values('dcnt', ascending=False)
            elif treeNm == 'tree2':
                if leafType == 'A':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / bcnt asc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('bcnt')
                elif leafType == 'C':
                    midSet = midSet[(midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt'])]
                    midSet = midSet[midSet['rRat'] > 20]
                    midSet = midSet.sort_values('dcnt', ascending=False)
                elif leafType == 'D':
                    midSet = midSet[(midSet['dvsb'] > 0)]
                    midSet = midSet[(midSet['prevDcnt'] > 0)]
                    midSet = midSet.sort_values('bcnt', ascending=False)
            elif treeNm == 'tree3':
                if leafType == 'A':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / bcnt asc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('bcnt')
                elif leafType == 'C':
                    midSet = midSet[(midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt'])]
                    midSet = midSet[midSet['rRat'] > 20]
                    midSet = midSet.sort_values('dcnt', ascending=False)
                elif leafType == 'D':
                    midSet = midSet[(midSet['dvsb'] > 0)]
                    midSet = midSet[(midSet['prevDcnt'] > 0)]
                    midSet = midSet.sort_values('bcnt', ascending=False)
            elif treeNm == 'tree4':
                if leafType == 'A':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / bcnt asc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('bcnt')
                elif leafType == 'C':
                    midSet = midSet[(midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt'])]
                    midSet = midSet[midSet['rRat'] > 20]
                    midSet = midSet.sort_values('dcnt', ascending=False)
                elif leafType == 'D':
                    midSet = midSet[(midSet['dvsb'] > 0)]
                    midSet = midSet[(midSet['prevDcnt'] > 0)]
                    midSet = midSet.sort_values('bcnt', ascending=False)
            elif treeNm == 'tree5':
                if leafType == 'A':
                    midSet = midSet[midSet['rRat'] > 20]
                    midSetChk = midSet[midSet['rRt'] > midSet['dvsb']]

                    if midSetChk['dvsb'].count() > 0:
                        midSet = midSet.sort_values('rRt', ascending=False)
                    else:
                        midSet = midSet.sort_values('sRt', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'C':
                    midSet = midSet.sort_values('bcnt')
                elif leafType == 'D':
                    midSet = midSet[midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt']]
                    midSet = midSet.sort_values('dcnt', ascending=False)
            elif treeNm == 'tree6':
                if lvl > 0 and lvl <= 3:
                    midSet = midSet[midSet['rRat'] > 35]
                elif lvl > 3 and lvl <= 6:
                    midSet = midSet[midSet['rRat'] > 25]
                elif lvl > 6 and lvl <= 9:
                    midSet = midSet[midSet['rRat'] > 15]

                midSet = midSet.sort_values('rRt', ascending=False)
            elif treeNm == 'tree7':
                if leafType == 'A':
                    if lvl > 0 and lvl <= 2:
                        midSet = midSet[midSet['rRat'] > 40]
                    elif lvl > 2 and lvl <= 4:
                        midSet = midSet[midSet['rRat'] > 30]
                    elif lvl > 4 and lvl <= 6:
                        midSet = midSet[midSet['rRat'] > 20]
                    elif lvl > 6:
                        midSet = midSet[midSet['rRat'] > 7]

                    midSet = midSet.sort_values('sRt', ascending=False)
                elif leafType == 'B':
                    if lvl > 0 and lvl <= 2:
                        midSet = midSet[midSet['rRat'] > 35]
                    elif lvl > 2 and lvl <= 4:
                        midSet = midSet[midSet['rRat'] > 25]
                    elif lvl > 4 and lvl <= 6:
                        midSet = midSet[midSet['rRat'] > 15]
                    elif lvl > 6:
                        midSet = midSet[midSet['rRat'] > 5]

                    midSet = midSet.sort_values('rRt', ascending=False)
            elif treeNm == 'tree8':
                if leafType == 'A':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    if lvl > 0 and lvl <= 2:
                        midSet = midSet[midSet['rRat'] > 40]
                    elif lvl > 2 and lvl <= 4:
                        midSet = midSet[midSet['rRat'] > 30]
                    elif lvl > 4 and lvl <= 6:
                        midSet = midSet[midSet['rRat'] > 20]
                    elif lvl > 6:
                        midSet = midSet[midSet['rRat'] > 7]

                    midSet = midSet.sort_values('rRt', ascending=False)
                elif leafType == 'C':
                    midSet = midSet[midSet['rRat'] > 20]
                    midSetChk = midSet[midSet['rRt'] > midSet['dvsb']]

                    if midSetChk['dvsb'].count() > 0:
                        midSet = midSet.sort_values('rRt', ascending=False)
                    else:
                        midSet = midSet.sort_values('sRt', ascending=False)
            elif treeNm == 'tree9':
                if leafType == 'A':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['rRat'] > 20]
                    midSetChk = midSet[midSet['rRt'] > midSet['dvsb']]

                    if midSetChk['dvsb'].count() > 0:
                        midSet = midSet.sort_values('rRt', ascending=False)
                    else:
                        midSet = midSet.sort_values('sRt', ascending=False)
                elif leafType == 'C':
                    midSet = midSet[midSet['rRt'] > midSet['rRt'].mean()]
                    midSet = midSet.sort_values('dcnt', ascending=False)
                elif leafType == 'D':
                    midSet = midSet[(midSet['dvsb'] > 0) & (midSet['prevDcnt'] > 0)]
                    midSet = midSet.sort_values('bcnt', ascending=False)
            elif treeNm == 'tree10':
                if leafType == 'A':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / bcnt asc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('bcnt')
                elif leafType == 'C':
                    midSet = midSet[(midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt'])]
                    midSet = midSet[midSet['rRat'] > 20]
                    midSet = midSet.sort_values('dcnt', ascending=False)
                elif leafType == 'D':
                    midSet = midSet[(midSet['dvsb'] > 0)]
                    midSet = midSet[(midSet['prevDcnt'] > 0)]
                    midSet = midSet.sort_values('bcnt', ascending=False)
                elif leafType == 'E':
                    # bcnt != 0 & dcnt != 0 & bcnt <= totCnt * 0.25 /	dcnt desc
                    midSet = midSet[midSet['bcnt'] <= (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.25]
                    midSet = midSet.sort_values('dcnt', ascending=False)

            # A,B,C,D 공통 시작
            conH = midSet.iloc[0]['tree']
            rConH = midSet.iloc[0]['condi']
            valH = midSet.iloc[0]['value']
            calX = midSet.iloc[0]['cal_x']
            calH = midSet.iloc[0]['cal_y']

            tmpBool = (data[conH] == calX)

            if lvl == 1 and invYn == 'N':
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
            conTmp = conData[
                ~((conData['colname'] == midSet.iloc[0]['tree']) & (conData['cal'] == midSet.iloc[0]['cal_y']))]
            data = data.drop(conH, axis=1)

            # 중복조건 있는지 확인
            # if leafType == 'B' or leafType == 'C' or leafType == 'D':
            #     condition, tmpBool = chkDupCond(data, midSet, condition, tmpBool)

        else:
            tmpBool = pd.DataFrame()
            tmpRt = pd.DataFrame([(0.8, 0.8, 0, 0)], columns=['bcnt', 'dcnt', 'bvsd', 'dvsb'])
            conTmp = conData
    except Exception as e:
        tmpBool = pd.DataFrame()
        tmpRt = pd.DataFrame([(0.8, 0.8, 0, 0)], columns=['bcnt', 'dcnt', 'bvsd', 'dvsb'])
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
    # cond = dfCon.iloc[:, 1]
    cond = dfCon
    condDf = pd.DataFrame()
    newDf = pd.DataFrame()

    newDf['ymd'] = df['yyyymmdd']
    newDf['stCd'] = df['stock_code']
    newDf['pur_gubn5'] = df['pur_gubn5']
    
    for i in range(0, cond.shape[0]):
        con = cond.iloc[i,0].split('>')[0]
        try:
            val = float(cond.iloc[i,0].split('>')[1])
            type = 'Num'
        except Exception as e:
            val = cond.iloc[i,0].split('>')[1]
            type = 'Col'
            pass
        colname = 'tree' + str(i)
        condDf = condDf.append(pd.DataFrame([(con, val, 'GT', colname, type)], columns=['condi', 'value', 'cal', 'colname', 'type']))
        condDf = condDf.append(pd.DataFrame([(con, val, 'LT', colname, type)], columns=['condi', 'value', 'cal', 'colname', 'type']))
        condDf = condDf[~condDf['condi'].str.contains('-')]

        try:
            if '-' not in str(con) and '+' not in str(con):
                # 조건을 만족하면(condi > value) 값에 1을 넣고 아니면 0
                if if_float(val):
                    tmpBoolP = df[con] > val
                else:
                    tmpBoolP = df[con] > df[val]

                newDf[colname] = tmpBoolP.apply(lambda x: 1 if x == True else 0)
                # newDf.append(tmpBoolP.apply(lambda x: 1 if x == True else 0), colname=['tree' + str(i)], ignore_index=True)
        except Exception as e:
            pass

    return newDf, condDf

def if_float(string):
    try:
        float(string)
        return True
    except ValueError:
        return False

def drawTree(data, level, limitRatio, initConds, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum, fileSize):
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
    # limitCntRatio = 0.3
    #####################################################################################
    mod = sys.modules[__name__]
    
    initDcnt = data.iloc[:, 2].value_counts()[0]
    # limitCnt = initDcnt * limitCntRatio * 0.01
    fResultT = pd.DataFrame()

    trees = pd.DataFrame([('tree1', 4)], columns=('treeNm', 'branch'))
    # trees = pd.DataFrame([('tree2', 4)], columns=('treeNm', 'branch'))
    trees = trees.append(pd.DataFrame([('tree2', 4)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree3', 4)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree4', 4)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree5', 4)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree6', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree7', 3)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree8', 3)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree9', 4)], columns=('treeNm', 'branch')))
    # trees = trees.append(pd.DataFrame([('tree10', 5)], columns=('treeNm', 'branch')))
    # i, j, row =  1, 3, 0
    for row in range(0, trees.shape[0]):
        # print('tree forloop')
        for i in range(0, level + 1):
            # print('level forloop')
            leaf = trees.iloc[row].branch ** i

            if i == 0:
                data['x'] = True
                datBool = data['x']
                data = data.drop('x', axis=1)

                if trees.iloc[row].treeNm != 'tree10':
                    setattr(mod, 'lvl_0_1', [datBool, initConds, 0.8, 0.8, 0.8, 0.8, '', '', data, data.iloc[:, 2]])
                else:
                    with open('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_0_1.pkl', 'wb') as f:
                        pickle.dump([datBool, initConds, 0.8, 0.8, 0.8, 0.8, '', '', data, data.iloc[:, 2]], f)
            else:
                for j in range(1, leaf + 1):
                    if trees.iloc[row].treeNm == 'tree1':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            if i > 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                ## Load pickle
                                with open(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i - 1, j // trees.iloc[row].branch + 1),
                                        'rb') as fr:
                                    tmp = pickle.load(fr)
                            else:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))
                            
                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0 :
                                print('[' + datetime.datetime.today().strftime(
                                    '%Y-%m-%d %H:%M:%S') + '] Lvl_' + str(i) + '_' + str(j) + ' - ' + trees.iloc[
                                          row].treeNm + ' / prevBcnt : ' + str(
                                    prevBcnt) + ' / prevDcnt : ' + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                    with open(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                    i, j),
                                            'wb') as f:
                                        pickle.dump(
                                            [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                             tmpRatList, pd.DataFrame(), pd.DataFrame()], f)
                                else:
                                    setattr(mod, 'lvl_{}_{}'.format(i, j),
                                            [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                             tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if prevDcnt > 0 :
                                # tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                    ## Load pickle
                                    with open(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i, j - 1),
                                            'rb') as fr:
                                        tmpInv = pickle.load(fr)
                                else:
                                    tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon = ''
                                tmpInvCon = changeCondition(tmpInv[6])
                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    try:
                                        tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                            data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                            tmpInv[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                            rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                    except Exception as e:
                                        print(e)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                    with open(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i, j),
                                            'wb') as f:
                                        pickle.dump(
                                            [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                             tmpRatList, pd.DataFrame(), pd.DataFrame()], f)
                                else:
                                    setattr(mod, 'lvl_{}_{}'.format(i, j),
                                            [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                             tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 3:  # j = 3
                            if prevDcnt > 0 :
                                # tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                    ## Load pickle
                                    with open(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i, j - 1),
                                            'rb') as fr:
                                        tmpInv = pickle.load(fr)
                                else:
                                    tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon = ''
                                tmpInvCon = changeCondition(tmpInv[6])
                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                        tmpInv[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                    with open(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i, j),
                                            'wb') as f:
                                        pickle.dump(
                                            [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                             tmpRatList, pd.DataFrame(), pd.DataFrame()], f)
                                else:
                                    setattr(mod, 'lvl_{}_{}'.format(i, j),
                                            [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                             tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0 :
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                    with open(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i, j),
                                            'wb') as f:
                                        pickle.dump(
                                            [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                             tmpRatList, pd.DataFrame(), pd.DataFrame()], f)
                                else:
                                    setattr(mod, 'lvl_{}_{}'.format(i, j),
                                            [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                             tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        # elif j % trees.iloc[row].branch == 0:
                        #     if prevDcnt > 0 :
                        #         # tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                        #         if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                        #             ## Load pickle
                        #             with open(
                        #                     'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                        #                         i, j - 1),
                        #                     'rb') as fr:
                        #                 tmpInv = pickle.load(fr)
                        #         else:
                        #             tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                        #         tmpInvCon = ''
                        #         tmpInvCon = changeCondition(tmpInv[6])
                        #         tmpInvCon = '(' + tmpInvCon + ')'

                        #         if i == 1:
                        #             tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                        #                 data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                        #                 tmpInv[1], 'E', rRatYn, rRatNum, rRtYn, rRtLvl,
                        #                 rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                        #         else:
                        #             tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                        #                 tmp[8], tmp[9], tmp[1], 'E', rRatYn, rRatNum, rRtYn, rRtLvl,
                        #                 rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                        #     else:
                        #         if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                        #             with open(
                        #                     'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                        #                         i, j),
                        #                     'wb') as f:
                        #                 pickle.dump(
                        #                     [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                        #                      tmpRatList, pd.DataFrame(), pd.DataFrame()], f)
                        #         else:
                        #             setattr(mod, 'lvl_{}_{}'.format(i, j),
                        #                     [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                        #                      tmpRatList, pd.DataFrame(), pd.DataFrame()])
                    elif trees.iloc[row].treeNm == 'tree2':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            if i > 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                ## Load pickle
                                with open(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i - 1, j // trees.iloc[row].branch + 1),
                                        'rb') as fr:
                                    tmp = pickle.load(fr)
                            else:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0 :
                                print('[' + datetime.datetime.today().strftime(
                                    '%Y-%m-%d %H:%M:%S') + '] Lvl_' + str(i) + '_' + str(
                                    j) + ' - ' + trees.iloc[row].treeNm + ' / prevBcnt : ' + str(
                                    prevBcnt) + ' / prevDcnt : ' + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if prevDcnt > 0 :
                                if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                    ## Load pickle
                                    with open(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i, j - 1),
                                            'rb') as fr:
                                        tmpInv = pickle.load(fr)
                                else:
                                    tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))

                                tmpInvCon = ''
                                tmpInvCon = changeCondition(tmpInv[6])
                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                        tmpInv[1],
                                        'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 3:
                            if prevDcnt > 0 :
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0 :
                                if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                    ## Load pickle
                                    with open(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i, j - 1),
                                            'rb') as fr:
                                        tmpInv1 = pickle.load(fr)
                                else:
                                    tmpInv1 = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon1 = ''
                                tmpInvCon1 = changeCondition(tmpInv1[6])
                                tmpInvCon1 = '(' + tmpInvCon1 + ')'

                                # inv_B
                                if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                    ## Load pickle
                                    with open(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i, j - 1),
                                            'rb') as fr:
                                        tmpInv2 = pickle.load(fr)
                                else:
                                    tmpInv2 = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon2 = ''
                                tmpInvCon2 = changeCondition(tmpInv2[6])
                                tmpInvCon2= '(' + tmpInvCon2 + ')'

                                tmpInvCon = tmpInvCon1 + ' AND ' + tmpInvCon2

                                expData = pd.DataFrame(tmpInv1[8].index, columns=['ex'])
                                expData = expData.append(pd.DataFrame(tmpInv2[8].index, columns=['ex']))

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        data.drop(expData.ex.unique()), data.drop(expData.ex.unique())['pur_gubn5'],
                                        tmpInv[1],
                                        'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                    elif trees.iloc[row].treeNm == 'tree3':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            if i > 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                ## Load pickle
                                with open(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i - 1, j // trees.iloc[row].branch + 1),
                                        'rb') as fr:
                                    tmp = pickle.load(fr)
                            else:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0 :
                                print('[' + datetime.datetime.today().strftime(
                                    '%Y-%m-%d %H:%M:%S') + '] Lvl_' + str(i) + '_' + str(
                                    j) + ' - ' + trees.iloc[row].treeNm + ' / prevBcnt : ' + str(
                                    prevBcnt) + ' / prevDcnt : ' + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if prevDcnt > 0 :
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 3:  # j = 3
                            if prevDcnt > 0 :
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0 :
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                    elif trees.iloc[row].treeNm == 'tree4':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            if i > 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                ## Load pickle
                                with open(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i - 1, j // trees.iloc[row].branch + 1),
                                        'rb') as fr:
                                    tmp = pickle.load(fr)
                            else:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0 :
                                print('[' + datetime.datetime.today().strftime(
                                    '%Y-%m-%d %H:%M:%S') + '] Lvl_' + str(i) + '_' + str(
                                    j) + ' - ' + trees.iloc[row].treeNm + ' / prevBcnt : ' + str(
                                    prevBcnt) + ' / prevDcnt : ' + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if prevDcnt > 0 :
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 3:  # j = 3
                            if prevDcnt > 0 :
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0 :
                                if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                    ## Load pickle
                                    with open(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i, j - 1),
                                            'rb') as fr:
                                        tmpInv1 = pickle.load(fr)
                                else:
                                    tmpInv1 = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon1 = ''
                                tmpInvCon1 = changeCondition(tmpInv1[6])
                                tmpInvCon1 = '(' + tmpInvCon1 + ')'

                                tmpInvCon = tmpInvCon1  # + ' AND ' + tmpInvCon2 + ' AND ' + tmpInvCon3

                                expData = pd.DataFrame(tmpInv1[8].index, columns=['ex'])
                                # expData = expData.append(pd.DataFrame(tmpInv2[8].index, columns=['ex']))
                                # expData = expData.append(pd.DataFrame(tmpInv3[8].index, columns=['ex']))

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        data.drop(expData.ex.unique()), data.drop(expData.ex.unique())['pur_gubn5'],
                                        tmpInv[1],
                                        'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                    elif trees.iloc[row].treeNm == 'tree5':
                        if j % trees.iloc[row].branch == 1:
                            if i > 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                ## Load pickle
                                with open(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i - 1, j // trees.iloc[row].branch + 1),
                                        'rb') as fr:
                                    tmp = pickle.load(fr)
                            else:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0 :
                                print('[' + datetime.datetime.today().strftime(
                                    '%Y-%m-%d %H:%M:%S') + '] Lvl_' + str(i) + '_' + str(
                                    j) + ' - ' + trees.iloc[row].treeNm + ' / prevBcnt : ' + str(
                                    prevBcnt) + ' / prevDcnt : ' + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if prevDcnt > 0 :
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 3:  # j = 3
                            if prevDcnt > 0 :
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0 :
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                    elif trees.iloc[row].treeNm == 'tree6':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            if i > 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                ## Load pickle
                                with open(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i - 1, j // trees.iloc[row].branch + 1),
                                        'rb') as fr:
                                    tmp = pickle.load(fr)
                            else:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0 :
                                print('[' + datetime.datetime.today().strftime(
                                    '%Y-%m-%d %H:%M:%S') + '] Lvl_' + str(i) + '_' + str(
                                    j) + ' - ' + trees.iloc[row].treeNm + ' / prevBcnt : ' + str(
                                    prevBcnt) + ' / prevDcnt : ' + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0 :
                                if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                    ## Load pickle
                                    with open(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i, j - 1),
                                            'rb') as fr:
                                        tmpInv = pickle.load(fr)
                                else:
                                    tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon = ''
                                tmpInvCon = changeCondition(tmpInv[6])
                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                        tmpInv[1],
                                        'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                    elif trees.iloc[row].treeNm == 'tree7':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            if i > 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                ## Load pickle
                                with open(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i - 1, j // trees.iloc[row].branch + 1),
                                        'rb') as fr:
                                    tmp = pickle.load(fr)
                            else:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0 :
                                print('[' + datetime.datetime.today().strftime(
                                    '%Y-%m-%d %H:%M:%S') + '] Lvl_' + str(i) + '_' + str(
                                    j) + ' - ' + trees.iloc[row].treeNm + ' / prevBcnt : ' + str(
                                    prevBcnt) + ' / prevDcnt : ' + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if prevDcnt > 0 :
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0 :
                                if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                    ## Load pickle
                                    with open(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i, j - 1),
                                            'rb') as fr:
                                        tmpInv1 = pickle.load(fr)
                                else:
                                    tmpInv1 = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon = ''
                                tmpInvCon = changeCondition(tmpInv1[6])
                                tmpInvCon = '(' + tmpInvCon + ')'

                                expData = pd.DataFrame(tmpInv1[8].index, columns=['ex'])
                                # expData = expData.append(pd.DataFrame(tmpInv2[8].index, columns=['ex']))

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        data.drop(expData.ex.unique()), data.drop(expData.ex.unique())['pur_gubn5'],
                                        tmpInv1[1],
                                        'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv1[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                    elif trees.iloc[row].treeNm == 'tree8':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            if i > 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                ## Load pickle
                                with open(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i - 1, j // trees.iloc[row].branch + 1),
                                        'rb') as fr:
                                    tmp = pickle.load(fr)
                            else:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0 :
                                print('[' + datetime.datetime.today().strftime(
                                    '%Y-%m-%d %H:%M:%S') + '] Lvl_' + str(i) + '_' + str(
                                    j) + ' - ' + trees.iloc[row].treeNm + ' / prevBcnt : ' + str(
                                    prevBcnt) + ' / prevDcnt : ' + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if prevDcnt > 0 :
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0 :
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                    elif trees.iloc[row].treeNm == 'tree9':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            if i > 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                ## Load pickle
                                with open(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i - 1, j // trees.iloc[row].branch + 1),
                                        'rb') as fr:
                                    tmp = pickle.load(fr)
                            else:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0 :
                                print('[' + datetime.datetime.today().strftime(
                                    '%Y-%m-%d %H:%M:%S') + '] Lvl_' + str(i) + '_' + str(
                                    j) + ' - ' + trees.iloc[row].treeNm + ' / prevBcnt : ' + str(
                                    prevBcnt) + ' / prevDcnt : ' + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if prevDcnt > 0 :
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 3:
                            if prevDcnt > 0 :
                                if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                    ## Load pickle
                                    with open(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i, j - 1),
                                            'rb') as fr:
                                        tmpInv = pickle.load(fr)
                                else:
                                    tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon = ''
                                tmpInvCon = changeCondition(tmpInv[6])
                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                        tmpInv[1],
                                        'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0 :
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                    elif trees.iloc[row].treeNm == 'tree10':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            with open('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(i - 1,
                                                                                                                 j // trees.iloc[row].branch + 1),
                                      'rb') as fr:
                                tmp = pickle.load(fr)

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            print('[' + datetime.datetime.today().strftime(
                                '%Y-%m-%d %H:%M:%S') + '] Lvl_' + str(i) + '_' + str(j) + ' - ' + trees.iloc[
                                      row].treeNm + ' / prevBcnt : ' + str(
                                prevBcnt) + ' / prevDcnt : ' + str(
                                prevDcnt))
                            tmpNxtRat = pd.DataFrame()
                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            tmpNxtRat = tmpRatList

                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                ## Load pickle
                                with open(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                            i, j - 1),
                                        'rb') as fr:
                                    tmpInv = pickle.load(fr)
                            else:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                            tmpInvCon = ''
                            tmpInvCon = changeCondition(tmpInv[6])
                            tmpInvCon = '(' + tmpInvCon + ')'

                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                tmpInv[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)

                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 3:  # j = 3
                            # tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                            if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                ## Load pickle
                                with open(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                            i, j - 1),
                                        'rb') as fr:
                                    tmpInv = pickle.load(fr)
                            else:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                            tmpInvCon = ''
                            tmpInvCon = changeCondition(tmpInv[6])
                            tmpInvCon = '(' + tmpInvCon + ')'

                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                tmpInv[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)

                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 4:
                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                                ## Load pickle
                                with open(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                            i, j - 1),
                                        'rb') as fr:
                                    tmpInv = pickle.load(fr)
                            else:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                            tmpInvCon = ''
                            tmpInvCon = changeCondition(tmpInv[6])
                            tmpInvCon = '(' + tmpInvCon + ')'

                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                tmpInv[1], 'E', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)

                    
                    if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                        with open('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(i, j),
                                    'wb') as f:
                            pickle.dump([tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon,
                                    tmpRatList, tmpDat, tmpAggrDf], f)
                    else:
                        setattr(mod, 'lvl_{}_{}'.format(i, j),
                                [tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon,
                                    tmpRatList, tmpDat, tmpAggrDf])

                    try:
                        bcnt = float(tmpRat.bcnt)
                    except Exception as e:
                        bcnt = 0.8
                        pass

                    try:
                        dcnt = float(tmpRat.dcnt)
                    except Exception as e:
                        dcnt = 0.8
                        pass

                    print('[' + datetime.datetime.today().strftime(
                        '%Y-%m-%d %H:%M:%S') + '] Lvl_' + str(i) + '_' + str(j) + ' - ' + trees.iloc[
                              row].treeNm + ' / bcnt : ' + str(
                        bcnt) + ' / dcnt : ' + str(
                        dcnt))

                    try:
                        fResultT = fResultT.append(
                            pd.DataFrame([('lvl_' + str(i) + '_' + str(j), tmpCon, bcnt, dcnt, bcnt / dcnt, dcnt / bcnt)],
                                         columns=['lvl', 'condi', 'bcnt', 'dcnt', 'bvsd', 'dvsb']))
                    except Exception as e:
                        pass

                    if i >= 7 and fileSize > 40 and trees.iloc[row].branch >= 4:
                        if i >= 2:  # 초기 1,2,3레벨일때는 데이터 셋이 크니까 없애버려
                            leaf2 = trees.iloc[row].branch ** (i - 2)
                            try:
                                for l in range(1, leaf2 + 1):
                                    # delattr(mod, 'lvl_{}_{}'.format(i-2, l))
                                    if os.path.isfile(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i - 2,
                                                j)):
                                        os.remove(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                                i - 2,
                                                j))
                            except Exception as e:
                                pass

                        if j >= trees.iloc[row].branch and j % trees.iloc[row].branch == 0:
                            try:
                                # delattr(mod, 'lvl_{}_{}'.format(i - 1, j // branch))
                                if os.path.isfile(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                            i - 1,
                                            j // trees.iloc[row].branch)):
                                    os.remove(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(
                                            i - 1,
                                            j // trees.iloc[row].branch))
                            except Exception as e:
                                print(e)
                                pass
                            
                            try:
                                delattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch))
                            except Exception as e:
                                print(e)
                                pass


                        if i == level:
                            try:
                                # delattr(mod, 'lvl_{}_{}'.format(i, j - branch + 1))
                                if os.path.isfile(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(i,
                                                                                                                   j - trees.iloc[row].branch + 1)):
                                    os.remove(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name + '/lvl_{}_{}.pkl'.format(i,
                                                                                                                   j - trees.iloc[row].branch + 1))
                            except Exception as e:
                                pass
                    else:
                        if i >= 2:  # 초기 1,2,3레벨일때는 데이터 셋이 크니까 없애버려
                            leaf2 = trees.iloc[row].branch ** (i - 2)
                            try:
                                for l in range(1, leaf2 + 1):
                                    delattr(mod, 'lvl_{}_{}'.format(i-2, l))
                            except Exception as e:
                                pass

                        if j >= trees.iloc[row].branch and j % trees.iloc[row].branch == 0:
                            try:
                                delattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch))
                            except Exception as e:
                                print(e)
                                pass

                        if i == level:
                            try:
                                delattr(mod, 'lvl_{}_{}'.format(i, j - trees.iloc[row].branch + 1))
                            except Exception as e:
                                pass
                        

    return fResultT

def makeMatrixPM(data, aggr_df, conData, leafType, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum, condition, datBool, lvl, ratDf, invYn, treeNm):
    # data = tmp[8]
    # aggr_df = tmp[9]
    # conData = tmp[1]
    # leafType = 'B'
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
    # invYn = 'N'
    # treeNm = 'tree1'
    # -----------------------------------------
    # data = tmp[8][~tmpInv[0]]
    # aggr_df = tmp[8][~tmpInv[0]]['pur_gubn5']
    # conData = tmpInv[1]
    # leafType = 'A'
    # rRatYn = 'Y'
    # rRatNum = 1
    # rRtYn = 'Y'
    # rRtLvl = 3
    # rRtNum = 1
    # serial = 2
    # branch = 4
    # lvl = 2
    # condition = tmpInv[6]
    # datBool = ~tmpInv[0]
    # ratDf = ''
    # invYn = 'Y'
    # treeNm = 'tree1'
    try:
        prevDcnt = data.iloc[:, 2].value_counts()[0]
    except Exception as e:
        prevDcnt = 0.8

    try:
        prevBcnt = data.iloc[:, 2].value_counts()[1]
    except Exception as e:
        prevBcnt = 0.8

    try:
        if len(data) > 0:
            meltDt = data.iloc[:, 2:data.shape[1]].melt(id_vars='pur_gubn5')
            meltSummary = pd.DataFrame(meltDt.value_counts().reset_index(name='counts')).sort_values(
                ['variable', 'value'])
            tmpFinal = meltSummary.pivot(index=['variable', 'value'], columns='pur_gubn5', values='counts')

            tmpFinal.columns = tmpFinal.columns.values
            tmpFinal.reset_index(level=0, inplace=True)
            tmpFinal.reset_index(level=0, inplace=True)

            tmpFinal.columns = ['cal', 'tree', 'dcnt', 'bcnt']

            tmpFinal = tmpFinal.fillna(0.8)
            # tmpFinal[tmpFinal['bcnt'] == 0]['bcnt'] = 0.8
            # tmpFinal[tmpFinal['dcnt'] == 0]['dcnt'] = 0.8

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
            # 가지별로 계산하는 것을 makeMatrixPM에서 처리하겠음
            if treeNm == 'tree1':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('bvsd', ascending=False)
                    else:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / bcnt asc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.3]
                    midSet = midSet.sort_values('rrr', ascending=False)
            elif treeNm == 'tree2':
                if leafType == 'A':
                    if lvl == 1:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / bcnt asc
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('bcnt')
                    else:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / bcnt asc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.3]
                    midSet = midSet.sort_values('bcnt')
            elif treeNm == 'tree3':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt']]
                        midSet = midSet.sort_values('dcnt', ascending=False)
                    else:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt']]
                    midSet = midSet.sort_values('dcnt', ascending=False)
            elif treeNm == 'tree4':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet.sort_values('bcnt', ascending=False)
                    else:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet.sort_values('bcnt', ascending=False)
            elif treeNm == 'tree5':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[midSet['bcnt'] <= (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.25]
                        midSet = midSet.sort_values('dcnt', ascending=False)
                    else:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['bcnt'] <= (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.25]
                    midSet = midSet.sort_values('dcnt', ascending=False)
            elif treeNm == 'tree6':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[midSet['rRat'] > 20]
                        midSetChk = midSet[midSet['rRt'] > midSet['dvsb']]

                        if midSetChk['dvsb'].count() > 0:
                            midSet = midSet.sort_values('rRt', ascending=False)
                        else:
                            midSet = midSet.sort_values('sRt', ascending=False)
                    else:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['rRat'] > 20]
                    midSetChk = midSet[midSet['rRt'] > midSet['dvsb']]

                    if midSetChk['dvsb'].count() > 0:
                        midSet = midSet.sort_values('rRt', ascending=False)
                    else:
                        midSet = midSet.sort_values('sRt', ascending=False)
            elif treeNm == 'tree7':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[midSet['rRat'] > 35]
                        midSet = midSet.sort_values('rRt', ascending=False)
                    else:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    if lvl > 0 and lvl <= 2:
                        midSet = midSet[midSet['rRat'] > 35]
                    elif lvl > 2 and lvl <= 4:
                        midSet = midSet[midSet['rRat'] > 25]
                    elif lvl > 4 and lvl <= 6:
                        midSet = midSet[midSet['rRat'] > 15]
                    elif lvl > 6:
                        midSet = midSet[midSet['rRat'] > 5]

                    midSet = midSet.sort_values('rRt', ascending=False)
            elif treeNm == 'tree8':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[midSet['rRat'] > 40]
                        midSet = midSet.sort_values('sRt', ascending=False)
                    else:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    if lvl > 0 and lvl <= 2:
                        midSet = midSet[midSet['rRat'] > 40]
                    elif lvl > 2 and lvl <= 4:
                        midSet = midSet[midSet['rRat'] > 30]
                    elif lvl > 4 and lvl <= 6:
                        midSet = midSet[midSet['rRat'] > 20]
                    elif lvl > 6:
                        midSet = midSet[midSet['rRat'] > 7]

                    midSet = midSet.sort_values('sRt', ascending=False)

            # A,B,C,D 공통 시작
            conH = midSet.iloc[0]['tree']
            rConH = midSet.iloc[0]['condi']
            valH = midSet.iloc[0]['value']
            calX = midSet.iloc[0]['cal_x']
            calH = midSet.iloc[0]['cal_y']

            tmpBool = (data[conH] == calX)

            if lvl == 1 and invYn == 'N':
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
            conTmp = conData[~((conData['colname'] == midSet.iloc[0]['tree']))]
            data = data.drop(conH, axis=1)

            # 중복조건 있는지 확인
            if leafType == 'B' or leafType == 'C' or leafType == 'D':
                condition, tmpBool = chkDupCond(data, midSet, condition, tmpBool)

        else:
            tmpBool = pd.DataFrame()
            tmpRt = pd.DataFrame([(0.8, 0.8, 0, 0)], columns=['bcnt', 'dcnt', 'bvsd', 'dvsb'])
            conTmp = conData
    except Exception as e:
        tmpBool = pd.DataFrame()
        tmpRt = pd.DataFrame([(0.8, 0.8, 0, 0)], columns=['bcnt', 'dcnt', 'bvsd', 'dvsb'])
        conTmp = conData

    return data, aggr_df, tmpRt, tmpBool, condition, conTmp, midSet

def drawTreePM(data, level, limitRatio, initConds, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum):
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

    fResult = pd.DataFrame()

    trees = pd.DataFrame([('tree1', 2)], columns=('treeNm', 'branch'))
    trees = trees.append(pd.DataFrame([('tree2', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree3', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree4', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree5', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree6', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree7', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree8', 2)], columns=('treeNm', 'branch')))
    # row = 0
    # i = 2 j = 2 row = 0
    try:
        for row in range(0, trees.shape[0]):
            for i in range(0, level + 1):
                leaf = trees.iloc[row].branch ** i

                if i == 0:
                    data['x'] = True
                    datBool = data['x']
                    data = data.drop('x', axis=1)
                    setattr(mod, 'lvl_0_1', [datBool, initConds, 0.8, 0.8, 0.8, 0.8, '', '', data, data.iloc[:, 2]])
                else:
                    for j in range(1, leaf + 1):
                        # -----------------------------------------------------------------------------------------------
                        print('[' + datetime.datetime.today().strftime(
                            '%Y-%m-%d %H:%M:%S') + '] Lvl_' + str(i) + '_' + str(j))
                        # -----------------------------------------------------------------------------------------------
                        if j % trees.iloc[row].branch == 1:
                            tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            print('[' + datetime.datetime.today().strftime(
                                '%Y-%m-%d %H:%M:%S') + '] Lvl_' + str(i) + '_' + str(j) + ' - ' + trees.iloc[
                                      row].treeNm + ' / prevBcnt : ' + str(
                                prevBcnt) + ' / prevDcnt : ' + str(
                                prevDcnt))
                            tmpNxtRat = pd.DataFrame()
                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrixPM(
                                tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmp[6], tmp[0], i, tmp[7], 'N', trees.iloc[row].treeNm)
                            tmpNxtRat = tmpRatList
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrixPM(
                                tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)

                        if i >= 3:  # 초기 1,2,3레벨일때는 데이터 셋이 크니까 없애버려
                            setattr(mod, 'lvl_{}_{}'.format(i, j),
                                    [pd.DataFrame(), pd.DataFrame(), tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb,
                                     tmpCon,
                                     tmpRatList, pd.DataFrame(), pd.DataFrame()])

                        if i != level:
                            # print('here2!!')
                            setattr(mod, 'lvl_{}_{}'.format(i, j),
                                    [tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon,
                                     tmpRatList, tmpDat, tmpAggrDf])
                        else:  # 마지막 레벨일때는 램을 많이 먹는 데이터셋은 저장하지 않음
                            # print('here3!!')
                            setattr(mod, 'lvl_{}_{}'.format(i, j),
                                    [pd.DataFrame(), pd.DataFrame(), tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb,
                                     tmpCon,
                                     tmpRatList, pd.DataFrame(), pd.DataFrame()])

            # mod = sys.modules[__name__]
            for i in range(0, level + 1):
                leaf = trees.iloc[row].branch ** i
                for j in range(1, leaf + 1):
                    tmp = getattr(mod, 'lvl_{}_{}'.format(i, j))
                    # print('lvl_'+str(i)+'_'+str(j))
                    # print(tmp[3].value_counts('pur_gubn5')[0])
                    try:
                        bcnt = float(tmp[2])
                        if bcnt == 0:
                            bcnt = 0.8
                    except Exception as e:
                        bcnt = 0.8
                        pass

                    try:
                        dcnt = float(tmp[3])
                        if dcnt == 0:
                            dcnt = 0.8
                    except Exception as e:
                        dcnt = 0.8
                        pass

                    fResult = fResult.append(
                        pd.DataFrame([('lvl_' + str(i) + '_' + str(j), tmp[6], bcnt, dcnt,
                                       bcnt / dcnt, dcnt / bcnt)],
                                     columns=['lvl', 'condi', 'bcnt', 'dcnt', 'bvsd', 'dvsb']))
                    # 모든 변수 삭제
                    delattr(mod, 'lvl_{}_{}'.format(i, j))
    except Exception as e:
        print(e)
        setattr(mod, 'lvl_{}_{}'.format(i, j),
                [tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon, tmpRatList, tmpDat,
                 tmpAggrDf])

    return fResult

def makeMatrixPMN(data, aggr_df, conData, leafType, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum, condition, datBool, lvl, ratDf, invYn, treeNm):
    # data = tmp[8]
    # aggr_df = tmp[9]
    # conData = tmp[1]
    # leafType = 'B'
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
    # invYn = 'N'
    # treeNm = 'tree1'
    # -----------------------------------------
    # data = tmp[8][~tmpInv[0]]
    # aggr_df = tmp[8][~tmpInv[0]]['pur_gubn5']
    # conData = tmpInv[1]
    # leafType = 'A'
    # rRatYn = 'Y'
    # rRatNum = 1
    # rRtYn = 'Y'
    # rRtLvl = 3
    # rRtNum = 1
    # serial = 2
    # branch = 4
    # lvl = 2
    # condition = tmpInv[6]
    # datBool = ~tmpInv[0]
    # ratDf = ''
    # invYn = 'Y'
    # treeNm = 'tree1'
    try:
        prevDcnt = data.iloc[:, 2].value_counts()[0]
    except Exception as e:
        prevDcnt = 0.8

    try:
        prevBcnt = data.iloc[:, 2].value_counts()[1]
    except Exception as e:
        prevBcnt = 0.8

    try:
        if len(data) > 0:
            meltDt = data.iloc[:, 2:data.shape[1]].melt(id_vars='pur_gubn5')
            meltSummary = pd.DataFrame(meltDt.value_counts().reset_index(name='counts')).sort_values(
                ['variable', 'value'])
            tmpFinal = meltSummary.pivot(index=['variable', 'value'], columns='pur_gubn5', values='counts')

            tmpFinal.columns = tmpFinal.columns.values
            tmpFinal.reset_index(level=0, inplace=True)
            tmpFinal.reset_index(level=0, inplace=True)

            tmpFinal.columns = ['cal', 'tree', 'dcnt', 'bcnt']
            tmpFinal = tmpFinal.fillna(0.8)
            # tmpFinal[tmpFinal['bcnt'] == 0]['bcnt'] = 0.8
            # tmpFinal[tmpFinal['dcnt'] == 0]['dcnt'] = 0.8

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
            # 가지별로 계산하는 것을 makeMatrixPMN에서 처리하겠음
            if treeNm == 'tree1':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet.sort_values('dvsb', ascending=False)

                        conH = midSet.iloc[0]['tree']
                        rConH = midSet.iloc[0]['condi']
                        valH = midSet.iloc[0]['value']
                        calX = midSet.iloc[0]['cal_x']
                        calH = midSet.iloc[0]['cal_y']
                    else:
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.3]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('bcnt')
            elif treeNm == 'tree2':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)

                        conH = midSet.iloc[1]['tree']
                        rConH = midSet.iloc[1]['condi']
                        valH = midSet.iloc[1]['value']
                        calX = midSet.iloc[1]['cal_x']
                        calH = midSet.iloc[1]['cal_y']
                    else:
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.22]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt']]
                    midSet = midSet.sort_values('dcnt', ascending=False)
            elif treeNm == 'tree3':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.4]
                        midSet = midSet.sort_values('dvsb', ascending=False)

                        conH = midSet.iloc[0]['tree']
                        rConH = midSet.iloc[0]['condi']
                        valH = midSet.iloc[0]['value']
                        calX = midSet.iloc[0]['cal_x']
                        calH = midSet.iloc[0]['cal_y']
                    else:
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.23]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet.sort_values('bcnt', ascending=False)
            elif treeNm == 'tree4':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.4]
                        midSet = midSet.sort_values('dvsb', ascending=False)

                        conH = midSet.iloc[1]['tree']
                        rConH = midSet.iloc[1]['condi']
                        valH = midSet.iloc[1]['value']
                        calX = midSet.iloc[1]['cal_x']
                        calH = midSet.iloc[1]['cal_y']
                    else:
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet.sort_values('bcnt')
            elif treeNm == 'tree5':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet.sort_values('bcnt', ascending=False)

                        conH = midSet.iloc[0]['tree']
                        rConH = midSet.iloc[0]['condi']
                        valH = midSet.iloc[0]['value']
                        calX = midSet.iloc[0]['cal_x']
                        calH = midSet.iloc[0]['cal_y']
                    else:
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.24]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['bcnt'] <= (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.25]
                    midSet = midSet.sort_values('dcnt', ascending=False)
            elif treeNm == 'tree6':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet.sort_values('bcnt', ascending=False)

                        conH = midSet.iloc[1]['tree']
                        rConH = midSet.iloc[1]['condi']
                        valH = midSet.iloc[1]['value']
                        calX = midSet.iloc[1]['cal_x']
                        calH = midSet.iloc[1]['cal_y']
                    else:
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.25]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['rRat'] > 20]
                    midSetChk = midSet[midSet['rRt'] > midSet['dvsb']]

                    if midSetChk['dvsb'].count() > 0:
                        midSet = midSet.sort_values('rRt', ascending=False)
                    else:
                        midSet = midSet.sort_values('sRt', ascending=False)
            elif treeNm == 'tree7':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet.sort_values('bcnt')

                        conH = midSet.iloc[0]['tree']
                        rConH = midSet.iloc[0]['condi']
                        valH = midSet.iloc[0]['value']
                        calX = midSet.iloc[0]['cal_x']
                        calH = midSet.iloc[0]['cal_y']
                    else:
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.26]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    if lvl > 0 and lvl <= 2:
                        midSet = midSet[midSet['rRat'] > 35]
                    elif lvl > 2 and lvl <= 4:
                        midSet = midSet[midSet['rRat'] > 25]
                    elif lvl > 4 and lvl <= 6:
                        midSet = midSet[midSet['rRat'] > 15]
                    elif lvl > 6:
                        midSet = midSet[midSet['rRat'] > 5]

                    midSet = midSet.sort_values('rRt', ascending=False)
            elif treeNm == 'tree8':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet.sort_values('dcnt', ascending=False)

                        conH = midSet.iloc[0]['tree']
                        rConH = midSet.iloc[0]['condi']
                        valH = midSet.iloc[0]['value']
                        calX = midSet.iloc[0]['cal_x']
                        calH = midSet.iloc[0]['cal_y']
                    else:
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.27]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    if lvl > 0 and lvl <= 2:
                        midSet = midSet[midSet['rRat'] > 40]
                    elif lvl > 2 and lvl <= 4:
                        midSet = midSet[midSet['rRat'] > 30]
                    elif lvl > 4 and lvl <= 6:
                        midSet = midSet[midSet['rRat'] > 20]
                    elif lvl > 6:
                        midSet = midSet[midSet['rRat'] > 7]

                    midSet = midSet.sort_values('sRt', ascending=False)
            elif treeNm == 'tree9':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet.sort_values('dcnt', ascending=False)

                        conH = midSet.iloc[1]['tree']
                        rConH = midSet.iloc[1]['condi']
                        valH = midSet.iloc[1]['value']
                        calX = midSet.iloc[1]['cal_x']
                        calH = midSet.iloc[1]['cal_y']
                    else:
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.28]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['bcnt'] <= (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.25]
                    midSet = midSet.sort_values('dcnt', ascending=False)

            if (lvl > 1 and leafType == 'A') or leafType == 'B':
                conH = midSet.iloc[0]['tree']
                rConH = midSet.iloc[0]['condi']
                valH = midSet.iloc[0]['value']
                calX = midSet.iloc[0]['cal_x']
                calH = midSet.iloc[0]['cal_y']

            tmpBool = (data[conH] == calX)

            if lvl == 1 and invYn == 'N':
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
            conTmp = conData[~((conData['colname'] == midSet.iloc[0]['tree']))]
            data = data.drop(conH, axis=1)

            # 중복조건 있는지 확인
            if leafType == 'B' or leafType == 'C' or leafType == 'D':
                condition, tmpBool = chkDupCond(data, midSet, condition, tmpBool)

        else:
            tmpBool = pd.DataFrame()
            tmpRt = pd.DataFrame([(0.8, 0.8, 0, 0)], columns=['bcnt', 'dcnt', 'bvsd', 'dvsb'])
            conTmp = conData
    except Exception as e:
        tmpBool = pd.DataFrame()
        tmpRt = pd.DataFrame([(0.8, 0.8, 0, 0)], columns=['bcnt', 'dcnt', 'bvsd', 'dvsb'])
        conTmp = conData

    return data, aggr_df, tmpRt, tmpBool, condition, conTmp, midSet

def drawTreePMN(data, level, limitRatio, initConds, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum):
    #####################################################################################
     # 트리를 그리는 로직
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

    fResult = pd.DataFrame()

    trees = pd.DataFrame([('tree1', 2)], columns=('treeNm', 'branch'))
    trees = trees.append(pd.DataFrame([('tree2', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree3', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree4', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree5', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree6', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree7', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree8', 2)], columns=('treeNm', 'branch')))
    # row = 0
    # i = 1 j = 2 row = 0
    try:
        for row in range(0, trees.shape[0]):
            for i in range(0, level + 1):
                leaf = trees.iloc[row].branch ** i

                if i == 0:
                    data['x'] = True
                    datBool = data['x']
                    data = data.drop('x', axis=1)
                    setattr(mod, 'lvl_0_1', [datBool, initConds, 0.8, 0.8, 0.8, 0.8, '', '', data, data.iloc[:, 2]])
                else:
                    for j in range(1, leaf + 1):
                        # -----------------------------------------------------------------------------------------------
                        print('[' + datetime.datetime.today().strftime(
                            '%Y-%m-%d %H:%M:%S') + '] Lvl_' + str(i) + '_' + str(j))
                        # -----------------------------------------------------------------------------------------------
                        if j % trees.iloc[row].branch == 1:
                            tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))
                            # tmp = getattr(mod, 'lvl_0_1')
                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            print('[' + datetime.datetime.today().strftime(
                                '%Y-%m-%d %H:%M:%S') + '] Lvl_' + str(i) + '_' + str(j) + ' - ' + trees.iloc[
                                      row].treeNm + ' / prevBcnt : ' + str(
                                prevBcnt) + ' / prevDcnt : ' + str(
                                prevDcnt))
                            tmpNxtRat = pd.DataFrame()
                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrixPMN(
                                tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmp[6], tmp[0], i, tmp[7], 'N', trees.iloc[row].treeNm)
                            tmpNxtRat = tmpRatList
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrixPMN(
                                tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)

                        if i >= 3:  # 초기 1,2,3레벨일때는 데이터 셋이 크니까 없애버려
                            setattr(mod, 'lvl_{}_{}'.format(i, j),
                                    [pd.DataFrame(), pd.DataFrame(), tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb,
                                     tmpCon,
                                     tmpRatList, pd.DataFrame(), pd.DataFrame()])

                        if i != level:
                            # print('here2!!')
                            setattr(mod, 'lvl_{}_{}'.format(i, j),
                                    [tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon,
                                     tmpRatList, tmpDat, tmpAggrDf])
                        else:  # 마지막 레벨일때는 램을 많이 먹는 데이터셋은 저장하지 않음
                            # print('here3!!')
                            setattr(mod, 'lvl_{}_{}'.format(i, j),
                                    [pd.DataFrame(), pd.DataFrame(), tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb,
                                     tmpCon,
                                     tmpRatList, pd.DataFrame(), pd.DataFrame()])

            # mod = sys.modules[__name__]
            for i in range(0, level + 1):
                leaf = trees.iloc[row].branch ** i
                for j in range(1, leaf + 1):
                    tmp = getattr(mod, 'lvl_{}_{}'.format(i, j))
                    # print('lvl_'+str(i)+'_'+str(j))
                    # print(tmp[3].value_counts('pur_gubn5')[0])
                    try:
                        bcnt = float(tmp[2])
                        if bcnt == 0:
                            bcnt = 0.8
                    except Exception as e:
                        bcnt = 0.8
                        pass

                    try:
                        dcnt = float(tmp[3])
                        if dcnt == 0:
                            dcnt = 0.8
                    except Exception as e:
                        dcnt = 0.8
                        pass

                    fResult = fResult.append(
                        pd.DataFrame([('lvl_' + str(i) + '_' + str(j), tmp[6], bcnt, dcnt,
                                       bcnt / dcnt, dcnt / bcnt)],
                                     columns=['lvl', 'condi', 'bcnt', 'dcnt', 'bvsd', 'dvsb']))
                    # 모든 변수 삭제
                    delattr(mod, 'lvl_{}_{}'.format(i, j))
    except Exception as e:
        print(e)
        setattr(mod, 'lvl_{}_{}'.format(i, j),
                [tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon, tmpRatList, tmpDat,
                 tmpAggrDf])

    return fResult

def checkCondition(data, condition):
    condition = pd.DataFrame(condition.split(' AND '))

    for i in range(0, condition.shape[0]):
        if condition.iloc[i,0].find('<=') > 0:
            con = condition.iloc[i,0].split('<=')[0]
            con = str(con).replace(' ','')

            try:
                val = float(condition.iloc[i,0].split('<=')[1])
                data = data[data[con] <= val]
            except Exception as e:
                val = condition.iloc[i,0].split('<=')[1]
                val = str(val).replace(' ', '')
                data = data[data[con] <= data[val]]
                pass

        else:
            con = condition.iloc[i,0].split('>')[0]
            con = str(con).replace(' ', '')

            try:
                val = float(condition.iloc[i,0].split('>')[1])
                data = data[data[con] > val]
            except Exception as e:
                val = condition.iloc[i,0].split('>')[1]
                val = str(val).replace(' ', '')
                data = data[data[con] > data[val]]
                pass

    return print(data.value_counts('pur_gubn5'))

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

def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)
        pass

def run(newDff, paramlvl, paramLimitRatio, dfConF, paramrRatYn, paramrRatNum, paramrRtYn, paramrRtLvl, paramrRtNum, name, fileSize):
    createFolder('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name)
    removeAllFile('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/' + name)

    # 실행 구문
    fResult_md = drawTree(newDff, paramlvl, paramLimitRatio, dfConF, paramrRatYn, paramrRatNum, paramrRtYn, paramrRtLvl, paramrRtNum, fileSize)
    fResult_pm = drawTreePM(newDff, paramlvl, paramLimitRatio, dfConF, paramrRatYn, paramrRatNum, paramrRtYn, paramrRtLvl, paramrRtNum)
    fResult_pmn = drawTreePMN(newDff, paramlvl, paramLimitRatio, dfConF, paramrRatYn, paramrRatNum, paramrRtYn, paramrRtLvl, paramrRtNum)

    fResult = fResult_md
    fResult = fResult.append(fResult_pm)
    fResult = fResult.append(fResult_pmn)

    fResult = fResult.iloc[:, 1:fResult.shape[1]].drop_duplicates()
    fResult = fResult.sort_values('dvsb', ascending=False)

    return fResult

def run2(data):
    # data = pd.read_csv('C:/Users/Shine_anal/PycharmProjects/anlaysis/kbuy1_20210927_parallel_all.csv')
    # data = data[(data['dvsb'] > condDcnt) & (data['dcnt'] > condDvsb)]
    data['conLevel'] = 0
    data['dupYn'] = 'N'

    data = data.drop_duplicates()
    data = data.reset_index(drop=True)

    for i in range(0, data.shape[0]):
        try:
            data['conLevel'][i] = pd.DataFrame(data['condi'][i].split(' AND ')).shape[0]
        except Exception as e:
            data['conLevel'][i] = 0
            pass

    data = data.sort_values(by=['conLevel', 'condi'], ascending=[True, True])
    data = data.reset_index(drop=True)

    for j in range(0, data.shape[0] - 1):
        for k in range(j + 1, data.shape[0]):
            print('j : ' + str(j) + ' k : ' + str(k))
            try:
                if (data['condi'][j] in data['condi'][k]) and (data['conLevel'][j] < data['conLevel'][k]):
                    # print('j : (' + str(j) + ')' + data['condi'][j] + ' / k : (' + str(k) + ')' + data['condi'][k] )
                    data['dupYn'][j] = 'Y'
            except Exception as e:
                pass

    return data[data['dupYn'] == 'N']

def similarDel(result, data):
    result = midSet
    data = df
    modResult = pd.DataFrame()
    modResultEtc = pd.DataFrame()

    for b in range(0, result.shape[0]):
        if if_float(result.iloc[b]['value']):
            modResult = modResult.append(result.iloc[b])
        else:
            modResultEtc = modResultEtc.append(result.iloc[b])
            modResultEtc['chkYn'] = 'Y'

    result = modResult

    result['chkYn'] = 'Y'
    resultGt = result[(result['cal_y'] == 'GT')].sort_values(by=['condi', 'bcnt'],
                                                           ascending=[False, False]).drop_duplicates()
    resultLt = result[(result['cal_y'] == 'LT')].sort_values(by=['condi', 'bcnt'],
                                                           ascending=[False, False]).drop_duplicates()
    resultGt = resultGt.reset_index(drop=True)
    resultLt = resultLt.reset_index(drop=True)

    finalSet = pd.DataFrame()

    for y in data.iloc[:, 14:data.shape[1]].columns:
        # for y in data.iloc[:, 14:50].columns:
        # y = 'Adx'
        x = resultGt[(resultGt['condi'] == y)]
        x = x.reset_index(drop=True)
        chkNum = 0
        print('[' + datetime.datetime.today().strftime(
            '%Y-%m-%d %H:%M:%S') + '] condi : ' + y)

        if x.shape[0] > 2:
            for j in range(0, x.shape[0] - 1):
                if chkNum > 5 or j == 0:
                    chkNum = 0

                chkNum = chkNum + (x.iloc[j].dcnt - x.iloc[j + 1].dcnt) / x.iloc[j].dcnt * 100

                try:
                    if chkNum > 5 or j == 0:
                        x['chkYn'][j] = 'Y'
                    else:
                        x['chkYn'][j] = 'N'
                except Exception as e:
                    x['chkYn'][j] = 'N'

                finalSet = finalSet.append(x.iloc[j])
        else:
            for j in range(0, x.shape[0] - 1):
                finalSet = finalSet.append(x.iloc[j])


    for d in data.iloc[:, 14:data.shape[1]].columns:
        # for d in data.iloc[:, 14:50].columns:
        z = resultLt[(resultLt['condi'] == d)]
        z = z.reset_index(drop=True)
        print('[' + datetime.datetime.today().strftime(
            '%Y-%m-%d %H:%M:%S') + '] condi : ' + d)
        chkNum = 0

        if z.shape[0] > 2:
            for j in range(0, z.shape[0] - 1):
                if chkNum > 5 or j == 0:
                    chkNum = 0

                chkNum = chkNum + (z.iloc[j].dcnt - z.iloc[j + 1].dcnt) / z.iloc[j].dcnt * 100

                if chkNum > 5 or j == 0:
                    z['chkYn'][j] = 'Y'
                else:
                    z['chkYn'][j] = 'N'
                # print((z.iloc[j].bcnt - z.iloc[j+1].bcnt) / z.iloc[j].bcnt * 100)
                # print(z['condi'][j])
                finalSet = finalSet.append(z.iloc[j])
        else:
            for j in range(0, z.shape[0] - 1):
                finalSet = finalSet.append(z.iloc[j])

    finalSet = finalSet[finalSet['chkYn'] == 'Y']
    finalSet = finalSet.append(modResultEtc)

    return finalSet

def removeAllFile(directory):
    if os.path.exists(directory):
        for file in os.scandir(directory):
            os.remove(file.path)

def checkConditionCnt(data, condition):
    # data = df
    # condition = finalData.iloc[1]['condi']
    rtnCon = condition
    condition = pd.DataFrame(condition.split(' AND '))
    
    for i in range(0, condition.shape[0]):
        if condition.iloc[i,0].find(' OR ') < 0:
            if condition.iloc[i,0].find('<=') > 0:
                con = condition.iloc[i,0].split('<=')[0]
                con = str(con).replace(' ','')
                
                try:
                    if if_float(condition.iloc[i,0].split('<=')[1]):
                        val = float(condition.iloc[i,0].split('<=')[1])
                        data = data[data[con] <= val]
                    else:
                        val = condition.iloc[i,0].split('<=')[1]
                        val = str(val).replace(' ', '')
                        data = data[data[con] <= data[val]]
                except Exception as e:
                    val = condition.iloc[i,0].split('<=')[1]
                    val = str(val).replace(' ', '')
                    data = data[data[con] <= data[val]]
                    pass

            else:
                con = condition.iloc[i,0].split('>')[0]
                con = str(con).replace(' ', '')

                try:
                    if if_float(condition.iloc[i,0].split('>')[1]):
                        val = float(condition.iloc[i,0].split('>')[1])
                        data = data[data[con] > val]
                    else:
                        val = condition.iloc[i,0].split('>')[1]
                        val = str(val).replace(' ', '')
                        data = data[data[con] > data[val]]
                except Exception as e:
                    val = condition.iloc[i,0].split('>')[1]
                    val = str(val).replace(' ', '')
                    data = data[data[con] > data[val]]
                    pass

    return data, rtnCon

def checkConditionPrevRun2(finalData, data, condDcnt, condDvsb):
    finalData = finalData[(finalData['dvsb'] > condDcnt) & (finalData['dcnt'] > condDvsb)]
    rtnSet = pd.DataFrame()
    for i in range(0, finalData.shape[0]):
        # i, data = 0, df
        con = finalData.iloc[i]['condi']
        con = con.replace( '(', '').replace(')', '')

        print('i : ' + str(i) + ' / ' + con)
        
        rtnData, rtnCon = checkConditionCnt(data, con)
        try:
            dcnt = rtnData.value_counts('pur_gubn5')[0]
        except Exception as e:
            dcnt = 0.8

        try:
            bcnt = rtnData.value_counts('pur_gubn5')[1]
        except Exception as e:
            bcnt = 0.8

        if finalData.iloc[i]['bcnt'] == bcnt and finalData.iloc[i]['dcnt'] == dcnt:
            rtnSet = rtnSet.append(finalData.iloc[i])
    
    return rtnSet

###############################################################################################################################
# 파라미터 세팅
###############################################################################################################################
name = 'sbuy2cmart4_colm'   #name setting
paramLimitRatio = 0.5

paramlvl        = 8
paramrRatYn     = 'Y'
paramrRatNum    = 1
paramrRtYn      = 'Y'
paramrRtLvl     = 3
paramrRtNum     = 1

###############################################################################################################################
# Data Loading
###############################################################################################################################
# data source파일을 로딩한다..
# createFolder('C:/Users/Shine_anal/Desktop/inobuyt/' + name)
df = pd.read_csv('C:/Users/Shine_anal/Desktop/inobuyt/' + name + '_d_close_h4_9year_202009.csv')
fileSize = os.path.getsize('C:/Users/Shine_anal/Desktop/inobuyt/' + name + '_d_close_h4_9year_202009.csv') / 1024 / 1024

# str을 가지고 만들어낸 csv파일을 로딩한다.. 
dfCon = pd.read_csv('C:/Users/Shine_anal/Desktop/inobuyt/' + name + '_str.csv', header=None)
dfCon = dfCon.drop_duplicates()
###############################################################################################################################
# 조건 이니셜라이즈 del(str)
###############################################################################################################################
newDff, dfConF = initCond(df, dfCon)
###############################################################################################################################
# 실행
###############################################################################################################################
fResult = run(newDff, paramlvl, paramLimitRatio, dfConF, paramrRatYn, paramrRatNum, paramrRtYn, paramrRtLvl, paramrRtNum, name, fileSize)
###############################################################################################################################
# 실제 데이터에 대입하여 조건식들이 맞는지 확인
###############################################################################################################################
fResult2 = checkConditionPrevRun2(fResult, df, 9, 10) # 실행결과,원 데이터,condDcnt, condDvsb를 넣는다..
###############################################################################################################################
# 상위레벨 조건 지우기 
###############################################################################################################################
# fResult = pd.read_csv('C:/Users/Shine_anal/PycharmProjects/anlaysis/dtree_inobuyt1.csv')
finalData = run2(fResult2) # 실행결과
finalData.to_csv('C:/Users/Shine_anal/Desktop/inobuyt/dtree_' + name +'_final_{}.csv'.format(datetime.datetime.today().strftime('%m%d%H%M')))

