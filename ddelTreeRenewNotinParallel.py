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

def makeFinalSet(path, name):
    # 결과 합치기
    fResultMid = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTRENEW/" + name + "_result.csv")
    # fResultMid = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULT/ncbuy3_result_original_2.csv")

    tmpL = fResultMid['condi'].drop_duplicates()  # 중복조건 제거
    fResultMid = fResultMid.iloc[tmpL.index]
    fResultMid = fResultMid.iloc[:, 1:fResultMid.shape[1]].drop_duplicates()

    fResultMid = fResultMid.sort_values('condi')  # dvsb가 좋은 순서로 정렬
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

    fResultFin = fResultMid[fResultMid['chk'] == 0]

    fResultFin = fResultFin.sort_values('dvsb', ascending=False)  # dvsb가 좋은 순서로 정렬
    fResultFin.to_csv(path + name + "_ddelTreeRenewParallel_result.csv")

    os.remove("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTRENEW/" + name + "_result.csv")

    return fResultFin


if __name__ == '__main__':
    #####################################################################################################
    # 파라미터 세팅   초기 조건 만들지 안고 계속 20개 가지 치기
    # 가지 내 데이터는 not in하여 만들어내고 각 컬럼별 비율 산출 한 후 조건을 찾는다..
    # 찾은 조건을 원시 데이터에 대입하여 그다음 레벨 데이터를 만들어낸다..
    # 입력한 레벨 -2레벨 까지는 위 조건으로 수행하고 그 이후는 원래 로직으로 수행한다..
    #####################################################################################################
    name = 'sjtabuy'  # 사용자 지정 명
    path = "C:/Users/Shine_anal/Desktop/inott/"  # 사용자 지정명 + _com.csv 파일이 존재하는 폴더 (분석할 csv파일)
    paramLevel = 5  # 돌릴LEVEL수
    paramLimitRatio = 1  # 초기데이터 중 dcnt대비 x% 이상의 가지만 돌리게 하는 조건 (%)
    paramLastRatio = 0.7  # 마지막 레벨에서 dvsb가 x:1 이상인 것만 돌리게 하는 조건
    #####################################################################################################
    ray.init(num_cpus=20, log_to_driver=False)

    ray.shutdown()