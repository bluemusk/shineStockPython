# coding=utf-8
# 라이브러리 불러오기
import pydotplus
from sklearn.tree import export_graphviz
import matplotlib.pyplot as plt
from collections import Counter
import pandas as pd
from sklearn.metrics import accuracy_score
import sklearn.model_selection as ms
import warnings
from sklearn.tree import DecisionTreeClassifier, export_text, export_graphviz
import os
import sklearn
from sklearn import tree
from sklearn.datasets import load_iris
import graphviz
os.environ["PATH"] += os.pathsep + 'C:/Program Files/Graphviz/bin/'

warnings.filterwarnings('ignore')

##############################################################################
# 매수해야하는 timing을 찾아보자
##############################################################################
df = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/SELLCONDFIND.csv")
df = df.fillna(0)
df['result'] = 0

for i in range(0, df.shape[0]):
    if df['RN2'].iloc[i] <= 30:
        df['result'].iloc[i] = 1

### 의사결정 트리 시각화
# 종속변수, 입력변수 나누기

# fncbuy3      33083
# fsbuy2       27926
# fkbuy1       20489
# fkosbuy2     11078
# fsbuy3        6686
# fkosbuy3      6215
# fgkosbuy4     1905
# fncbuy2       1280
# fgkosbuy3      750

df = df[df['LABEL'] == 'fncbuy3']

x = df.iloc[:, 11:df.shape[1]-3]
y = df.iloc[:, -1]

x.info()
y.value_counts()



x_train, x_test, y_train, y_test = ms.train_test_split(x, y,
                                                      test_size = 0.2)

Counter(y_train)
Counter(y_test)

# 의사결정트리 만들기 (엔트로피구하기, 트리2단계)
model = DecisionTreeClassifier(criterion="entropy", max_depth=5)

# 모델 학습
model.fit(x_train, y_train)

# test데이터 라벨 예측
y_pred = model.predict(x_test)

# 정확도 계산
accuracy_score(y_test, y_pred)

# 입력변수들의 중요도 확인(확률값)
model.feature_importances_

importance = pd.DataFrame({'feature': x_train.columns, 'importance': model.feature_importances_})
importance = importance.sort_values('importance', ascending=False)

treeMODEL = DecisionTreeClassifier(criterion = 'entropy',
                             max_depth = 10,
                             random_state = 0).fit(x_train, y_train)

# 시각화
dot_data = tree.export_graphviz(treeMODEL,   # 의사결정나무 모형 대입
                               out_file = None,  # file로 변환할 것인가
                               feature_names = list(x.columns),  # feature 이름
                               class_names = 'result',  # target 이름
                               filled = True,           # 그림에 색상을 넣을것인가
                               rounded = True,          # 반올림을 진행할 것인가
                               special_characters = True)   # 특수문자를 사용하나

graph = graphviz.Source(dot_data)
graph


plotResult = sklearn.tree.plot_tree(treeMODEL,
                                    feature_names = x_train.columns,
                                   filled = True)

#가지치기

tree_prune = DecisionTreeClassifier(criterion = 'entropy',
                             max_depth = 4,
                             random_state = 0).fit(x_train, y_train)

tree_prune.fit = tree_prune.fit(x_train, y_train)

fig, axes = plt.subplots(nrows = 1, ncols = 1, figsize = (10,5))

plt.figure(figsize = (16,9))
plotResult = sklearn.tree.plot_tree(tree_prune,
                                    feature_names = x_train.columns,
                                   filled = True)
####################################################################################################################
data = x
aggr_df = y

result = data.iloc[:, 6:data.shape[1]].apply(lambda x: custom_func(x, aggr_df), axis=0)
final = makeFinalDf(result)

final[final['bcnt'] > 1000].sort_values('bvsd', ascending=False)

df.value_counts('result')
# 0    308383
# 1      9180



df['R_SMA20_C'].quantile(0)
df['R_SMA20_C'].quantile(0.25)
df['R_SMA20_C'].quantile(0.5)
R_SMA20_C
RRR_1A_FC


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