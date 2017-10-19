# -*- coding: utf-8 -*-

import sys
import re
import collections as cl
import pandas as pd
import chardet
from urllib.request import urlopen
import pydocumentdb
import pydocumentdb.document_client as document_client
sys.path.append('../Util')
import documentdb_util
import date_util


def cleansingMarketOperation():

    documentDbUtil = documentdb_util.DocumentDBUtil()
    dateUtil = date_util.DateUtil()

    date_list = []
    brand_type_list = []
    remaining_years_list = []
    price_list = []
    price_add_count = 0
    condition_list = []

    ### ドキュメントDBから日銀オペ情報を取得
    documents = documentDbUtil.getDocument('DOCUMENTDB_COLLECTION_MARKET_OPERATIONS')

    if (len(documents) < 1):
        print ("document is not found")
    else:
        ### オペの内容の表の項目をループして必要項目を取得
        for doc in documents:
            for item in doc['offer']:

                ### 1列目に国債買入という単語入っていた場合、2列目以降の値を取得
                if price_add_count != 0:
                    i = 0
                    while i < price_add_count:
                        price_list.append(item)
                        i += 1
                    price_add_count = 0
                else:
                    ### date変換
                    date_formated = dateUtil.convertString2Date(doc['date'])

                    ### 国債買入の場合、内容を取得する
                    pattern = r"国債買入"
                    matchOB = re.match(pattern, item)
                    if matchOB:
                        if "国債買入（残存期間１年以下）" == item:
                            for num in [0,1]:
                                date_list.append(date_formated)
                                brand_type_list.append('normal')
                                remaining_years_list.append(num)
                                condition_list.append(item)
                                price_add_count += 1
                        elif "国債買入（残存期間１年超３年以下）" == item:
                            for num in [2,3]:
                                date_list.append(date_formated)
                                brand_type_list.append('normal')
                                remaining_years_list.append(num)
                                condition_list.append(item)
                                price_add_count += 1
                        elif "国債買入（残存期間３年超５年以下）" == item:
                            for num in [4,5]:
                                date_list.append(date_formated)
                                brand_type_list.append('normal')
                                remaining_years_list.append(num)
                                condition_list.append(item)
                                price_add_count += 1
                        elif "国債買入（残存期間５年超１０年以下）" == item:
                            for num in range(6, 10):
                                date_list.append(date_formated)
                                brand_type_list.append('normal')
                                remaining_years_list.append(num)
                                condition_list.append(item)
                                price_add_count += 1
                        elif "国債買入（残存期間１０年超２５年以下）" == item:
                            for num in range(11, 25):
                                date_list.append(date_formated)
                                brand_type_list.append('normal')
                                remaining_years_list.append(num)
                                condition_list.append(item)
                                price_add_count += 1
                        elif "国債買入（残存期間２５年超）" == item:
                            for num in range(26, 900):
                                date_list.append(date_formated)
                                brand_type_list.append('normal')
                                remaining_years_list.append(num)
                                condition_list.append(item)
                                price_add_count += 1
                        elif "国債買入（物価連動債）" == item:
                            date_list.append(date_formated)
                            brand_type_list.append('物価連動債')
                            remaining_years_list.append('-')
                            condition_list.append(item)
                            price_add_count += 1
                        elif "国債買入（固定利回り方式）（残存期間５年超１０年以下）" == item:
                            for num in range(6, 10):
                                date_list.append(date_formated)
                                brand_type_list.append('固定利回り方式')
                                remaining_years_list.append(num)
                                condition_list.append(item)
                                price_add_count += 1
                        elif "国債買入（変動利付債）" == item:
                            date_list.append(date_formated)
                            brand_type_list.append('変動利付債')
                            remaining_years_list.append('-')
                            condition_list.append(item)
                            price_add_count += 1
                        else:
                            print ('Unexpected condtion found!:%s' % item)
    
    ### DICTに詰め替え
    dict4store = []
    for i in range(len(date_list)):
        data = cl.OrderedDict()
        if date_list[i]:
            data['key'] = str(date_list[i]) + '@' + str(remaining_years_list[i])
            data['date'] = str(date_list[i])
            data['brand_type'] = brand_type_list[i]
            data['remaining_years'] = remaining_years_list[i]
            data['price'] = price_list[i]
            data['condition'] = condition_list[i]
            dict4store.append(data)

    ### ドキュメントdbに保存
    # documentDbUtil.store2DocmentDb('DOCUMENTDB_COLLECTION_MARKET_OPERATIONS_CLEANSED', dict4store)

    ### 条件一覧確認
    condition_list_uq = list(set(condition_list))
    print ('Here is the condition list...')
    print (condition_list_uq)

    ### csvファイルに出力（オプション）
    df = pd.DataFrame.from_dict(dict4store)
    df.to_csv('output.csv', encoding='shift_jis')


if __name__ == '__main__':
    cleansingMarketOperation()