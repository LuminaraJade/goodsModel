import oss2
from collections import defaultdict
import random
from datetime import datetime, timedelta
import ossfs
import json
from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as F
import pandas as pd
import pickle
import io
from pyhive import hive
import os
#1222有点问题
# recent_path = '20241222'



# 提取 JSON 字符串中的所有键值对
def extract_all_keys(x):
    if isinstance(x, str):  
        try:
            return json.loads(x)  
        except json.JSONDecodeError:
            return None 
    return None  


def extract_json_columns(column_name, df,json_name):
    extracted_columns = {}

    # 遍历每一行，解析 JSON 并提取所需字段
    for idx, item in df[f'ranked_categories.{column_name}'].items():  
        if pd.notnull(item) and isinstance(item, str):  
            try:
                parsed_json = json.loads(item)
                
                annual_volJson = parsed_json.get(json_name, {})

                extracted_columns[idx] = {
                    f"{column_name}_raw_max": annual_volJson.get('raw_max'),
                    f"{column_name}_raw_min": annual_volJson.get('raw_min'),
                    f"{column_name}_raw_count": annual_volJson.get('raw_count'),
                    f"{column_name}_max_": annual_volJson.get('max_'),
                    f"{column_name}_mean_": annual_volJson.get('mean_'),
                    f"{column_name}_mid_": annual_volJson.get('mid_'),
                    f"{column_name}_min_": annual_volJson.get('min_'),
                    f"{column_name}_count_": annual_volJson.get('count_'),
                    f"{column_name}_group_by": annual_volJson.get('group_by'),
                    f"{column_name}_unit": annual_volJson.get('unit'),
                    f"{column_name}_update_time": annual_volJson.get('update_time')
                }
            except json.JSONDecodeError as e:
                print(f"无法解析 JSON：{item}, 错误：{e}")

    extracted_df = pd.DataFrame.from_dict(extracted_columns, orient='index')
    result_df = df.join(extracted_df)

    return result_df



# 配置 OSS 相关信息
access_key_id = 'LTYa4jzcF'
access_key_secret = 'k1odPBs63uR3Y'
bucket_name = 'mini-dsp'
endpoint = 'https.com'

auth = oss2.Auth(access_key_id, access_key_secret)
bucket = oss2.Bucket(auth, endpoint, bucket_name)


# RECENT_PATH = ['20250108','20250109','20250110','20250111','20250112']
RECENT_PATH = ['20241222','20241223','20241224','20241225','20241226','20241227','20241228','20241229','20241230','20241231','20250101','20250102','20250103','20250104','20250105','20250106','20250107']

# RECENT_PATH = ['20250109']
for recent_path in RECENT_PATH:

    ##读取lable数据
    # 更新目标路径为实际写入的路径
    data_path = f"DataOperation/Results/F_data/lable_data_raw_{recent_path}/"  # 确认文件实际存储路径

    dataframes = []
    for obj in oss2.ObjectIterator(bucket, prefix=data_path):
        if obj.key.endswith(".parquet"):  # 筛选出以 .parquet 结尾的文件
            oss_path = f"oss://{bucket_name}/{obj.key}"
            # 使用 ossfs 读取 Parquet 文件
            fs = ossfs.OSSFileSystem(key=access_key_id, secret=access_key_secret, endpoint=endpoint)
            # 读取 Parquet 文件为 Pandas DataFrame
            df = pd.read_parquet(oss_path, filesystem=fs)
            dataframes.append(df)

    if dataframes:
        lable_data = pd.concat(dataframes, ignore_index=True)
    else:
        lable_data = pd.DataFrame()  # 如果没有匹配文件，返回空 DataFrame



    #读取dua和增长率数据
    data_path = f"DataOperation/Results/F_data/feature_from_dua_and_growth_data_raw_{recent_path}/"  # 确认文件实际存储路径

    # 存放所有 DataFrame 的列表
    dataframes = []

    # 遍历符合路径的文件
    for obj in oss2.ObjectIterator(bucket, prefix=data_path):
        if obj.key.endswith(".parquet"):  
            oss_path = f"oss://{bucket_name}/{obj.key}"
            fs = ossfs.OSSFileSystem(key=access_key_id, secret=access_key_secret, endpoint=endpoint)
            df = pd.read_parquet(oss_path, filesystem=fs)
            dataframes.append(df)

    if dataframes:
        feature_from_dua_data = pd.concat(dataframes, ignore_index=True)
    else:
        feature_from_dua_data = pd.DataFrame()  






    #读取llm数据
    data_path = f"DataOperation/Results/F_data/feature_from_LLM_data_raw_{recent_path}/"  # 确认文件实际存储路径

    dataframes = []

    # 遍历符合路径的文件
    for obj in oss2.ObjectIterator(bucket, prefix=data_path):
        if obj.key.endswith(".parquet"):  
            oss_path = f"oss://{bucket_name}/{obj.key}"
            fs = ossfs.OSSFileSystem(key=access_key_id, secret=access_key_secret, endpoint=endpoint)
            df = pd.read_parquet(oss_path, filesystem=fs)
            dataframes.append(df)

    # 合并所有 DataFrame
    if dataframes:
        feature_from_llm_data = pd.concat(dataframes, ignore_index=True)
    else:
        feature_from_llm_data = pd.DataFrame() 

    # print(feature_from_llm_data.head())



    #读取ici数据
    data_path = f"DataOperation/Results/F_data/ICI_data_raw_{recent_path}/"  # 确认文件实际存储路径

    dataframes = []

    for obj in oss2.ObjectIterator(bucket, prefix=data_path):
        if obj.key.endswith(".parquet"): 
            oss_path = f"oss://{bucket_name}/{obj.key}"
            fs = ossfs.OSSFileSystem(key=access_key_id, secret=access_key_secret, endpoint=endpoint)
            df = pd.read_parquet(oss_path, filesystem=fs)
            dataframes.append(df)

    if dataframes:
        feature_from_ici_data = pd.concat(dataframes, ignore_index=True)
    else:
        feature_from_ici_data = pd.DataFrame() 
        


    lable_data_raw = lable_data
    feature_from_ssr_data = pd.read_csv('feature_from_SSR_data_20241223_13:40.csv')

    lable_dua_merged_data = (
        lable_data
        .merge(feature_from_dua_data, left_on='title', right_on='item_title', how='left')
    )


    lable_dua_merged_data_useful = lable_dua_merged_data.drop(['item_title', 'buy_num_y','click_num_y', 
                                                                'coupons_share_url', 'goods_id', 
                                                                'item_id',
                                                                'title_y', 'update', 'row_num',
                                                                'item_id_temp'],axis=1)

    # lable_dua_merged_data_useful = lable_dua_merged_data


    # 应用函数，解析 JSON 字符串
    feature_from_llm_data['parsed_json'] = feature_from_llm_data['llm_nor'].apply(extract_all_keys)

    keys_to_extract = [
        "category_first", "category_second", "brand_real", "brand_quality",
        "audience", "subj_scenario", "obj_scenario", "time_scenario",
        "dep_scenario", "feature", "category", "scenario", "brand"
    ]

    # 提取每个字段并创建对应的列
    for key in keys_to_extract:
        feature_from_llm_data[key] = feature_from_llm_data['parsed_json'].apply(lambda x: x.get(key) if x else None)
    feature_from_llm_data.drop(columns=['parsed_json'], inplace=True)



    llm_ssr_merged_data = (
        feature_from_llm_data
        .merge(feature_from_ssr_data, left_on='category_second', right_on='ranked_categories.category_name', how='left')
    )

    llm_ssr_merged_data_useful_feature = llm_ssr_merged_data[['item_title','scenario','category','feature','dep_scenario','time_scenario','obj_scenario','subj_scenario','audience','brand_quality','brand_real','category_second','category_first','ranked_categories.annual_voljson','ranked_categories.pricejson','ranked_categories.discountjson','ranked_categories.p_diffjson']]

    data_except_ic = (
        lable_dua_merged_data_useful
        .merge(llm_ssr_merged_data_useful_feature, left_on='title_x', right_on='item_title', how='left')
    )






    data_except_ic = extract_json_columns('annual_voljson',data_except_ic,'annual_volJson')
    data_except_ic = extract_json_columns('pricejson',data_except_ic,'priceJson')
    data_except_ic = extract_json_columns('discountjson',data_except_ic,'discountJson')
    data_except_ic = extract_json_columns('p_diffjson',data_except_ic,'p_diffJson')


    data_except_ic.drop(columns=['annual_voljson_group_by', 'annual_voljson_unit', 'annual_voljson_update_time'], inplace=True)
    data_except_ic.drop(columns=['pricejson_group_by', 'pricejson_unit', 'pricejson_unit'], inplace=True)
    data_except_ic.drop(columns=['discountjson_count_', 'discountjson_group_by', 'discountjson_unit','discountjson_update_time'], inplace=True)
    data_except_ic.drop(columns=['p_diffjson_group_by', 'p_diffjson_unit', 'p_diffjson_update_time'], inplace=True)


    final_data = (
        data_except_ic
        .merge(feature_from_ici_data, left_on='title_x', right_on='title', how='left')
    )

    final_data.drop(columns=['item_title', 'ranked_categories.annual_voljson', 'ranked_categories.pricejson','ranked_categories.discountjson','ranked_categories.p_diffjson','item_id','title'], inplace=True)

    out_put_path = 'z_data_new2/total_raw'
    final_data.to_pickle(os.path.join(out_put_path,f'data_raw_{recent_path}_py.pkl'))
    print(recent_path)






