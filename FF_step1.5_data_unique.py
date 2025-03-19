import pandas as pd
import os
from collections import Counter

def clean_data(df):
    original_shape = df.shape[0]  

    # 清洗操作
    df = df.drop_duplicates()  # 去重
    df = df.dropna(subset=['category_name']) 
    df = df.drop_duplicates(subset=['title_x'])  

    cleaned_shape = df.shape[0] 
    print(f"Rows before cleaning: {original_shape}, Rows after cleaning: {cleaned_shape}")
    
    return df


def labelProcess(df):
    # 1. 去除 df 中的 buy_num_x == 1 的行
    df = df[df['buy_num_x'] != 1]
    
    # 2. 增加新的一列，列名 lable_x
    # 3. 这一列的赋值逻辑如下：
    # 如果当前行 buy_num/click_num >= 0.2 & buy_num >= 2 & click_num >= 3 则赋值 1；
    # 如果 buy_num == 0 & click_num >= 3 则赋值 0
    # df.loc[:, 'lable_x'] = df.apply(
    #     lambda row: 1 if (row['buy_num_x'] / row['click_num_x'] >= 0.2 and row['buy_num_x'] >= 2 and row['click_num_x'] >= 3)
    #     else (0 if (row['buy_num_x'] == 0 and row['click_num_x'] >= 5) else None), axis=1
    # )
    

    df.loc[:, 'lable_x'] = df.apply(
    lambda row: 1 if (row['buy_num_x'] / row['click_num_x'] >= 0.2 and row['buy_num_x'] >= 2 and row['click_num_x'] >= 3)
    else (0 if (row['buy_num_x'] == 0 and row['click_num_x'] >= 5 and row['weekly_order_cnt'] < 7) else None), axis=1
    )
    
    # 4. 删除不满足上述逻辑的行
    df = df.dropna(subset=['lable_x'])
    
    # 6. 打印出关键信息
    print(f"Dataframe {f'data_cleaned'} shape after processing: {df.shape}")
    print(f"Number of rows removed (buy_num_x == 1 or invalid 'lable_x'): {df.shape[0]}")
    return df


add_features = pd.read_csv('tb_category_feature.csv')
# add_features = pd.read_csv('addFeature.csv')

def file_merge(df):

    merged_data_more_fuature = df.merge(add_features, on=['level_one_category_name', 'category_name'], how='left')
    return merged_data_more_fuature



def process_and_output(data):
    # 假设 data 已经定义并包含数据
    data['feature_split'] = data['feature'].str.split(';')
    
    # 计算每行拆分后的个数
    data['split_count'] = data['feature_split'].apply(lambda x: len(x) if x is not None else 0)
    
    # 统计所有特征出现的次数
    all_features = [feature for sublist in data['feature_split'].dropna() for feature in sublist]
    feature_counts = Counter(all_features)

    def process_features(features, feature_counts, max_len=6):
        if features is None:
            features = []
        if len(features) > max_len:
            features = sorted(features, key=lambda x: feature_counts[x], reverse=True)[:max_len]
        # 如果不足 max_len 个，填充 -1
        features.extend([-1] * (max_len - len(features)))
        return features

    # 拆分特征列并保留前6个
    data[['feature1', 'feature2', 'feature3', 'feature4', 'feature5', 'feature6']] = data['feature_split'].apply(lambda x: pd.Series(process_features(x, feature_counts)))
    
    # 删除不需要的列
    data = data.drop(columns=['feature_split', 'split_count', 'feature'])
    
    return data


def split_and_expand(data, max_len=2):
    # 要处理的列名列表
    columns_to_process = ['scenario', 'brand_real', 'audience']
    
    for column in columns_to_process:
        if column not in data.columns:
            print(f"Error: Column '{column}' does not exist in the DataFrame")
            continue
        
        # 拆分列
        data[f'{column}_split'] = data[column].str.split(';')
        
        # 统计每个拆分项出现的次数
        all_items = [item for sublist in data[f'{column}_split'].dropna() for item in sublist]
        item_counts = Counter(all_items)
        
        # 定义一个函数来处理每行的拆分项
        def process_items(items, item_counts, max_len):
            if items is None:
                items = []
            if len(items) > max_len:
                # 按照 item_counts 选取频数最多的前 max_len 个
                items = sorted(items, key=lambda x: item_counts[x], reverse=True)[:max_len]
            # 如果不足 max_len 个，填充 -1
            items.extend([-1] * (max_len - len(items)))
            return items
        
        # 应用函数到每行
        expanded_columns = data[f'{column}_split'].apply(lambda x: pd.Series(process_items(x, item_counts, max_len)))
        expanded_columns.columns = [f'{column}_{i+1}' for i in range(max_len)]
        
        # 将新列合并到原始数据框
        data = pd.concat([data, expanded_columns], axis=1)
        data.drop(columns=[f'{column}_split'], inplace=True)
        data.drop(columns=[column], inplace=True)
    
    return data




# # data_0705 = pd.read_pickle('data_raw_20240705.pkl')

data_path = 'z_data_new2/total_raw'
output_path = 'z_data_new2/data01_v1'
# fileName = ['data_raw_20241222_py.pkl','data_raw_20241223_py.pkl','data_raw_20241224_py.pkl','data_raw_20241225_py.pkl','data_raw_20241226_py.pkl','data_raw_20241227_py.pkl','data_raw_20241228_py.pkl','data_raw_20241229_py.pkl','data_raw_20241230_py.pkl','data_raw_20241231_py.pkl','data_raw_20250101_py.pkl','data_raw_20250102_py.pkl','data_raw_20250103_py.pkl','data_raw_20250104_py.pkl','data_raw_20250105_py.pkl','data_raw_20250106_py.pkl','data_raw_20250107_py.pkl']

fileName = ['data_raw_20250107_py.pkl','data_raw_20250108_py.pkl','data_raw_20250109_py.pkl','data_raw_20250110_py.pkl','data_raw_20250111_py.pkl','data_raw_20250112_py.pkl']
for file in fileName:
    file_pre = file.split('.')[0]
    split_and_expand(process_and_output(file_merge(labelProcess(clean_data(pd.read_pickle(os.path.join(data_path,file))))))).to_pickle(os.path.join(output_path,f'{file_pre}_merged_91.pkl'))
    print(f'{file_pre}_merged_101.pkl saved')
# data_1230_cleaned = labelProcess(clean_data(pd.read_pickle(os.path.join(data_path,'data_raw_20241230_py.pkl'))))



