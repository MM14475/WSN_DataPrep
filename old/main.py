import pandas as pd
import os
import json
import time
import re
from datetime import datetime

# with open(r'setting\config.json', encoding="utf-8") as config_file:
#     config = json.load(config_file)

script_path = os.path.abspath(__file__)
script_dir = os.path.dirname(script_path)
with open(f'{script_dir}\\setting\\config.json', encoding="utf-8-sig") as config_file:
    config = json.load(config_file)
    
LOGGING_DATA_PATH = config["LOGGING_DATA_PATH"]
OUTPUT_FOLDER_PATH = config["OUTPUT_FOLDER_PATH"]
PREPROCESSED_FILE_PATH = os.path.join(OUTPUT_FOLDER_PATH, 'preprocessed_file_history.json')

# スケール情報のJSONファイル
SCALE_JSON_PATH = config["SCALE_JSON_PATH"]
SENS_TYPE_JSON_PATH = config["SENS_TYPE_JSON_PATH"]

# カラム名変更の関数


def generate_node_list(start_node_id, end_node_id):
    """
    カラム名の生成
    """
    if not (1 <= start_node_id <= 9999 and 1 <= end_node_id <= 9999):
        raise ValueError("Node IDs must be between 1 and 9999")
    if start_node_id > end_node_id:
        raise ValueError(
            "Start node ID must be less than or equal to end node ID")

    result = ["TIME"]

    for node_id in range(start_node_id, end_node_id + 1):
        node_prefix = f"ノード{node_id:04d}"

        # Add common fields
        result.extend([
            f"{node_prefix}:ノードID",
            f"{node_prefix}:電波強度",
            f"{node_prefix}:センサ種別"
        ])

        # Add sensor value fields
        for i in range(1, 20):
            result.extend([
                f"{node_prefix}:値{i}",
                f"{node_prefix}:スケール{i}",
                f"{node_prefix}:単位{i}"
            ])

    return result

def get_node_folders(logging_folder_path):
    # フォルダ内の全てのファイルとフォルダを取得
    all_entries = os.listdir(logging_folder_path)
    
    # "node"で始まるフォルダのみをフィルタリング
    node_folders = [
        os.path.join(logging_folder_path, entry) for entry in all_entries 
        if os.path.isdir(os.path.join(logging_folder_path, entry)) and entry.startswith("node")
    ]
    
    return node_folders

def extract_node_ids(node_path):
    # 正規表現で "node" に続く数字の範囲を抽出
    match = re.search(r'node(\d+)-(\d+)', node_path)
    
    if match:
        start_id = int(match.group(1))
        end_id = int(match.group(2))
        return start_id, end_id
    else:
        raise ValueError("The path does not contain a valid 'node' range.")

def save_file_history(file_path,json_file_path=PREPROCESSED_FILE_PATH):
    # 初期化されたデータ構造
    data = {
        "preprocessed_file_path": []
    }

    # 既存のJSONファイルが存在する場合、その内容を読み込む
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
            try:
                data = json.load(json_file)
            except json.JSONDecodeError:
                pass  # ファイルが壊れている場合は初期化されたデータ構造を使用
    
    # 新しいエントリを作成
    new_entry = {
        "file_name": os.path.basename(file_path),
        "file_path": file_path,
    }
    
    # データに新しいエントリを追加
    data["preprocessed_file_path"].append(new_entry)

    # 更新されたデータを書き戻す
    with open(json_file_path, 'w+') as json_file:
        json.dump(data, json_file, indent=4)

def is_file_processed(file_path, json_file_path=PREPROCESSED_FILE_PATH):
    # JSONファイルが存在しない場合はFalseを返す
    if not os.path.exists(json_file_path):
        return False

    try:
        with open(json_file_path, 'r') as json_file:
            data = json.load(json_file)
            
            # JSONデータの構造が期待通りでない場合もFalseを返す
            if "preprocessed_file_path" not in data:
                return False
            
            # 処理済みファイルパスのリストから該当するものを検索
            for entry in data["preprocessed_file_path"]:
                if entry.get("file_path") == file_path:
                    return True

    except (json.JSONDecodeError, IOError) as e:
        print(f"Error reading JSON file: {e}")
    
    return False
        
def main():

    # 今日の日付を取得
    today = datetime.today()
    # yyyymmddの形式で日付をフォーマット
    today = today.strftime('%Y%m%d')
    
    # JSONファイルの読み込み
    with open(SCALE_JSON_PATH, 'r') as file:
        df_scale = pd.DataFrame(json.load(file))

    # センサー種別ごとのカラム情報のJSONファイルを読み込み
    df_sens_type = pd.read_json(SENS_TYPE_JSON_PATH, encoding="utf-8")

    node_folders = get_node_folders(LOGGING_DATA_PATH)
    
    for node_folder in node_folders:
        
        start_node, end_node = extract_node_ids(node_folder)
        
        file_list = os.listdir(node_folder)
        
        for preprocessing_file in file_list:
            
            # 処理済みのファイルの場合はスキップ
            if is_file_processed(f'{node_folder}\\{preprocessing_file}'):
                continue
                
            # 処理時間の測定
            s_time = time.time()
            
            print(f"処理開始:{preprocessing_file.split("\\")[-1]}")
            data = pd.read_csv(f'{node_folder}\\{preprocessing_file}', encoding='cp932', skiprows=2)
            df = data.copy()
            
            # ファイル名から日付を取得
            yyyymmdd = preprocessing_file.split('_')[-1]
            yyyymmdd = yyyymmdd.split('.')[0]
            
            # カラム名変更
            df.columns = generate_node_list(start_node, end_node)

            # TIME列だけ取り出しておく
            df_tmp_time = df.TIME.copy()
            df_scaled = pd.DataFrame()

            for node_id in range(start_node, end_node+1):

                # データ加工用に元データのコピーを作成
                df_tmp = df.loc[:, df.columns.str.contains(f'{node_id:04d}')].copy()

                if (df_tmp.iloc[:, 0].isnull().all()):
                    continue

                # センサー種別ごと測定値のカラム名称を取得する。  例：値1 = 電源電圧[V]、値2 = パケット番号[times],,,,,
                df_filtered_sens_type = df_sens_type[df_sens_type['sens_code_dec']
                                                    == df_tmp.iloc[-1, 2]]

                # 不要なカラム名と、NaN値を含む列名を削除した新しいリスト
                df_filtered_sens_columns = list(filter(lambda x: not pd.isna(
                    x), df_filtered_sens_type.values.flatten().tolist()))
                df_filtered_sens_columns = df_filtered_sens_columns[3:]

                # センサー種別の情報から、処理が必要なカラム名だけを取得する。
                value_columns = [col for col in df_tmp.columns if "値" in col]
                value_columns = value_columns[:len(df_filtered_sens_columns)]
                scale_columns = [col for col in df_tmp.columns if "スケール" in col]
                scale_columns = scale_columns[:len(df_filtered_sens_columns)]

                for v_col, s_col in zip(value_columns, scale_columns):
                    # スケール列のデータ型をfloatに変換
                    # .loc を使用して明示的に操作し、警告回避
                    df_tmp[s_col] = df_tmp[s_col].astype(float)

                    # スケール列の値を、10のべき乗に変換する。
                    df_tmp.loc[:, s_col] = df_tmp.loc[:, s_col].map(df_scale['scale'])

                # 値列とスケール倍率を掛け算して実際の測定値を算出する。
                result = df_tmp.loc[:, scale_columns].values * \
                    df_tmp.loc[:, value_columns].values
                df_tmp_scaled = pd.DataFrame(
                    result, columns=df_tmp.loc[:, value_columns].columns, index=df_tmp.index)

                # カラム名を、測定種別_[単位]の形に書き換える。
                df_tmp_scaled.columns = df_filtered_sens_columns

                # TIME列、ノードID、電波強度、[スケール調整後の値]を行方向に結合
                df_result = pd.concat(
                    [df_tmp_time, df_tmp.iloc[:, 0:2], df_tmp_scaled], axis=1)

                # カラム名の調整
                df_result.rename(
                    columns={f"ノード{node_id:04d}:ノードID": "ノードID"}, inplace=True)
                df_result.rename(
                    columns={f"ノード{node_id:04d}:電波強度": "電波強度_[dB]"}, inplace=True)

                # ピボットの解除
                df_result_melt = df_result.melt(
                    id_vars=["TIME", "ノードID"], var_name="測定種別", value_name="測定値")

                # 処理結果を列方向に結合
                df_scaled = pd.concat([df_scaled, df_result_melt], axis=0)


            # 空データの場合は処理をスキップする。
            if df_scaled.shape == (0,0):
                # 処理実行日＝ファイル名の場合は保存しない
                if yyyymmdd == today:
                    continue
                
                else:
                    save_file_history(f'{LOGGING_DATA_PATH}\\node{start_node}-{end_node}\\{preprocessing_file}')
                    continue
            
            else:
                df_scaled = df_scaled.dropna()
                df_scaled["ノードID"] = df_scaled["ノードID"].astype(int)
            
            # TIMEでソート
            df_scaled.sort_values(by="TIME",inplace=True)

            # ディレクトリが存在するか確認し、存在しない場合は作成
            if not os.path.exists(f'{OUTPUT_FOLDER_PATH}\\node{start_node}-{end_node}'):
                os.makedirs(f'{OUTPUT_FOLDER_PATH}\\node{start_node}-{end_node}')
            
            # 処理後のファイルを保存する。    
            output_csvfile_path =f'{OUTPUT_FOLDER_PATH}\\node{start_node}-{end_node}\\node{start_node}-{end_node}_{yyyymmdd}'
            df_scaled.to_csv(f'{output_csvfile_path}.csv' ,index=False, encoding='shift-jis')
            df_scaled.to_parquet(f'{output_csvfile_path}.parquet',index=False)

            print(f"処理完了  ID{start_node} to {end_node}  --->  実行時間：{time.time()-s_time}")
            
            # 処理済みのファイルとしてJSONファイルに保存
            if yyyymmdd == today:
                continue
            else:
                save_file_history(f'{LOGGING_DATA_PATH}\\node{start_node}-{end_node}\\{preprocessing_file}')

if __name__ == "__main__":
    main()
