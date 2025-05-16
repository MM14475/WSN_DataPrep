import pandas as pd
import os
import json
import time
import re

from datetime import datetime, date
from typing import List, Dict, Any


script_path = os.path.abspath(__file__)
script_dir = os.path.dirname(script_path)
with open(f'setting\\config.json', encoding="utf-8-sig") as config_file:
    config = json.load(config_file)
    
LOGGING_DATA_PATH = config["LOGGING_DATA_PATH"]
OUTPUT_FOLDER_PATH = config["OUTPUT_FOLDER_PATH"]
PREPROCESSED_FILE_PATH = os.path.join(OUTPUT_FOLDER_PATH, 'preprocessed_file_history.json')

# スケール情報のJSONファイル
SCALE_JSON_PATH = config["SCALE_JSON_PATH"]
SENS_TYPE_JSON_PATH = config["SENS_TYPE_JSON_PATH"]


CURRENT_DATA_EXCEL_FILE_PATH = 'sensor_data.xlsx'
CURRENT_DATA_EXCEL_FILE_PATH = config["CURRENT_DATA_EXCEL_FILE_PATH"]
CURRENT_SENSOR_READINGS_JSON = r'C:\Box\030_Int_FacilityJob_MMC_MMA1P0\300_技術_A1P21\309_先進的施設管理\MDAS\可視化アプリ用\MDAS_WSN_DataPrep\setting\current_sensor_readings.json'
CURRENT_SENSOR_READINGS_JSON = config["CURRENT_SENSOR_READINGS_JSON"]
MANAGEMENT_LEDGER_PATH = r'C:\Box\030_Int_FacilityJob_MMC_MMA1P0\300_技術_A1P21\309_先進的施設管理\MDAS\可視化アプリ用\MDAS_WSN_DataPrep\無線センサ管理台帳.xlsx'
MANAGEMENT_LEDGER_PATH = config["MANAGEMENT_LEDGER_PATH"]
MANAGEMENT_LEDGER_SHEET_NAME = 'Sheet1'
MANAGEMENT_LEDGER_SHEET_NAME = config["MANAGEMENT_LEDGER_SHEET_NAME"]



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




def load_sensor_ledger(path: str, sheet_name: str) -> pd.DataFrame:
    """
    センサ管理台帳を読み込みます。

    Args:
        path (str): センサ管理台帳のExcelファイルのパス。
        sheet_name (str): 読み込むシート名。

    Returns:
        pd.DataFrame: センサ管理台帳のデータフレーム。
    """
    try:
        sensor_ledger = pd.read_excel(path, sheet_name=sheet_name, engine='openpyxl')
        # print("センサ管理台帳を正常に読み込みました。")
        return sensor_ledger
    except Exception as e:
        # print(f"センサ管理台帳の読み込み中にエラーが発生しました: {e}")
        raise

def load_sensor_sheets(json_path: str) -> List[Dict[str, Any]]:
    """
    JSONファイルからセンサーシート情報を読み込みます。

    Args:
        json_path (str): current_sensor_readings.jsonのパス。

    Returns:
        List[Dict[str, Any]]: センサーシート情報のリスト。
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            sensor_sheets = json.load(f)
        # print("sensor_sheets JSONを正常に読み込みました。")
        return sensor_sheets
    except Exception as e:
        # print(f"sensor_sheets JSONの読み込み中にエラーが発生しました: {e}")
        raise

def clean_sheet_names(sensor_sheets: List[Dict[str, Any]]) -> None:
    """
    シート名に含まれる"/"を削除します。

    Args:
        sensor_sheets (List[Dict[str, Any]]): センサーシート情報のリスト。
    """
    for sheet in sensor_sheets:
        original_sheet_name = sheet['sheet_name']
        cleaned_sheet_name = original_sheet_name.replace('/', '')
        sheet['sheet_name'] = cleaned_sheet_name
    # print("シート名のクリーンアップを完了しました。")

def initialize_sheet_dataframes(sensor_sheets: List[Dict[str, Any]], columns: List[str]) -> None:
    """
    センサーシートごとに空のデータフレームを初期化します。

    Args:
        sensor_sheets (List[Dict[str, Any]]): センサーシート情報のリスト。
        columns (List[str]): データフレームのカラム名。
    """
    for sheet in sensor_sheets:
        if sheet.get('dataframe') is None:
            sheet['dataframe'] = pd.DataFrame(columns=columns)
    # print("シートごとのデータフレームを初期化しました。")

def add_sensor_data(sensor_sheets: List[Dict[str, Any]], 
                   df_result: pd.DataFrame, 
                   sensor_ledger: pd.DataFrame) -> None:
    """
    df_resultの最新データを対応するシートに追加します。

    Args:
        sensor_sheets (List[Dict[str, Any]]): センサーシート情報のリスト。
        df_result (pd.DataFrame): 最新のセンサーデータを含むデータフレーム。
        sensor_ledger (pd.DataFrame): センサ管理台帳のデータフレーム。
    """
    
    # print('add_sensor_data run')
    
    if df_result.empty:
        print("df_resultが空です。処理を中断します。")
        return
    
    last_row = df_result.tail(1).copy()
    current_id = last_row['ノードID'].iloc[0]
    
    sens_type_array = sensor_ledger[sensor_ledger['ID'] == current_id]['センサ種別'].values
    

    if str(sens_type_array) =="":
        return

    elif str(sens_type_array[0]) == 'nan':
        return
    
    if len(sens_type_array) == 0:
        print(f"センサーID {current_id} がセンサ管理台帳に存在しません。")
        return
    
    
    sens_type = sens_type_array[0]
    
    # sensor_sheets内で対応するセンサ種別のシートを検索
    target_sheet = next(
        (sheet for sheet in sensor_sheets if sheet['sheet_name'] == sens_type.replace("/","")),
        None
    )
    
    if target_sheet is None:
        print(f"センサ種別 '{sens_type}' に対応するシートが見つかりません。")
        return
    
    if target_sheet['dataframe'] is None:
        target_sheet['dataframe'] = last_row.reset_index(drop=True)
        # print(f"センサーID {current_id} のデータをシート '{sens_type}' にセットしました。")
    else:
        target_sheet['dataframe'] = pd.concat(
            [target_sheet['dataframe'], last_row],
            ignore_index=True
        )


def write_to_excel(sensor_sheets: List[Dict[str, Any]], output_path: str) -> None:
    """
    すべてのシートのデータフレームをエクセルファイルに書き出します。

    Args:
        sensor_sheets (List[Dict[str, Any]]): センサーシート情報のリスト。
        output_path (str): 出力するエクセルファイルのパス。
    """
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for sheet in sensor_sheets:
                sheet_name = sheet.get('sheet_name', 'Unnamed_Sheet')
                df = sheet.get('dataframe')
                if df is None:
                    print(f"警告: シート '{sheet_name}' のデータフレームが None です。空のシートを作成します。")
                    # 空のデータフレームを作成
                    empty_df = pd.DataFrame(columns=[])
                    empty_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    continue
                
                if not isinstance(df, pd.DataFrame):
                    print(f"警告: シート '{sheet_name}' のデータは DataFrame ではありません。スキップします。")
                    continue  # シートの書き出しをスキップ
                
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"データをエクセルファイル '{output_path}' に出力しました。")
    except Exception as e:
        print(f"エクセルファイルへの書き出し中にエラーが発生しました: {e}")
        raise
    
# 今日の日付を取得
today = datetime.today()
# yyyymmddの形式で日付をフォーマット
today = today.strftime('%Y%m%d')

# JSONファイルの読み込み
with open(SCALE_JSON_PATH, 'r') as file:
    df_scale = pd.DataFrame(json.load(file))
    
# センサ管理台帳の読み込み
sensor_ledger = load_sensor_ledger(MANAGEMENT_LEDGER_PATH, MANAGEMENT_LEDGER_SHEET_NAME)

# 最新情報を吐き出すエクセルファイルのシート名情報をJSONファイルから読み出し
sensor_sheets = load_sensor_sheets(CURRENT_SENSOR_READINGS_JSON)

# Sheet名に"/"が含まれるとエラーが出るため名称を一部置換と、初期化する
clean_sheet_names(sensor_sheets=sensor_sheets)

# センサー種別ごとのカラム情報のJSONファイルを読み込み
df_sens_type = pd.read_json(SENS_TYPE_JSON_PATH, encoding="utf-8")

node_folders = get_node_folders(LOGGING_DATA_PATH)

# 今日の日付をyyyymmdd形式で取得

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
        
        read_time = time.time() - s_time
        
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
            
            
            # 最新値だけを取得する。
            if today == yyyymmdd:
                # **************************************************************************
                add_sensor_data(sensor_sheets=sensor_sheets,
                                df_result=df_result,
                                sensor_ledger=sensor_ledger)
                # for senser_data in sensor_sheets:
                #     print(senser_data['sheet_name'])
                #     print(senser_data['dataframe'])
                # **************************************************************************

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
        s_write_time = time.time()
        output_csvfile_path =f'{OUTPUT_FOLDER_PATH}\\node{start_node}-{end_node}\\node{start_node}-{end_node}_{yyyymmdd}'
        df_scaled.to_csv(f'{output_csvfile_path}.csv' ,index=False, encoding='shift-jis')
        df_scaled.to_parquet(f'{output_csvfile_path}.parquet',index=False)
        write_time = time.time() - s_write_time
                
        # print(f"処理完了  ID{start_node} to {end_node}  --->  実行時間：{time.time()-s_time}")
        # print(f"処理完了  ID{start_node} to {end_node}  --->  実行時間：{time.time()-s_time:.3f} (読み時間{read_time:.3f}  書き出し時間{write_time:.3f})")
        
        # 処理済みのファイルとしてJSONファイルに保存
        if yyyymmdd == today:
            continue
        else:
            save_file_history(f'{LOGGING_DATA_PATH}\\node{start_node}-{end_node}\\{preprocessing_file}')

    # データをエクセルに書き出し
write_to_excel(sensor_sheets, CURRENT_DATA_EXCEL_FILE_PATH)
    
    