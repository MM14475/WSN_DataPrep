import os
import re
import json
import time
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd

# 設定ファイルの読み込み
script_path = os.path.abspath(__file__)
script_dir = os.path.dirname(script_path)
with open('setting/config.json', encoding="utf-8-sig") as config_file:
    config = json.load(config_file)

LOGGING_DATA_PATH = config["LOGGING_DATA_PATH"]
OUTPUT_FOLDER_PATH = config["OUTPUT_FOLDER_PATH"]
PREPROCESSED_FILE_PATH = os.path.join(OUTPUT_FOLDER_PATH, 'preprocessed_file_history.json')
SCALE_JSON_PATH = config["SCALE_JSON_PATH"]
SENS_TYPE_JSON_PATH = config["SENS_TYPE_JSON_PATH"]
CURRENT_DATA_EXCEL_FILE_PATH = config["CURRENT_DATA_EXCEL_FILE_PATH"]
CURRENT_SENSOR_READINGS_JSON = config["CURRENT_SENSOR_READINGS_JSON"]
MANAGEMENT_LEDGER_PATH = config["MANAGEMENT_LEDGER_PATH"]
MANAGEMENT_LEDGER_SHEET_NAME = config["MANAGEMENT_LEDGER_SHEET_NAME"]

def generate_node_list(start_node_id: int, end_node_id: int) -> List[str]:
    """
    指定したノードID範囲のカラム名リストを生成する。
    Args:
        start_node_id (int): 開始ノードID
        end_node_id (int): 終了ノードID
    Returns:
        List[str]: カラム名リスト
    """
    if not (1 <= start_node_id <= 9999 and 1 <= end_node_id <= 9999):
        raise ValueError("Node IDs must be between 1 and 9999")
    if start_node_id > end_node_id:
        raise ValueError("Start node ID must be less than or equal to end node ID")
    result = ["TIME"]
    for node_id in range(start_node_id, end_node_id + 1):
        node_prefix = f"ノード{node_id:04d}"
        result.extend([
            f"{node_prefix}:ノードID",
            f"{node_prefix}:電波強度",
            f"{node_prefix}:センサ種別"
        ])
        for i in range(1, 20):
            result.extend([
                f"{node_prefix}:値{i}",
                f"{node_prefix}:スケール{i}",
                f"{node_prefix}:単位{i}"
            ])
    return result

def get_node_folders(logging_folder_path: str) -> List[str]:
    """
    指定フォルダ内の"node"で始まるサブフォルダのパス一覧を返す。
    """
    all_entries = os.listdir(logging_folder_path)
    node_folders = [
        os.path.join(logging_folder_path, entry) for entry in all_entries
        if os.path.isdir(os.path.join(logging_folder_path, entry)) and entry.startswith("node")
    ]
    return node_folders

def extract_node_ids(node_path: str) -> tuple[int, int]:
    """
    フォルダ名からノードID範囲を抽出する。
    """
    match = re.search(r'node(\d+)-(\d+)', node_path)
    if match:
        return int(match.group(1)), int(match.group(2))
    raise ValueError("The path does not contain a valid 'node' range.")

def save_file_history(file_path: str, json_file_path: str = PREPROCESSED_FILE_PATH) -> None:
    """
    処理済みファイルの履歴をJSONに保存する。
    """
    data = {"preprocessed_file_path": []}
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r') as json_file:
            try:
                data = json.load(json_file)
            except json.JSONDecodeError:
                pass
    new_entry = {"file_name": os.path.basename(file_path), "file_path": file_path}
    data["preprocessed_file_path"].append(new_entry)
    with open(json_file_path, 'w+', encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=4, ensure_ascii=False)

def is_file_processed(file_path: str, json_file_path: str = PREPROCESSED_FILE_PATH) -> bool:
    """
    ファイルが既に処理済みかどうかを判定する。
    """
    if not os.path.exists(json_file_path):
        return False
    try:
        with open(json_file_path, 'r') as json_file:
            data = json.load(json_file)
            if "preprocessed_file_path" not in data:
                return False
            for entry in data["preprocessed_file_path"]:
                if entry.get("file_path") == file_path:
                    return True
    except (json.JSONDecodeError, IOError) as e:
        print(f"Error reading JSON file: {e}")
    return False

def load_sensor_ledger(path: str, sheet_name: str) -> pd.DataFrame:
    """
    センサ管理台帳を読み込む。
    """
    try:
        return pd.read_excel(path, sheet_name=sheet_name, engine='openpyxl')
    except Exception as e:
        raise

def load_sensor_sheets(json_path: str) -> List[Dict[str, Any]]:
    """
    センサーシート情報をJSONから読み込む。
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        raise

def clean_sheet_names(sensor_sheets: List[Dict[str, Any]]) -> None:
    """
    シート名に含まれる"/"を削除する。
    """
    for sheet in sensor_sheets:
        sheet['sheet_name'] = sheet['sheet_name'].replace('/', '')

def initialize_sheet_dataframes(sensor_sheets: List[Dict[str, Any]], columns: List[str]) -> None:
    """
    センサーシートごとに空のデータフレームを初期化する。
    """
    for sheet in sensor_sheets:
        if sheet.get('dataframe') is None:
            sheet['dataframe'] = pd.DataFrame(columns=columns)

def add_sensor_data(sensor_sheets: List[Dict[str, Any]], df_result: pd.DataFrame, sensor_ledger: pd.DataFrame) -> None:
    """
    最新データを対応するシートに追加する。
    """
    if df_result.empty:
        print("df_resultが空です。処理を中断します。")
        return
    last_row = df_result.tail(1).copy()
    current_id = last_row['ノードID'].iloc[0]
    sensor_info = sensor_ledger[sensor_ledger['ID'] == current_id]
    if sensor_info.empty:
        print(f"センサーID {current_id} がセンサ管理台帳に存在しません。")
        return
    sens_type = sensor_info['センサ種別'].iat[0]
    measurement_target = sensor_info['測定対象'].iat[0]
    if pd.isnull(sens_type) or str(sens_type).strip() == "":
        return
    last_row.insert(loc=2, column='測定対象', value=measurement_target)
    cleaned_sensor_type = str(sens_type).replace("/", "")
    target_sheet = next((sheet for sheet in sensor_sheets if sheet.get('sheet_name') == cleaned_sensor_type), None)
    if target_sheet is None:
        print(f"センサ種別 '{sens_type}' に対応するシートが見つかりません。")
        return
    if target_sheet.get('dataframe') is None:
        target_sheet['dataframe'] = last_row.reset_index(drop=True)
    else:
        target_sheet['dataframe'] = pd.concat([
            target_sheet['dataframe'], last_row
        ], ignore_index=True)

def write_to_excel(sensor_sheets: List[Dict[str, Any]], output_path: str) -> None:
    """
    すべてのシートのデータフレームをエクセルファイルに書き出す。
    """
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            for sheet in sensor_sheets:
                sheet_name = sheet.get('sheet_name', 'Unnamed_Sheet')
                df = sheet.get('dataframe')
                if df is None:
                    print(f"警告: シート '{sheet_name}' のデータフレームが None です。空のシートを作成します。")
                    pd.DataFrame(columns=[]).to_excel(writer, sheet_name=sheet_name, index=False)
                    continue
                if not isinstance(df, pd.DataFrame):
                    print(f"警告: シート '{sheet_name}' のデータは DataFrame ではありません。スキップします。")
                    continue
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"データをエクセルファイル '{output_path}' に出力しました。")
    except Exception as e:
        print(f"エクセルファイルへの書き出し中にエラーが発生しました: {e}")
        raise

today = datetime.today().strftime('%Y%m%d')

with open(SCALE_JSON_PATH, 'r', encoding='utf-8') as file:
    df_scale = pd.DataFrame(json.load(file))

sensor_ledger = load_sensor_ledger(MANAGEMENT_LEDGER_PATH, MANAGEMENT_LEDGER_SHEET_NAME)
sensor_sheets = load_sensor_sheets(CURRENT_SENSOR_READINGS_JSON)
clean_sheet_names(sensor_sheets)
df_sens_type = pd.read_json(SENS_TYPE_JSON_PATH, encoding="utf-8")
node_folders = get_node_folders(LOGGING_DATA_PATH)

for node_folder in node_folders:
    start_node, end_node = extract_node_ids(node_folder)
    file_list = os.listdir(node_folder)
    for preprocessing_file in file_list:
        file_path = os.path.join(node_folder, preprocessing_file)
        if is_file_processed(file_path):
            continue
        s_time = time.time()
        print(f"処理開始: {os.path.basename(preprocessing_file)}")
        data = pd.read_csv(file_path, encoding='cp932', skiprows=2)
        df = data.copy()
        read_time = time.time() - s_time
        yyyymmdd = preprocessing_file.split('_')[-1].split('.')[0]
        new_columns = generate_node_list(start_node, end_node)
        if len(new_columns) < df.shape[1]:
            new_columns.extend(df.columns[len(new_columns):])
        df.columns = new_columns
        df_tmp_time = df.TIME.copy()
        df_scaled = pd.DataFrame()
        for node_id in range(start_node, end_node + 1):
            df_tmp = df.loc[:, df.columns.str.contains(f'{node_id:04d}')].copy()
            if df_tmp.iloc[:, 0].isnull().all():
                continue
            df_filtered_sens_type = df_sens_type[df_sens_type['sens_code_dec'] == df_tmp.iloc[-1, 2]]
            df_filtered_sens_columns = list(filter(lambda x: not pd.isna(x), df_filtered_sens_type.values.flatten().tolist()))[3:]
            value_columns = [col for col in df_tmp.columns if "値" in col][:len(df_filtered_sens_columns)]
            scale_columns = [col for col in df_tmp.columns if "スケール" in col][:len(df_filtered_sens_columns)]
            for v_col, s_col in zip(value_columns, scale_columns):
                df_tmp[s_col] = df_tmp[s_col].astype(float)
                df_tmp.loc[:, s_col] = df_tmp.loc[:, s_col].map(df_scale['scale'])
            result = df_tmp.loc[:, scale_columns].values * df_tmp.loc[:, value_columns].values
            df_tmp_scaled = pd.DataFrame(result, columns=df_tmp.loc[:, value_columns].columns, index=df_tmp.index)
            df_tmp_scaled.columns = df_filtered_sens_columns
            df_result = pd.concat([df_tmp_time, df_tmp.iloc[:, 0:2], df_tmp_scaled], axis=1)
            df_result.rename(columns={f"ノード{node_id:04d}:ノードID": "ノードID"}, inplace=True)
            df_result.rename(columns={f"ノード{node_id:04d}:電波強度": "電波強度[dB]"}, inplace=True)
            if today == yyyymmdd:
                add_sensor_data(sensor_sheets, df_result, sensor_ledger)
            df_result_melt = df_result.melt(id_vars=["TIME", "ノードID"], var_name="測定種別", value_name="測定値")
            df_scaled = pd.concat([df_scaled, df_result_melt], axis=0)
        if df_scaled.shape == (0, 0):
            if yyyymmdd == today:
                continue
            else:
                save_file_history(file_path)
                continue
        else:
            df_scaled = df_scaled.dropna()
            df_scaled["ノードID"] = df_scaled["ノードID"].astype(int)
        df_scaled.sort_values(by="TIME", inplace=True)
        output_dir = os.path.join(OUTPUT_FOLDER_PATH, f'node{start_node}-{end_node}')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        s_write_time = time.time()
        output_csvfile_path = os.path.join(output_dir, f'node{start_node}-{end_node}_{yyyymmdd}')
        df_scaled.to_csv(f'{output_csvfile_path}.csv', index=False, encoding='shift-jis')
        df_scaled.to_parquet(f'{output_csvfile_path}.parquet', index=False)
        write_time = time.time() - s_write_time
        if yyyymmdd == today:
            continue
        else:
            save_file_history(file_path)
write_to_excel(sensor_sheets, CURRENT_DATA_EXCEL_FILE_PATH)

