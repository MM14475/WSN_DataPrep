import os
import glob
import datetime
import shutil

create_directory_folder = r"C:\Users\MM14475.CO\Desktop\A\LoggingLog"
copy_directory_folder = r"\\m5fsv01\KMM共有領域\KA5300\J_環境管理\80_施設MDASプロジェクト\KA1018_nomi\A\LoggingLog"

# nodeで始まるフォルダ名を取得
create_list = [d for d in os.listdir(copy_directory_folder) if d.startswith("node") and os.path.isdir(os.path.join(copy_directory_folder, d))]

# create_directory_folderにnodeフォルダを作成
for node in create_list:
    create_path = os.path.join(create_directory_folder, node)
    if not os.path.exists(create_path):
        os.makedirs(create_path)
        print(f"Created directory: {create_path}")
    else:
        print(f"Directory already exists: {create_path}")

# 日付範囲
threshold = 20250420
today = int(datetime.datetime.now().strftime("%Y%m%d"))

# 各nodeフォルダ内のCSVファイルをコピー
for node in create_list:
    src_node_path = os.path.join(copy_directory_folder, node)
    dst_node_path = os.path.join(create_directory_folder, node)
    csv_files = glob.glob(os.path.join(src_node_path, "*.CSV"))
    for file in csv_files:
        filename = os.path.basename(file)
        # ファイル名から日付部分を抽出（例: node1-17_20250421.CSV → 20250421）
        try:
            date_part = filename.split("_")[-1].replace(".CSV", "")
            file_date = int(date_part)
            if threshold <= file_date <= today:
                dst_file = os.path.join(dst_node_path, filename)
                shutil.copy2(file, dst_file)
                print(f"Copied: {file} -> {dst_file}")
        except Exception as e:
            print(f"Skipped {filename}: {e}")