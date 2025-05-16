import json
import re

# 変換先ネットワークパスのプレフィックス
NETWORK_PREFIX = r"\\m5fsv01\KMM共有領域\KA5300\J_環境管理\80_施設MDASプロジェクト\KA1018_nomi\A\LoggingLog"
# 変換対象ファイル
INPUT_PATH = r"test output/preprocessed_file_history.json"
OUTPUT_PATH = r"test output/preprocessed_file_history_converted.json"

# P:ドライブの正規表現パターン
PDRIVE_PATTERN = re.compile(r"^P:(.*?\\LoggingLog\\)(.+)$", re.UNICODE)

def convert_path(p):
    match = PDRIVE_PATTERN.match(p)
    if match:
        # サブディレクトリとファイル名を抽出
        subdir_and_file = match.group(2)
        # サブディレクトリ部分を取得
        subdir = match.group(1).replace('\\', '/')
        # サブディレクトリ名（nodeXX-YYなど）を抽出
        node_dir = subdir.split('/')[-2] if len(subdir.split('/')) > 1 else ''
        # 変換後パスを生成
        return f"{NETWORK_PREFIX}/{node_dir}/{subdir_and_file}".replace('/', '\\')
    return p

def main():
    with open(INPUT_PATH, encoding='utf-8') as f:
        data = json.load(f)

    for entry in data.get('preprocessed_file_path', []):
        if 'file_path' in entry:
            entry['file_path'] = convert_path(entry['file_path'])

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"変換完了: {OUTPUT_PATH}")

if __name__ == '__main__':
    main()
