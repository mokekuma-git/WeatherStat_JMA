#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""気象庁のindexファイルを読み込む

データ: https://www.data.jma.go.jp/obd/stats/data/mdrr/chiten/meta/smaster.index.zip
仕様: https://www.data.jma.go.jp/obd/stats/data/mdrr/man/smasterindex_format.pdf
"""

import pandas as pd
import codecs

# 各フィールドの名前を定義
SMASTER_COLNAME = [
    "地点番号", "空白", "管区コード", "観測回数", "全天日射蒸発量降水強風の有無", "天気概況大気現象",
    "現地・海面気圧の観測回数", "観測測器", "日照計測器", "特殊日勤官署", "アメダス府県コード", "観測所区分", "空白",
    "カナ地点名", "ローマ字地点名", "観測所緯度", "観測所経度", "風向風速計の高さｍ", "気圧計の高さｍ", "雨量計の地上からの高さｍ",
    "報告種別", "未使用", "観測開始日時", "観測終了年月日", "漢字地点名", "漢字官署名", "都道府県振興局名（左詰めで全角4文字まで）",
    "標高(官署の高さ)", "空白", "特別地域気象観測所", "視程計", "積雪計", "95 型・10 型", "山岳官署", "大気現象の観測状況", "空白"
]

# 各フィールドのバイト幅を定義します。
SMASTER_COLLEN = [3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 8, 12, 6, 7, 5, 5, 3, 1, 1, 8, 8, 12, 18, 8, 5, 12, 1, 1, 1, 1, 1, 1, 5]

def read_smaster(filename: str) -> pd.DataFrame:
    """SMASTER.INDEXファイルを読み込む

    Args:
        filename (str): SMASTER.INDEXファイルのパス
    
    Returns:
        pd.DataFrame: SMASTER.INDEXファイルの内容
    """
    df = pd.DataFrame(columns=SMASTER_COLNAME)
    row_list = []
    # バイナリモードでファイルを開きます。
    with open(filename, "rb") as file:
        for line in file:
            start = 0
            row = []
            for width in SMASTER_COLLEN:
                # バイト列をShift_JIS文字列に変換します。
                field = codecs.decode(line[start:start+width], "Shift_JIS").strip()
                row.append(field)
                start += width
            row_list.append(row)

    return pd.DataFrame(row_list, columns=SMASTER_COLNAME)
