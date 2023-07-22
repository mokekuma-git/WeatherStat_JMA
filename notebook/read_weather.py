#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""気象庁のサイトから気象データを取得する"""

from datetime import date
from io import StringIO
from typing import Dict, List, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

WEATHER_COLNAME = ["日", "現地気圧", "海面気圧", "降水量合計", "降水量1時間最大", "降水量10分最大", "平均気温",
                   "最高気温", "最低気温", "平均湿度", "最小湿度", "平均風速", "最大風速", "最大風向き",
                   "最大瞬間風速", "最大瞬間風向き", "日照時間", "降雪合計", "最深積雪", "昼天気概況", "夜天気概況"]

# ブラウザでの表表示URL
TABLEVIEWER_URL = "https://www.data.jma.go.jp/obd/stats/etrn/view/daily_s1.php?" \
                  "prec_no={}&block_no={}&year={}&month={}&day={}&view=p1"

# 観測項目選択HTMLのURL
ELEMENTS_URL = "https://www.data.jma.go.jp/gmd/risk/obsdl/top/element"

# 観測地点取得URL
STATION_URL = "https://www.data.jma.go.jp/gmd/risk/obsdl/top/station"

# 観測データダウンロードURL
CSVDL_URL = "https://www.data.jma.go.jp/gmd/risk/obsdl/show/table"

# 観測データダウンロードページのURL
DL_INDEX_URL = "https://www.data.jma.go.jp/gmd/risk/obsdl/index.php"


def get_weather_from_html(prec_no: int, block_no: int, year: int, month: int, day: int) -> pd.DataFrame:
    """気象データをブラウザ表表表示画面から取得する

    Args:
        prec_no (int): 地域コード
        block_no (int): 地点番号
        year (int): 年
        month (int): 月
        day (int): 日

    Returns:
        pd.DataFrame: 気象データ
    """
    # データを取得
    response = requests.get(TABLEVIEWER_URL.format(prec_no, block_no, year, month, day))

    # エンコーディングを明示的に指定
    response.encoding = "utf-8"

    # BeautifulSoupオブジェクトを作成
    soup = BeautifulSoup(response.text, "lxml")

    # 指定されたIDとクラスを持つ表を見つける
    table = soup.find("table", {"id": "tablefix1", "class": "data2_s"})

    # 表の指定されたクラスを持つすべての行を見つける
    rows = table.find_all("tr", {"class": "mtx", "style": "text-align:right;"})

    # 行からデータを抽出
    data = []
    for row in rows:
        cols = row.find_all("td")
        cols = [ele.text.strip() for ele in cols]
        data.append([ele for ele in cols if ele])

    # データフレームを作成
    return pd.DataFrame(data, columns=WEATHER_COLNAME)


def get_prefectures() -> Dict[int, str]:
    """気象庁観測地点情報から観測地域コードのリストを取得

    Returns:
        Dict[int, str]: 観測地域コードと都道府県名の辞書
    """
    # 気象庁の観測地点リストのURLからデータを取得
    response = requests.get(STATION_URL)
    response.encoding = "utf-8"
    soup = BeautifulSoup(response.text, "lxml")

    # 各都道府県の名前と観測地域コードを抽出し、辞書に格納
    prefectures = {
        int(div.find("input", {"name": "prid"})["value"]): div.text
        for div in soup.find_all("div", class_="prefecture")
    }

    return prefectures


def get_stations(pd_: int) -> Dict[int, str]:
    """気象庁の観測地点情報から観測地点コードのリストを取得

    Args:
        pd_ (int): 観測地域コード

    Returns:
        Dict[int, str]: 観測地点コードと観測地点情報
    """
    # POSTリクエストを送信
    response = requests.post(STATION_URL, data={"pd": pd_})
    response.encoding = "utf-8"
    # レスポンスを表示
    soup = BeautifulSoup(response.text, "lxml")
    stations = {
        div.find("input", {"name": "stid"})["value"]: div
        for div in soup.find_all("div", class_="station")
    }

    def parse_hidden(parent: Tag):
        hiddens = parent.find_all("input", type="hidden")
        return {elm.get("name"): elm.get("value") for elm in hiddens}

    def parse_text(text):
        data = {}
        for line in text.split("\n"):
            pair = line.replace("：", ":").split(":")
            if len(pair) < 2:
                # print("Invalid line: %s" % line)
                continue
            data[pair[0]] = pair[1]
        return data

    def kansoku_items(bits):
        if bits is None or len(bits) < 5:
            return {}
        return {
            "rain": (bits[0] != "0"),
            "wind": (bits[1] != "0"),
            "temp": (bits[2] != "0"),
            "sun": (bits[3] != "0"),  # 日照のみ2が存在する。どういう意味？
            "snow": (bits[4] != "0"),
            "etc": (bits[5] != "0")
        }

    parsed = {
        id: {**parse_hidden(station), **parse_text(station["title"])}
        for id, station in stations.items()
    }

    for station in parsed.values():
        station.update(kansoku_items(station["kansoku"]))

    return parsed


def get_aggrg_periods() -> Dict[int, Dict[str, Union[str, List[int]]]]:
    """気象庁の観測項目選択ページから観測集計期間のリストを取得

    Returns:
        Dict[int, Tuple[str, Optional(int)]]: 観測集計期間のリスト
            key: 観測集計期間ID
            value: 観測集計期間と期間範囲リスト (範囲リストを持つのはN日別値のみ)
    """
    # GETリクエストを送信
    response = requests.get(ELEMENTS_URL)
    response.encoding = "utf-8"
    # レスポンスを表示
    soup = BeautifulSoup(response.text, "lxml")
    aggrg_period = soup.find(id="aggrgPeriod")

    def parse_periods(parent: Tag) -> Dict[str, Union[str, List[int]]]:
        content = {}
        if parent.find("select") is None:
            content["name"] = parent.find("span").text
        else:  # N日別値
            content["name"] = parent.find("input")["id"]
            content["range"] = [int(x["value"]) for x in parent.find_all("option")]
        return content

    periods = {
        int(inp["value"]): parse_periods(inp.parent)
        for inp in aggrg_period.find_all("input", {"name": "aggrgPeriod"})
    }
    return periods


def get_elements(aggrg_periods: int=1, is_type_num: int=1) -> Dict[str, str]:
    """気象庁の観測項目選択ページから観測項目のリストを取得

    Args:
        aggrgPeriods (int): 観測集計期間ID
        isTypeNumber (int): 観測項目の種類 (0: 気象要素, 1: 気象要素以外)

    Returns:
        Dict[str, str]: 観測項目IDと観測項目名の辞書
    """
    # POSTリクエストを送信
    data = {"aggrgPeriod": aggrg_periods, "isTypeNumber": is_type_num}
    response = requests.post(ELEMENTS_URL, data=data)
    response.encoding = "utf-8"
    # レスポンスを表示
    soup = BeautifulSoup(response.text, "lxml")

    def parse_items(elem):
        if "disabled" in elem.attrs:
            return {}
        value = elem["value"]
        item = {"name": elem["id"]}
        options = elem.parent.find_all("option")
        if options:
            item["options"] = [parse_number(x["value"]) for x in options]
        hidden = elem.parent.find("input", {"type": "hidden"})
        if hidden:
            item["hidden"] = parse_number(hidden.next_sibling.text.strip())
        return {value: item}

    items = {}
    # aggrgPeriods が 8XX の場合は期間クラス指定は1文字目 (8) のみ
    # 今のところ、複数桁のケースは 8XX のみ
    for td_ in soup.find_all("td", class_="kikan" + str(aggrg_periods)[0]):
        inp = td_.find("input", {"type": "checkbox", "name": "element"})
        items.update(parse_items(inp))

    return items


def parse_number(num_str: str) -> Union[int, float]:
    """文字列を、整数はintに、小数点を含むものはfloatに変換する

    Args:
        num_str (str): 変換する文字列

    Returns:
        Union[int, float]: 変換後の数値

    Raises:
        ValueError: intにもfloatにも変換できない場合
    """
    try:
        return int(num_str)
    except ValueError:
        return float(num_str)


def get_weather_as_csv(phpsessid: str, aggrg_period: int,
                       station: str, elements: List[int],
                       begin_date: date, end_date: date) -> pd.DataFrame:
    """気象庁の観測データダウンロードページから気象データを取得する

    Args:
        phpsessid (str): PHPSESSID
        aggrg_period (int): 観測集計期間ID
        station (str): 観測地点コード
        element (List[int]): 観測項目IDのリスト
        begin_date (date): 取得する期間の開始日
        end_date (date): 取得する期間の終了日

    Returns:
        pd.DataFrame: 気象データ

    Raises:
        ValueError: 観測項目IDが不正な場合
    """
    element_str = "[" + ",".join([f'["{elm}",""]' for elm in elements]) + "]"

    params = {
        "PHPSESSID": phpsessid,
        # 共通フラグ
        "rmkFlag": 1,         # 利用上注意が必要なデータを格納する
        "disconnectFlag": 1,  # 観測環境の変化にかかわらずデータを格納する
        "csvFlag": 1,         # すべて数値で格納する
        "ymdLiteral": 1,      # 日付は日付リテラルで格納する
        "youbiFlag": 0,       # 日付に曜日を表示する
        "kijiFlag": 0,        # 最高・最低（最大・最小）値の発生時刻を表示
        # 時別値データ選択
        "aggrgPeriod": aggrg_period,          # 日別値
        "stationNumList": f'["{station}"]',   # 観測地点コードのリスト
        "elementNumList": element_str,        # 項目IDのリスト
        "ymdList": (                          # 取得する期間
            f'["{begin_date.year}", "{end_date.year}",'
            f' "{begin_date.month}", "{end_date.month}",'
            f' "{begin_date.day}", "{end_date.day}"]'
        ),
        "jikantaiFlag": 0,         # 特定の時間帯のみ表示する
        "jikantaiList": "[1,24]",  # デフォルトは全部
        "interAnnualFlag": 1,      # 連続した期間で表示する
        # 以下、意味の分からないフラグ類
        "optionNumList": [],
        "downloadFlag": True,      # CSV としてダウンロードする？
        "huukouFlag": 0,
    }

    response = requests.post(CSVDL_URL, data=params)
    response.encoding = "shift_jis"
    return read_csv(response.text)


def read_csv(csv_text: str) -> pd.DataFrame:
    """気象庁が出すCSVをDataFrameとして読み込む

    Args:
        csv_text (str): CSVのテキスト

    Returns:
        pd.DataFrame: 気象データ
    """
    lines = csv_text.split("\n")
    # print(lines[0])

    df_ = pd.read_csv(StringIO("".join(lines[2:])), header=[0, 1, 2, 3])

    # ヘッダーの修正
    df_.columns = df_.columns.droplevel([2])
    df_.columns = pd.MultiIndex.from_tuples(
        [(x if 'Unnamed' not in x else '') for x in col] for col in df_.columns)

    # print(df.columns)
    return df_


def get_phpsessid() -> str:
    """気象庁の観測データダウンロードページからPHPSESSIDを取得する

    Returns:
        str: PHPSESSID
    """
    response = requests.get(DL_INDEX_URL)
    response.encoding = "utf-8"

    soup = BeautifulSoup(response.text, "lxml")
    return soup.find("input", {"id": "sid"})["value"]


if __name__ == "__main__":
    get_weather_as_csv(get_phpsessid(), 2, "c47662", [201],
                       date(2023, 1, 1), date(2023, 6, 31))
