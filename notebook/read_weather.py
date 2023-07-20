#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""気象庁のサイトから気象データを取得する"""

from typing import Dict
from bs4 import BeautifulSoup, Tag
import requests
import pandas as pd

WEATHER_COLNAME = ["日", "現地気圧", "海面気圧", "降水量合計", "降水量1時間最大", "降水量10分最大", "平均気温",
                  "最高気温", "最低気温", "平均湿度", "最小湿度", "平均風速", "最大風速", "最大風向き",
                  "最大瞬間風速", "最大瞬間風向き", "日照時間", "降雪合計", "最深積雪", "昼天気概況", "夜天気概況"]

# URLを指定
url = "https://www.data.jma.go.jp/obd/stats/etrn/view/daily_s1.php?prec_no={}&block_no={}&year={}&month={}&day={}&view=p1"

def get_weather_from_html(prec_no: int, block_no: int, year: int, month: int, day: int) -> pd.DataFrame:
    """気象データを取得する

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
    response = requests.get(url.format(prec_no, block_no, year, month, day))

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


from datetime import date
import urllib.request
import lxml.html


def encode_data(data):
    return urllib.parse.urlencode(data).encode(encoding="ascii")

def get_phpsessid():
    URL="http://www.data.jma.go.jp/gmd/risk/obsdl/index.php"
    xml = urllib.request.urlopen(URL).read().decode("utf-8")
    tree = lxml.html.fromstring(xml)
    return tree.cssselect("input#sid")[0].value
   


STATION_URL = "https://www.data.jma.go.jp/gmd/risk/obsdl/top/station"

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


def get_stations(pd: int) -> Dict[int, str]:
    """気象庁の観測地点情報から観測地点コードのリストを取得

    Args:
        pd (int): 観測地域コード
    
    Returns:
        Dict[int, str]: 観測地点コードと観測地点情報
    """
    # POSTリクエストを送信
    response = requests.post(STATION_URL, data={"pd": pd})
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
        id: {**parse_hidden(st), **parse_text(st["title"])}
        for id, st in stations.items()
    }

    for st in parsed.values():
        st.update(kansoku_items(st["kansoku"]))

    return parsed

# 観測所フラグ調査結果
#for id, st in stations.items():
#    if st["地点名"] == "父島":
#        print(id, st["kansoku"])
# a1646 100000, "降雨"
### kansoku[0] = 降雨
# a1468 111000, "気温", "降雨", "風速"
# a1509 110000, "降雨", "風速"
### kansoku[1] = 風速
### kansoku[2] = 気温
### a0370 111200, "気温", "降雨", "風速", "日照"
### s47662 111111, "気温", "降雨", "風速", "日照", "積雪", "その他"
### kansoku[3] = 日照
### kansoku[4] = 積雪
### kansoku[5] = その他


# 観測項目選択
def get_aggrgPeriods():
    URL="http://www.data.jma.go.jp/gmd/risk/obsdl/top/element"
    xml = urllib.request.urlopen(URL).read().decode("utf-8")  # HTTP GET
    tree = lxml.html.fromstring(xml)

    def parse_periods(dom):
        if dom.find("label") is not None:
            val = dom.find("label/input").attrib["value"]
            key = dom.find("label/span").text
            rng = None
        else:
            val = dom.find("input").attrib["value"]
            key = dom.find("span/label").text
            rng = list(map(lambda x: int(x.get("value")),
                           dom.find("span/select").getchildren()))
        return (key, (val, rng))

    perdoms = tree.cssselect("#aggrgPeriod")[0].find("div/div").getchildren()
    periods = dict(map(parse_periods, perdoms))
    return periods

def get_elements(aggrgPeriods: int=1, isTypeNumber: int=1):
    URL="http://www.data.jma.go.jp/gmd/risk/obsdl/top/element"
    data = encode_data({"aggrgPeriod": aggrgPeriods,
                        "isTypeNumber": isTypeNumber})
    xml = urllib.request.urlopen(URL, data=data).read().decode("utf-8")
    open("tmp.html", "w").write(xml)
    tree = lxml.html.fromstring(xml)

    boxes = tree.cssselect("input[type=checkbox]")
    options, items = boxes[0:4], boxes[4:]

    def parse_items(dom):
        if "disabled" in dom.attrib: return None
        if dom.name == "kijiFlag": return None
        name     = dom.attrib["id"]
        value    = dom.attrib["value"]
        options  = None
        select = dom.getnext().find("select")
        if select is not None:
            options = list(map(lambda x: int(x.get("value")),
                               select.getchildren()))
        return (name, (value, options))
    
    items = dict(filter(lambda x: x, map(parse_items, items)))
    return items


def download_csv(phpsessid: str, aggrPeriod: int, station: str, element: str,
                 begin_date: date, end_date: date):
    params = {
        "PHPSESSID": phpsessid,
        # 共通フラグ
        "rmkFlag": 1,        # 利用上注意が必要なデータを格納する
        "disconnectFlag": 1, # 観測環境の変化にかかわらずデータを格納する
        "csvFlag": 1,        # すべて数値で格納する
        "ymdLiteral": 1,     # 日付は日付リテラルで格納する
        "youbiFlag": 0,      # 日付に曜日を表示する
        "kijiFlag": 0,       # 最高・最低（最大・最小）値の発生時刻を表示
        # 時別値データ選択
        "aggrgPeriod": aggrPeriod,    # 日別値
        "stationNumList": f'["{station}"]',      # 観測地点IDのリスト
        "elementNumList": f'[["{element}",""]]', # 項目IDのリスト
        "ymdList": '["{}", "{}", "{}", "{}", "{}", "{}"]'.format(
            begin_date.year,  end_date.year,
            begin_date.month, end_date.month,
            begin_date.day,   end_date.day),       # 取得する期間
        "jikantaiFlag": 0,        # 特定の時間帯のみ表示する
        "jikantaiList": "[1,24]", # デフォルトは全部
        "interAnnualFlag": 1,     # 連続した期間で表示する
        # 以下、意味の分からないフラグ類
        "optionNumList": [],
        "downloadFlag": True,   # CSV としてダウンロードする？
        "huukouFlag": 0,
    }

    URL="http://www.data.jma.go.jp/gmd/risk/obsdl/show/table"
    data = encode_data(params)
    csv = urllib.request.urlopen(URL, data=data).read().decode("shift-jis")
    print(csv)


# aggrPeriod
# 1: 日別値
# 2: 半旬別値
# 4: 旬別値
# 5: 月別値
# 6: 3か月別値
# 8: N日別値 (81NN でN日別値 5日別なら815、10日別なら8110)
# 9: 時別値


if __name__ == "__main__":
    # print(get_aggrgPeriods())
    element = get_elements(get_aggrgPeriods()["時別値"][0])["気温"][0]
    station = get_stations(get_prefectures["東京"])["東京"]["id"]
    phpsessid = get_phpsessid()

    download_hourly_csv(phpsessid, station, element,
                        date(2014, 1, 1), date(2014, 1, 31))
