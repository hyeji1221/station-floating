import csv
import urllib.request as urllib
from pathlib import Path

from xml.etree.ElementTree import ElementTree, fromstring
from elasticsearch import Elasticsearch, helpers


def station_info():
    BASE_DIR = Path(__file__).resolve().parent.parent
    f = open(str(BASE_DIR) + '\seoul\subway.csv', 'r', encoding='utf-8-sig')
    rdr = csv.reader(f)
    data = {}
    for station in rdr:
        if station[1] == "서울":
            station[1] += '역'
        if station[3] != '' and station[4] != '':
            data[station[1]] = [float(station[3]), float(station[4])]
    return data

def update():
    data = station_info()
    docs = []
    url = 'http://openapi.seoul.go.kr:8088/547171685163686f35324270474f6e/xml/CardSubwayStatsNew/1/600/20220208'
    es = Elasticsearch(['http://34.64.183.172:9200/'])

    response = urllib.urlopen(url)
    xml_str = response.read().decode('utf-8')
    tree = ElementTree(fromstring(xml_str))
    root = tree.getroot()

    for row in root.iter("row"):
        station = row.find('SUB_STA_NM').text
        if station in data:
            line = row.find('LINE_NUM').text
            ride = int(row.find('RIDE_PASGR_NUM').text)
            alight = int(row.find('ALIGHT_PASGR_NUM').text)
            place_x = data[station][0]
            place_y = data[station][1]

            doc = {
                "_index": "station",
                "_id": station,
                "_source": {
                    "line": line,
                    "station": station,
                    "location": {
                        "lat": place_x,
                        "lon": place_y
                    },
                    "ride": ride,
                    "alight": alight
                }
            }
            docs.append(doc)

    res = helpers.bulk(es, docs)
