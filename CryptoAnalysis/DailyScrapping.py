import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import requests
import sys
import influxdb
import time
import datup
import re
import json

ins = datup.Datup(
    "AKIAJRKTJCOQZJ36YWFA",
    "x0HDqGg6nrGlpLYM4k3jRsd7pLOYp216jD8s1pUn",
    "coincrypto-datalake",
    suffix_name="daily_data_"
)

df_download = datup.download_csv(ins,"as-is/cryptos", "cryptos")

client = influxdb.DataFrameClient(host="35.163.83.223", port=8086, username='admin', password='TaroCristhian71780')

client.switch_database('daily')
protocol = 'line'

def regularexpressions_daily():

    date_ = datetime.now()
    yesterday = date_ - timedelta(days=1)
    fecha = yesterday.strftime("%Y-%m-%d")

    df_open = pd.DataFrame()
    df_vol = pd.DataFrame()
    df_max = pd.DataFrame()
    df_min = pd.DataFrame()
    df_close = pd.DataFrame()
    df_market = pd.DataFrame()
    
    uris_failed = []

    uris = df_download["coinmarket_cap_uri"].tolist()
    symbols = df_download["symbols"].tolist()

    for uri, symbol in zip(uris, symbols):
        if symbol == "HT":
            continue
        else:
            trying_uris = 1
            while True:
                r = requests.get(f"https://coinmarketcap.com{uri}/historical-data/")
                if r.status_code == 200:
                    soup = BeautifulSoup(r.text, "html.parser")
                    first = re.findall(r'{"props".+', str(soup))
                    second = re.findall(r'</script><script nomodule.+', str(first[0]))
                    json_file = json.loads(str(first[0]).replace(second[0],""))
                    try:
                        dates = fecha
                        open_values = json_file["props"]["initialProps"]["pageProps"]["info"]["statistics"]["openYesterday"]
                        close_values = json_file["props"]["initialProps"]["pageProps"]["info"]["statistics"]["closeYesterday"]
                        max_values = json_file["props"]["initialProps"]["pageProps"]["info"]["statistics"]["highYesterday"]
                        min_values = json_file["props"]["initialProps"]["pageProps"]["info"]["statistics"]["lowYesterday"]
                        vol_values = json_file["props"]["initialProps"]["pageProps"]["info"]["statistics"]["volumeYesterday"]
                        market_values = json_file["props"]["initialProps"]["pageProps"]["info"]["statistics"]["marketCap"]
                    except:
                        print("entré al exception")
                        break            

                    df_open_temp = pd.DataFrame({
                        "date":dates,
                        f"{symbol}":open_values
                    }, index=[0])
                    df_open_temp["date"] = pd.to_datetime(df_open_temp["date"])
                    df_open_temp = df_open_temp.set_index("date").to_period("s")
                    df_open = pd.concat([df_open,df_open_temp],axis=1)
                    df_close_temp = pd.DataFrame({
                        "date":dates,
                        f"{symbol}":close_values
                    }, index=[0])
                    df_close_temp["date"] = pd.to_datetime(df_close_temp["date"])
                    df_close_temp = df_close_temp.set_index("date").to_period("s")
                    df_close = pd.concat([df_close,df_close_temp],axis=1)
                    df_max_temp = pd.DataFrame({
                        "date":dates,
                        f"{symbol}":max_values
                    }, index=[0])
                    df_max_temp["date"] = pd.to_datetime(df_max_temp["date"])
                    df_max_temp = df_max_temp.set_index("date").to_period("s")
                    df_max = pd.concat([df_max,df_max_temp],axis=1)
                    df_min_temp = pd.DataFrame({
                        "date":dates,
                        f"{symbol}":min_values
                    }, index=[0])
                    df_min_temp["date"] = pd.to_datetime(df_min_temp["date"])
                    df_min_temp = df_min_temp.set_index("date").to_period("s")
                    df_min = pd.concat([df_min,df_min_temp],axis=1)
                    df_vol_temp = pd.DataFrame({
                        "date":dates,
                        f"{symbol}":vol_values
                    }, index=[0])
                    df_vol_temp["date"] = pd.to_datetime(df_vol_temp["date"])
                    df_vol_temp = df_vol_temp.set_index("date").to_period("s")
                    df_vol = pd.concat([df_vol,df_vol_temp],axis=1)
                    df_market_temp = pd.DataFrame({
                        "date":dates,
                        f"{symbol}":market_values
                    }, index=[0])
                    df_market_temp["date"] = pd.to_datetime(df_market_temp["date"])
                    df_market_temp = df_market_temp.set_index("date").to_period("s")
                    df_market = pd.concat([df_market,df_market_temp],axis=1)

                    print(f"{symbol} added correctly")
                    ins.logger.debug(f"{symbol} added correctly\n")           

                    break
                else:
                    if trying_uris == 5:
                        uris_failed.append(uri)
                        break
                    else:                
                        time.sleep(7)
                        trying_uris = trying_uris + 1
                        ins.logger.debug(f"{symbol} added uri try number {trying_uris}\n")
                        print(f"{symbol} added uri try number {trying_uris}")

            time.sleep(5)

    client.write_points(df_open.astype("float"), "open", protocol=protocol)
    client.write_points(df_close.astype("float"), "close", protocol=protocol)
    client.write_points(df_max.astype("float"), "max", protocol=protocol)
    client.write_points(df_min.astype("float"), "min", protocol=protocol)
    client.write_points(df_vol.astype("float"), "volume", protocol=protocol)
    client.write_points(df_market.astype("float"), "market", protocol=protocol)
    datup.upload_log(ins,logfile=ins.log_filename,stage='output/logs')

if __name__ == "__main__":
    regularexpressions_daily()