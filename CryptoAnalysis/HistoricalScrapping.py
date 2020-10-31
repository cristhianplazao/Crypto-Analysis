import pandas as pd
from bs4 import BeautifulSoup
import requests
import sys
import datetime as dt
import influxdb
import time

def historical():
    date_ = dt.datetime.now()
    fecha = date_.strftime("%Y%m%d")

    r = requests.get(f"https://coinmarketcap.com")
    soup = BeautifulSoup(r.text, "html.parser")
    tr_list = soup.find("table", {"class":"cmc-table cmc-table___11lFC cmc-table-homepage___2_guh"}).find("tbody").find_all("tr")
    uris = []
    symbols = []
    cryptos = []
    for tr in tr_list:
        try:
            uris.append(tr.find("a").get("href"))
            cryptos.append(tr.find("p",{"class":"Text-sc-1eb5slv-0 iTmTiC"}).get_text())
            symbols.append(tr.find("p",{"class":"Text-sc-1eb5slv-0 eweNDy coin-item-symbol"}).get_text())
        except AttributeError as error:
            pass

    client = influxdb.DataFrameClient(host="52.27.24.79", port=8086, username='admin', password='TaroCristhian71780')
    client.switch_database('daily')
    protocol = 'line'

    df_open = pd.DataFrame()
    df_vol = pd.DataFrame()
    df_max = pd.DataFrame()
    df_min = pd.DataFrame()
    df_close = pd.DataFrame()
    df_market = pd.DataFrame()
    uris_failed = []
    soup_failed = []

    for uri, symbol in zip(uris, symbols):
        trying_soup = 1
        trying_uris = 1
        while True:
            r = requests.get(f"https://coinmarketcap.com{uri}/historical-data/?start=20130428&end={fecha}")
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                date_ = soup.find_all("td",{"class":"cmc-table__cell cmc-table__cell--sticky cmc-table__cell--left"})
                first_six = soup.find_all("td",{"class":"cmc-table__cell cmc-table__cell--right"})
                if len(date_) > 0 and len(first_six) > 0:
                    dates = [date.findChildren()[0].get_text() for date in date_]
                    open_values = [float(open_val.findChildren()[0].get_text().replace(",","")) for open_val in [first_six[_open] for _open in range(0, len(first_six)-1, 6)]]
                    max_values = [float(max_val.findChildren()[0].get_text().replace(",","")) for max_val in [first_six[_open] for _open in range(1, len(first_six)-1, 6)]]
                    min_values = [float(min_val.findChildren()[0].get_text().replace(",","")) for min_val in [first_six[_open] for _open in range(2, len(first_six)-1, 6)]]
                    close_values = [float(close_val.findChildren()[0].get_text().replace(",","")) for close_val in [first_six[_open] for _open in range(3, len(first_six)-1, 6)]]
                    vol_values = [float(vol_val.findChildren()[0].get_text().replace(",","")) for vol_val in [first_six[_open] for _open in range(4, len(first_six)-1, 6)]]
                    market_values = [float(maket_cap.findChildren()[0].get_text().replace(",","")) for maket_cap in [first_six[_open] for _open in range(5, len(first_six), 6)]]
                    
                    df_open_temp = pd.DataFrame({
                        "date":dates,
                        f"{symbol}":open_values
                    })
                    df_open_temp["date"] = pd.to_datetime(df_open_temp["date"])
                    df_open_temp = df_open_temp.set_index("date").to_period("s")
                    df_open = pd.concat([df_open,df_open_temp],axis=1)

                    df_close_temp = pd.DataFrame({
                        "date":dates,
                        f"{symbol}":close_values
                    })
                    df_close_temp["date"] = pd.to_datetime(df_close_temp["date"])
                    df_close_temp = df_close_temp.set_index("date").to_period("s")
                    df_close = pd.concat([df_close,df_close_temp],axis=1)

                    df_max_temp = pd.DataFrame({
                        "date":dates,
                        f"{symbol}":max_values
                    })
                    df_max_temp["date"] = pd.to_datetime(df_max_temp["date"])
                    df_max_temp = df_max_temp.set_index("date").to_period("s")
                    df_max = pd.concat([df_max,df_max_temp],axis=1)

                    df_min_temp = pd.DataFrame({
                        "date":dates,
                        f"{symbol}":min_values
                    })
                    df_min_temp["date"] = pd.to_datetime(df_min_temp["date"])
                    df_min_temp = df_min_temp.set_index("date").to_period("s")
                    df_min = pd.concat([df_min,df_min_temp],axis=1)

                    df_vol_temp = pd.DataFrame({
                        "date":dates,
                        f"{symbol}":vol_values
                    })
                    df_vol_temp["date"] = pd.to_datetime(df_vol_temp["date"])
                    df_vol_temp = df_vol_temp.set_index("date").to_period("s")
                    df_vol = pd.concat([df_vol,df_vol_temp],axis=1)

                    df_market_temp = pd.DataFrame({
                        "date":dates,
                        f"{symbol}":market_values
                    })
                    df_market_temp["date"] = pd.to_datetime(df_market_temp["date"])
                    df_market_temp = df_market_temp.set_index("date").to_period("s")
                    df_market = pd.concat([df_market,df_market_temp],axis=1)

                    print(f"{symbol} added correctly")
                    
                    break

                    #df = pd.DataFrame({
                    #    "date":dates,
                    #    "open":open_values,
                    #    "max":max_values,
                    #    "min":min_values,
                    #    "close":close_values,
                    #    "volume":vol_values,
                    #    "market":market_values
                    #})
                    #df["date"] = pd.to_datetime(df["date"])
                    #df = df.set_index("date").to_period("s")
                    #client.write_points(df, symbol, protocol=protocol)
                    #print(f"{symbol} writed on influx")

                else:
                    if trying_soup == 5:
                        soup_failed.append(uri)
                        break
                    else:
                        time.sleep(2)
                        trying_soup = trying_soup + 1
                        print(f"{symbol} added soup try number {trying_soup}")
            else:
                if trying_uris == 5:
                    uris_failed.append(uri)
                    break
                else:                
                    time.sleep(2)
                    trying_uris = trying_uris + 1
                    print(f"{symbol} added soup try number {trying_uris}")

    client.write_points(df_open, "open", protocol=protocol)
    client.write_points(df_close, "close", protocol=protocol)
    client.write_points(df_max, "max", protocol=protocol)
    client.write_points(df_min, "min", protocol=protocol)
    client.write_points(df_vol, "volume", protocol=protocol)
    client.write_points(df_market, "market", protocol=protocol)

if __name__ == "__main__":
    historical()