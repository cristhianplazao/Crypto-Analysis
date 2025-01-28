import datup as dt
import pandas as pd
from bs4 import BeautifulSoup
import requests
import sys

ins = dt.Datup(
    "...",
    "...",
    "coincrypto-datalake",
    suffix_name="adding_new_data_"
)


def addingnew_values():
    cryptos = ["BTCV","BTC"]
    names_cryptos = ["bitcoin-vault","bitcoin"]
    for uri,s3 in zip(names_cryptos,cryptos):
        r = requests.get("https://coinmarketcap.com/currencies/"+uri+"/historical-data")
        soup = BeautifulSoup(r.text, "html.parser")
        date_ = soup.find("td",{"class":"cmc-table__cell cmc-table__cell--sticky cmc-table__cell--left"}).findChildren()[0].get_text()
        first_six = soup.find_all("td",{"class":"cmc-table__cell cmc-table__cell--right"})[:6]
        names = ["Open","Min","Max","Close","Vol","Cap"]
        six_values_d = dict()
        six_values_d["Date"] = [date_]
        for v,n in zip(first_six,names):
            six_values_d[n] = [str(v.get_text())]    
        df_coinmarketcap = pd.DataFrame(six_values_d)
        
        for column in df_coinmarketcap.columns[1:]:
            if column == "Vol" or column == "Cap":
                df_coinmarketcap[column] = df_coinmarketcap[column].apply(lambda x:x.replace(",","."))
            else:
                df_coinmarketcap[column] = df_coinmarketcap[column].apply(lambda x:x.replace(",","").replace(".",","))
        df=dt.download_csv(ins,"as-is/"+s3, filename=s3,datecols=True,sep=",")
        if df.iloc[0,0] == df_coinmarketcap.iloc[0,0]:
            pass
        else:
            df = pd.concat([df_coinmarketcap,df],axis=0)
            dt.upload_csv(ins,df,"as-is", s3)   
        
        del df;del df_coinmarketcap

    # Upload log
    ins.logger.debug('Training exponential smoothing completed...\n')
    dt.upload_log(ins,logfile=ins.log_filename,stage='output/logs')

if __name__ == "__main__":
    addingnew_values()
    sys.exit(0)