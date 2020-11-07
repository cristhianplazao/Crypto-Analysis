import pandas as pd
from bs4 import BeautifulSoup
import requests
import sys
import datup
from PIL import Image
import boto3

s3 = boto3.resource('s3', aws_access_key_id='AKIAJRKTJCOQZJ36YWFA', aws_secret_access_key='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')

def upload_image(image, bucket, key, filename):
    try:
        image.save('/tmp/' + filename)
        s3.Bucket(bucket).upload_file('/tmp/' + filename, key + filename)
    except FileNotFoundError as error:
        print(error)
        raise    
    return ("s3://{}/{}{}".format(bucket, key, filename))

ins = datup.Datup(
    "AKIAJRKTJCOQZJ36YWFA",
    "x0HDqGg6nrGlpLYM4k3jRsd7pLOYp216jD8s1pUn",
    "coincrypto-datalake",
    suffix_name="currencies_collection_"
)

def gettingcryptosinfo():
    r = requests.get(f"https://coinmarketcap.com")
    soup = BeautifulSoup(r.text, "html.parser")
    tr_list = soup.find("table", {"class":"cmc-table cmc-table___11lFC cmc-table-homepage___2_guh"}).find("tbody").find_all("tr")
    uris = []
    symbols = []
    cryptos = []
    s3_images_uris = []
    uris_web = []

    df_download = datup.download_csv(ins,"as-is/cryptos", "cryptos")
    list_symbols = df_download["symbols"].tolist()
    list_images = df_download["uri_images_s3"].tolist()

    for tr in tr_list:
        try:
            exists = False      
            symbol = tr.find("p",{"class":"Text-sc-1eb5slv-0 eweNDy coin-item-symbol"}).get_text()            
            for symbol_downloaded, images_downloaded in zip(list_symbols, list_images):
                if symbol_downloaded == symbol:
                    if images_downloaded == "No Image":
                        try:
                            link = tr.find("a", {"class":"cmc-link"}).get("href")
                            request_symbol = requests.get(f"https://coinmarketcap.com{link}")
                            soup = BeautifulSoup(request_symbol.text, "html.parser")
                            image_uri = soup.find("div", {"class":"cmc-details-panel-header__name"}).find("img")["src"]
                            image = Image.open(requests.get(image_uri, stream = True).raw)
                            upload_image(image, "coincrypto-images", "images/cryptos/", f"{symbol}.png")
                            uri_s3 = f"https://coincrypto-images.s3.amazonaws.com/images/cryptos/{symbol}.png"
                            key = df_download.loc[df_download["symbols"].isin([symbol])].index[0]
                            df_download.at[key, "uri_images_s3"] = uri_s3
                            datup.upload_csv(ins, df_download, "as-is", "cryptos")
                            ins.logger.debug(f"{symbol} that exists without image has been modified with the image")
                            print(f"{symbol} that exists without image has been modified with the image")
                        except:
                            ins.logger.debug(f"{symbol} that exists has not been modified")
                            print(f"{symbol} that exists has not been modified")
                    else:
                        pass      
                    ins.logger.debug(f"{symbol} already exists")
                    print(f"{symbol} already exists")
                    exists = True
                    break
                else:
                    exists = False
            if exists :
                continue
            else :
                uris.append(tr.find("a").get("href"))
                cryptos.append(tr.find("p",{"class":"Text-sc-1eb5slv-0 iTmTiC"}).get_text())               
                symbols.append(symbol)
                try:
                    image_uri = tr.find("img",{"class":"coin-logo"})["src"]
                    image = Image.open(requests.get(image_uri, stream = True).raw)
                    upload_image(image, "coincrypto-images", "images/cryptos/", f"{symbol}.png")
                    uri_s3 = f"https://coincrypto-images.s3.amazonaws.com/images/cryptos/{symbol}.png"
                    s3_images_uris.append(uri_s3)
                    uris_web.append(f"/cryptocurrencie/{symbol}")
                    ins.logger.debug(f"New symbol: {symbol} and its image added correctly")
                    print(f"New symbol: {symbol} and its image added correctly")
                except TypeError as error:
                    s3_images_uris.append("No Image")
                    uris_web.append(f"/cryptocurrencie/{symbol}")
                    ins.logger.debug(f"New symbol: {symbol} added correctly. Image doesn't exists")
                    print(f"New symbol: {symbol} added correctly. Image doesn't exists")
        except AttributeError as error:
            ins.logger.debug(f"{error} error")
            pass

    if len(symbols) > 0 : 
        df_news = pd.DataFrame({
            "coinmarket_cap_uri":uris,
            "cryptos_name":cryptos,
            "symbols":symbols,
            "uri_images_s3":s3_images_uris,
            "uri_web":uris_web
        })
        df = pd.concat([df_download,df_news], 0)
        s3_cryptos = datup.upload_csv(ins, df, "as-is", "cryptos")
        ins.logger.debug(f"process finished and saved in {s3_cryptos}")
    else:
        ins.logger.debug(f"process finished nothing to save")
    datup.upload_log(ins,logfile=ins.log_filename,stage='output/logs')

if __name__ == "__main__":
    gettingcryptosinfo()