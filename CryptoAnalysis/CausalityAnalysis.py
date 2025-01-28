import influxdb
import pandas as pd
import datup as dt
import datetime
import boto3
import matplotlib.pyplot as plt
from decimal import Decimal
from datetime import timedelta

s3 = boto3.resource('s3', aws_access_key_id='AKIAJRKTJCOQZJ36YWFA', aws_secret_access_key='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')

def upload_json(dataframe, bucket, io_json_key, out_json):
    try:
        dataframe.to_json('/tmp/' + out_json, orient='records', force_ascii=False)
        s3.Bucket(bucket).upload_file('/tmp/' + out_json, io_json_key + out_json)
    except FileNotFoundError as error:
        print(error)
        raise    
    return ("s3://{}/{}{}".format(bucket, io_json_key, out_json))

def upload_image_plotlib(data, bucket, key, filename):
    try:
        fig, ax = plt.subplots(1,1,figsize=(4,0.5))
        ax.plot(data,'#008000')
        for k,v in ax.spines.items():
            v.set_visible(False)
        ax.set_xticks([])
        ax.set_yticks([])
        plt.plot(len(data) -1, data[len(data) -1],'r.')
        ax.fill_between(range(len(data)), data, len(data)*[min(data)], alpha=0.1)
        plt.savefig(f"/tmp/{filename}", transparent=True, bbox_inches='tight', format="png")
        s3.Bucket(bucket).upload_file('/tmp/' + filename, key + filename)
        plt.close()
    except FileNotFoundError as error:
        print(error)

ins = dt.Datup(
    "AKIAJRKTJCOQZJ36YWFA",
    "x0HDqGg6nrGlpLYM4k3jRsd7pLOYp216jD8s1pUn",
    "coincrypto-datalake",
    suffix_name="correlation_process_"
)

def causality():
    client = influxdb.DataFrameClient(host="34.216.96.184", port=8086, username='admin', password='TaroCristhian71780')
    client.switch_database('daily')
    query = client.query("SELECT * FROM close order by time desc limit 90",database="daily")
    df = query["close"].fillna(0).sort_index(ascending=False)

    def correlation(df, days):
        if days == 1 :
            print("It is impossible correlate different vector with size 1")
        else :
            df = df.iloc[:days,:]
            corr = df.corr("spearman")    
        return corr

    date_ = datetime.datetime.now()
    tomorrow = date_ + timedelta(days=1)
    fecha = tomorrow.strftime("%Y-%m-%d")
    #fecha = date_.strftime("%Y-%m-%d")

    df_download = dt.download_csv(ins,"as-is/cryptos", "cryptos")

    process_calc = [3,7,15,30]

    main_list = []
    for column in df.columns:
        dict_ = dict()
        dict_["crypto"] = f"{column}"
        dict_["last_close_price"] = Decimal(str(df[column][0]))
        dict_["24h_pchange"] = df[column][:2].pct_change(fill_method="ffill")[1]
        data_15 = df[column][:15].sort_values(ascending=True).tolist()
        dict_["15_days_data"] = data_15
        upload_image_plotlib(data_15, "coincrypto-images", f"images/daily/{fecha}/", f"{column}.png")
        dict_["uri_15_graph"] = f"https://coincrypto-images.s3.amazonaws.com/images/daily/{fecha}/{column}.png"
        df_downloaded_temp = df_download.loc[df_download["symbols"]==f"{column}"]
        if len(df_downloaded_temp) > 0:
            key_temp = df_downloaded_temp.index[0]
            dict_["uri_image"] = df_downloaded_temp["uri_images_s3"][key_temp]
            dict_["uri_web"] = df_downloaded_temp["uri_web"][key_temp]
            days_list = []
            for process in process_calc:        
                temp_day = dict()
                df_corr = correlation(df, process)
                try:
                    corr = df_corr[column].sort_values(ascending=False)[:10]
                    caus = dt.causality_ts_granger(df[list(corr.index)],1)
                    caus = caus[f"{column}_x"].sort_values(ascending=False)
                    correlated = []
                    for causality,value in zip(caus.index,caus.values):
                        causality_name = causality.replace("_y","")                
                        temp_dict = dict()
                        temp_dict["crypto"] = causality_name
                        #temp_dict["value"] = value
                        df_downloaded_temp_intern = df_download.loc[df_download["symbols"]==f"{causality_name}"]
                        if len(df_downloaded_temp_intern) > 0:
                            key_temp_intern = df_downloaded_temp_intern.index[0]
                            temp_dict["uri_image"] = df_downloaded_temp_intern["uri_images_s3"][key_temp_intern]
                            temp_dict["uri_web"] = df_downloaded_temp_intern["uri_web"][key_temp_intern]
                            correlated.append(temp_dict)
                        else:
                            continue
                    temp_day["day"] = f"{process}"
                    temp_day["correlated_with"] = correlated
                    days_list.append(temp_day)
                    ins.logger.debug(f'Symbol: {column} well done processed in {process}')
                    print(f'Symbol: {column} well done processed in {process}')
                except KeyError as error:
                    temp_day["day"] = f"{process}"
                    correlated = []
                    for i in range(0,10):
                        temp_dict = dict()
                        temp_dict["crypto"] = "NO DATA"
                        #temp_dict["value"] = "NO DATA"
                        correlated.append(temp_dict)
                    temp_day["correlated_with"] = correlated
                    days_list.append(temp_day)
                    ins.logger.debug(f'Symbol: {column} not processed in {process}')
                    print(f'Symbol: {column} not processed in {process}')
            dict_["days"] = days_list
            main_list.append(dict_)
            ins.logger.debug(f'Symbol: {column} well total well done')
            print(f'Symbol: {column} well total well done')
        else:
            continue
        
    df = pd.DataFrame.from_dict(main_list)
    df_download["index"] = df_download.index
    df = pd.merge(df_download[["symbols","index"]], df, left_on="symbols", right_on="crypto")
    df = df.sort_values(by="index", ascending=True)
    upload_json(df, "coincrypto-datalake",f"web/{fecha}/",f"{fecha}.json")
    dt.upload_log(ins,logfile=ins.log_filename,stage='output/logs')

if __name__ == "__main__":
    causality()