import datup as dt
import pandas as pd
import sys

ins = dt.Datup(
    "...",
    "...",
    "coincrypto-datalake",
    suffix_name="preparing_data_"
)

def format_timeseries(df, dateindex, item, kpi, freq):
    """Return a dataframe with dates as index, items as dimensions and a kpi as records"""
    try:
        df[item] = df[item].astype(str)        
        df_gp = df.groupby([item, dateindex], as_index=False).agg({kpi: sum})                
        df_pivot = df_gp.pivot_table(index=[dateindex], columns=item, values=kpi, fill_value=0).reset_index().rename_axis(None, axis='columns').set_index(dateindex).asfreq(freq)
        df_pivot = df_pivot.resample(freq).sum()        
    except IOError as error:
        ins.logger.Exception(f'Exception found: {error}')
        raise
    return df_pivot

def preparing_data():
    cryptos = ["BTCV","BTC"]
    convert_columns = ["Open","Min","Max","Close","Vol"]

    for list_cryptos in cryptos:
        df=dt.download_csv(ins,"as-is/"+list_cryptos, filename=list_cryptos,datecols=True,sep=",")
        df["Date"] = pd.to_datetime(df["Date"])
        df["Crypto"]=list_cryptos+"/USD"
        for convert in convert_columns:
            df[convert] = df[convert].apply(lambda x:x.replace(".","").replace(",",".")).astype("float64")
            df_ts = format_timeseries(df,dateindex="Date",item="Crypto",kpi=convert,freq="D")
            df_ts = dt.transform_positive_timeseries(ins,df_ts, positive_fix=0.001)
            dt.upload_csv(ins,df_ts,"output/data-prepared/"+list_cryptos, list_cryptos+"-"+convert, index=True, ts_csv=True)

    # Upload log
    ins.logger.debug('Training exponential smoothing completed...\n')
    dt.upload_log(ins,logfile=ins.log_filename,stage='output/logs')

if __name__ == "__main__":
    preparing_data()
    sys.exit(0)