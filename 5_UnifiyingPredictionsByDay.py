import datup as dt
import pandas as pd
import sys

def unifying(FREQ, H_STEPS, T_SEASONS, LAGS):    
    es = dt.ExponentialSmoothing(
        FREQ=FREQ,
        H_STEPS=H_STEPS,
        T_SEASONS=T_SEASONS,
        LAGS=LAGS
    )

    ins = dt.Datup(
        "AKIAJRKTJCOQZJ36YWFA",
        "x0HDqGg6nrGlpLYM4k3jRsd7pLOYp216jD8s1pUn",
        "coincrypto-datalake",
        suffix_name="unifying_cryptos_"+str(es.H_STEPS)+"_"
    )

    cryptos = ["BTCV","BTC"]
    convert_columns = ["Open","Min","Max","Close","Vol"]

    df_total_pred = pd.DataFrame()
    for list_cryptos in cryptos:
        df_pred_temp = pd.DataFrame()
        for convert in convert_columns:
            df_pred = dt.download_csv(ins,"output/data-pred/"+list_cryptos+"/"+str(es.H_STEPS)+"/"+list_cryptos+"-"+convert,list_cryptos+"-"+convert)
            df_pred["Crypto"] = list_cryptos+"/USD"
            df_pred = df_pred.rename(columns={"Unnamed: 0":"Date", "Ctd Pronostico":convert})
            if len(df_pred_temp) > 0:
                df_pred.drop(["Date","Crypto"],axis=1,inplace=True)
            df_pred.drop(["Max Pronostico","Min Pronostico"],axis=1,inplace=True)
            df_pred_temp = pd.concat([df_pred_temp,df_pred],axis=1)
        df_total_pred = pd.concat([df_pred_temp, df_total_pred],axis=0)
        
    dt.upload_csv(ins,df_total_pred,"output/data-pred/unifieds","Unified-"+str(es.H_STEPS)+"-STEPS")
    ins.logger.debug('Prediction exponential smoothing completed...')
    dt.upload_log(ins,logfile=ins.log_filename, stage='output/logs')

def persistent():
    ins = dt.Datup(
        "AKIAJRKTJCOQZJ36YWFA",
        "x0HDqGg6nrGlpLYM4k3jRsd7pLOYp216jD8s1pUn",
        "coincrypto-datalake",
        suffix_name="persistent_cryptos_"
    )

    df_newadd = dt.download_csv(ins,"output/data-pred/unifieds/Unified-1-STEPS","Unified-1-STEPS")
    cryptos = ["BTCV","BTC"]
    for crypto in cryptos:
        try:
            df_old = dt.download_csv(ins,"output/persistent/"+crypto,crypto)
            df = df_newadd.loc[df_newadd["Crypto"].isin([crypto+"/USD"])]
            if df.iloc[0,0] == df_old["Date"][0]:
                pass
            else:
                df = pd.concat([df,df_old],axis=0)
                dt.upload_csv(ins,df,"output/persistent",crypto)
        except FileNotFoundError:
            dt.upload_csv(ins,df_newadd.loc[df_newadd["Crypto"].isin([crypto+"/USD"])],stage="output/persistent",filename=crypto)

    ins.logger.debug('Prediction exponential smoothing completed...')
    dt.upload_log(ins,logfile=ins.log_filename, stage='output/logs')


if __name__ == "__main__":
    FREQ=["7","7","14","30"]
    H_STEPS=[1,3,7,15]
    T_SEASONS=[7,7,14,30]
    LAGS=10
    for f,s,l in zip(FREQ,H_STEPS,T_SEASONS):
      unifying(f,s,l,LAGS)  
    
    persistent()        
    sys.exit(0)