import datup as dt
import pandas as pd
import sys

def predict(FREQ, H_STEPS, T_SEASONS, LAGS):
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
        suffix_name="predict_cryptos_"+str(es.H_STEPS)+"_"
    )

    cryptos = ["BTCV","BTC"]
    convert_columns = ["Open","Min","Max","Close","Vol"]

    for list_cryptos in cryptos:
        for convert in convert_columns:
            ts = dt.download_csv(ins,"output/data-prepared/"+list_cryptos+"/"+list_cryptos+"-"+convert,list_cryptos+"-"+convert,indexcol="Date",ts_csv=True,freq="D")[1:]
            ets_name = dt.get_model_name(ins,stage="output/data-modeled/"+list_cryptos+"/"+str(es.H_STEPS), item_id=list_cryptos+"-"+convert)
            ets_model = dt.download_model(ins,stage="output/data-modeled/"+list_cryptos+"/"+str(es.H_STEPS), item_id=list_cryptos+"-"+convert, filename=ets_name[0]) 
            pred, ci = es.ets_forecast(ts,model_name=ets_name,model=ets_model)
            df_pred_ci = pd.concat([pred, ci], axis='columns')
            dt.upload_csv(ins,df_pred_ci,stage='output/data-pred/'+list_cryptos+"/"+str(es.H_STEPS),filename=list_cryptos+"-"+convert, ts_csv=True, index=True)  

    ins.logger.debug('Prediction exponential smoothing completed...')
    dt.upload_log(ins,logfile=ins.log_filename, stage='output/logs')

if __name__ == "__main__":
    FREQ=["7","7","14","30"]
    H_STEPS=[1,3,7,15]
    T_SEASONS=[7,7,14,30]
    LAGS=10
    for f,s,l in zip(FREQ,H_STEPS,T_SEASONS):
      predict(f,s,l,LAGS)    
      
    sys.exit(0)