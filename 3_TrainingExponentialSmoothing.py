import datup as dt
import pandas as pd
import sys

def train(FREQ, H_STEPS, T_SEASONS, LAGS):
    es = dt.ExponentialSmoothing(
        FREQ=FREQ,
        H_STEPS=H_STEPS,
        T_SEASONS=T_SEASONS,
        LAGS=LAGS
    )

    ins = dt.Datup(
        "...",
        "...",
        "coincrypto-datalake",
        suffix_name="training_cryptos_"+str(es.H_STEPS)+"_"
    )

    cryptos = ["BTCV","BTC"]
    convert_columns = ["Open","Min","Max","Close","Vol"]

    EVAL_METRIC='rmsep'
    all_errors = []
    for list_cryptos in cryptos:
        for convert in convert_columns:
            ts = dt.download_csv(ins,"output/data-prepared/"+list_cryptos+"/"+list_cryptos+"-"+convert,list_cryptos+"-"+convert,indexcol="Date",ts_csv=True,freq="D")
            stationarity = True
            if stationarity: trans_statu = False
            else: trans_statu = True
            ann, ann_perf = es.train_ann(ts,transform=trans_statu)
            aan, aan_perf = es.train_aan(ts,transform=trans_statu)
            adn, adn_perf = es.train_adn(ts,transform=trans_statu)
            ana, ana_perf = es.train_ana(ts,transform=trans_statu)
            ana_ss, ana_ss_perf = es.train_statespace_ana(ts,transform=trans_statu)
            aaa, aaa_perf = es.train_aaa(ts,transform=trans_statu)
            aaa_ss, aaa_ss_perf = es.train_statespace_aaa(ts,transform=trans_statu)
            ada, ada_perf = es.train_ada(ts,transform=trans_statu)
            ada_ss, ada_ss_perf = es.train_statespace_ada(ts,transform=trans_statu)
            the_model, the_errors = es.select_best_model(ins,EVAL_METRIC, ann_perf, aan_perf, adn_perf, ana_perf, ana_ss_perf, aaa_perf, aaa_ss_perf, ada_perf, ada_ss_perf)        
            all_errors.append(the_errors)
            if stationarity:     
                models = {'ann-hw':ann, 'aan-hw':aan, 'adn-hw':adn, 'ana-hw':ana, 'ana-ss':ana_ss, 'aaa-hw':aaa, 'aaa-ss':aaa_ss, 'ada-hw':ada, 'ada-ss':ada_ss}
            else:    
                models = {'ann-ht':ann, 'aan-ht':aan, 'adn-ht':adn, 'ana-ht':ana, 'ana-st':ana_ss, 'aaa-ht':aaa, 'aaa-st':aaa_ss, 'ada-ht':ada, 'ada-st':ada_ss}
            model_obj = es.upload_best_model(ins,ts,best_model=the_model, item_id=list_cryptos+"-"+convert, stage='output/data-modeled/'+list_cryptos+"/"+str(es.H_STEPS),**models)
            resids = es.compute_ets_residuals(ins,ts,model=model_obj)
            dt.test_gaussianity(ins,residuals=resids)
            dt.test_acorr(ins,residuals=resids)
            df_all_errors = pd.DataFrame(all_errors, columns=['Model', 'AIC', 'AICc', 'RMSE', 'RMSEP', 'MAE', 'MAPE', 'MAEP', 'MASE'])

    ins.logger.debug('Average RMSE:{rmse:.3f} RMSEP:{rmsep:.3f} MAE:{mae:.3f} MAPE:{mape:.3f} MAEP:{maep:.3f} MASE:{mase:.3f}'.format(rmse=df_all_errors.loc[:,'RMSE'].mean(), mae=df_all_errors.loc[:,'MAE'].mean(), mape=df_all_errors.loc[:,'MAPE'].mean(),maep=df_all_errors.loc[:,'MAEP'].mean(), mase=df_all_errors.loc[:,'MASE'].mean(), rmsep=df_all_errors.loc[:,'RMSEP'].mean()))
    print('Average RMSE:{rmse:.3f} RMSEP:{rmsep:.3f} MAE:{mae:.3f} MAPE:{mape:.3f} MAEP:{maep:.3f} MASE:{mase:.3f}'.format(rmse=df_all_errors.loc[:,'RMSE'].mean(), mae=df_all_errors.loc[:,'MAE'].mean(), mape=df_all_errors.loc[:,'MAPE'].mean(),maep=df_all_errors.loc[:,'MAEP'].mean(),mase=df_all_errors.loc[:,'MASE'].mean(), rmsep=df_all_errors.loc[:,'RMSEP'].mean()))
    dt.upload_csv(ins,df_all_errors,stage='output/errors-modeled',filename='errors-cryptos')

    # Upload log
    ins.logger.debug('Training exponential smoothing completed...\n')
    dt.upload_log(ins,logfile=ins.log_filename,stage='output/logs')

if __name__ == "__main__":
    FREQ=["7","7","14","30"]
    H_STEPS=[1,3,7,15]
    T_SEASONS=[7,7,14,30]
    LAGS=10
    for f,s,l in zip(FREQ,H_STEPS,T_SEASONS):
      train(f,s,l,LAGS)    
      
    sys.exit(0)