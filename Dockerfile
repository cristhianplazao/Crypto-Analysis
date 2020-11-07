FROM python:3.8

RUN pip install datetime requests influxdb pandas bs4 datup joblib matplotlib scikit-learn

COPY CryptoAnalysis/HistoricalScrapping.py /opt/ml/code/CryptoAnalysis/HistoricalScrapping.py
COPY CryptoAnalysis/DailyScrapping.py /opt/ml/code/CryptoAnalysis/DailyScrapping.py
COPY CryptoAnalysis/GettingCryptosInfo.py /opt/ml/code/CryptoAnalysis/GettingCryptosInfo.py

ENV PYTHONUNBUFFERED=TRUE
ENTRYPOINT ["python3", "/opt/ml/code/CryptoAnalysis/HistoricalScrapping.py"]