FROM python:3.8

RUN pip install datetime requests influxdb pandas bs4 datup joblib

COPY CryptoAnalysis/HistoricalScrapping.py /opt/ml/code/CryptoAnalysis/HistoricalScrapping.py

RUN python3 /opt/ml/code/CryptoAnalysis/HistoricalScrapping.py