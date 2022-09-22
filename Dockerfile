FROM python:3.9

WORKDIR /src 

COPY /stocks-etl .

RUN pip install -r requirements.txt 

ENV PYTHONPATH=/src 

CMD ["python", "trading_price/pipeline/trading_price_pipeline.py"]
