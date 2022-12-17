import pandas as pd
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine
import logging

engine = create_engine("postgresql://postgres:admin@host.docker.internal:5434/postgres")

if __name__ == '__main__':
    currencies = pd.read_sql(f"select * from currencies", con=engine)

    # change datetime to timestamp
    date_ = date.today()
    # date_ = date(2022,12,6)
    today = datetime(date_.year, date_.month, date_.day)
    yesterday = today - timedelta(days=1)
    today = datetime.timestamp(today)
    yesterday = datetime.timestamp(yesterday)

    fact_daily_currency = currencies[(currencies.timestamp >= yesterday) & (currencies.timestamp < today)]
    fact_daily_currency = fact_daily_currency.groupby('currency_code').agg({'rate': 'mean'}).reset_index()
    fact_daily_currency.rename(columns={'rate':'daily_avg_rate'}, inplace=True)
    
    
    # Load to dwh
    try:
        res = fact_daily_currency.to_sql('fact_daily_currency', con=engine, schema='Schemas', index=False, if_exists='replace')
        logging.info(f'success insert data to table: fact_daily_currency, inserted {res} data')
    except Exception as e:
        logging.info('Failed to insert data to table: fact_daily_currency')
        logging.error(e)