from pymongo import MongoClient
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


if __name__ == '__main__':
    
    user = 'ZoomeRanged'
    password = 'sitorus1992oO'
    CONNECTION_STRING = 'mongodb+srv://ZoomeRanged:sitorus1992oO@cluster0.zbsedde.mongodb.net/?retryWrites=true&w=majority'

    conn = MongoClient(CONNECTION_STRING)
    
    engine = create_engine('postgresql+psycopg2://postgres:admin@localhost:5432/postgres')
    

    
    database = conn['sample_training']

    zips = database['zips']
    companies = database['companies']

    
    cursor_zips = zips.find({}, {'_id': 0})
    df_zips = pd.DataFrame.from_dict(list(cursor_zips))

    
    dakka = pd.DataFrame(df_zips['loc'].tolist())
    df_zips = pd.concat([df_zips, dakka], axis=1)
    df_zips.drop(['loc'], axis=1, inplace=True)
    df_zips.rename(columns={'x': 'latitude', 'y': 'longitude'}, inplace=True)
    df_zips.head()

    
    # delete array kecuali office
    frodo = [
        '_id','offices','image', 'products', 'relationships', 'competitions', 'providerships', 
        'funding_rounds', 'investments','acquisition','acquisitions','milestones','video_embeds',
        'screenshots','external_links','partners', 'ipo'
    ]

    
    bilbo = companies.aggregate([
        {"$addFields": {
            "office": {"$first": "$offices"}
        }},
        {"$unset" : frodo}
    ], allowDiskUse=True)
    df_companies = pd.DataFrame.from_dict(list(bilbo))

    
    baggins = {
        'description': '',
        'address1': '',
        'address2': '',
        'zip_code': '',
        'city': '',
        'state_code': '',
        'country_code': '',
        'latitude': None,
        'longitude': None
    }
    df_companies['office'] = np.where(df_companies['office'].notna(), df_companies['office'], baggins)

    
    temp = pd.DataFrame(df_companies['office'].tolist(), )
    companies_df = pd.concat([df_companies, temp], axis=1, )
    companies_df.drop(['office'], axis=1, inplace=True)
    companies_df.head()

    
   
    companies_df.to_sql('companies', engine, if_exists='replace')
    df_zips.to_sql('zips', engine, if_exists='replace')

    
    
        
    