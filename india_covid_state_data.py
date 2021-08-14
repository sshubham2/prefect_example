#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from lxml import html
import requests
from datetime import datetime
from sqlalchemy import create_engine
from prefect import task, Flow

@task
def get_date():
    page = requests.get('https://www.mohfw.gov.in/')
    tree = html.fromstring(page.content)

    date_string = tree.xpath('//h5/span/text()[1]')[0]
    date_string_split = date_string.split()
    _date = date_string_split[3]+date_string_split[4].lower().capitalize()+date_string_split[5][0:4]
    _date = datetime.strptime(_date, '%d%B%Y')
    _date  = _date.strftime("%m-%d-%Y")

    return _date


@task
#(max_retries=2, retry_delay=timedelta(minutes=120))
def get_state_data():
    state_data_df = pd.read_json('https://www.mohfw.gov.in/data/datanew.json')
    return state_data_df


@task
def transform_state_data(date, state_data_df):
    state_data_df = pd.DataFrame(state_data_df)
    #drop line containing total records
    state_data_df = state_data_df[:36]
    #drop 'sno' column
    state_data_df = state_data_df.drop(['sno'], axis=1)

    #append date extracted in datafeame
    state_data_df['date'] = date
    state_data_df.date = pd.to_datetime(state_data_df.date)

    #calculate new columns
    state_data_df['change_in_active'] = state_data_df['new_active']-state_data_df['active']
    state_data_df['change_in_positive'] = state_data_df['new_positive']-state_data_df['positive']
    state_data_df['change_in_cured'] = state_data_df['new_cured']-state_data_df['cured']
    state_data_df['change_in_death'] = state_data_df['new_death']-state_data_df['death']

    #rearrange columns
    state_data_df = state_data_df[['date', 'state_name', 'new_active', 'new_positive', 'new_cured',
       'new_death', 'active', 'positive', 'cured', 'death','change_in_active',
       'change_in_positive', 'change_in_cured', 'change_in_death','state_code']]
    
    return state_data_df

@task
def load_state_data(transformed_state_data):
    #engine = create_engine('sqlite:///covid_data.db', echo=True)
    engine = create_engine('postgresql://prefect:prefect198@localhost:5433/prefect_db')

    df = pd.DataFrame(transformed_state_data)
    df.to_sql('INDIA_COVID_STATE_DATA', con=engine,if_exists='append',index=False)


with Flow("India_Covid_State_Data-ETL") as flow:
    date = get_date()
    state_data = get_state_data()

    transformed_state_data = transform_state_data(date, state_data)

    load_data = load_state_data(transformed_state_data)

#flow.run()
flow.register(project_name="ICSD-ETL")