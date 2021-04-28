import datetime as dt
import pandas as pd 
import numpy as np
from ref import ref
from run_aws import s3, archive_bucket

def tz_datetime(t):
    #returns timestamp from Z format time (default in AWS output)
    y = dt.datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ")
    y = dt.datetime.strftime(y, '%Y/%m/%d %H:%M')
    time = pd.to_datetime(y)
    return time

def tz_date(t):
    #returns date from Z format time (default in AWS output)
    y = dt.datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ")
    y = dt.datetime.strftime(y, '%Y/%m/%d')
    time = pd.to_datetime(y)
    return time

from ref import airports 
def iata_to_icao(iata):
    try:
        icao = airports.loc[iata, 'ICAO']
    except KeyError:
        icao = np.nan
    return icao

def get_aws_key(type, airline, year, month, day):
    abbr = ref[airline]['abbr'].upper()
    m = str(month).zfill(2)
    if not day:
        prefix = f'{abbr}_monthly_{year}_{m}_{type}_data'
    else:
        d = str(day).zfill(2)
        prefix = f'{abbr}_daily_{year}_{m}_{d}_{type}_data'

    objs = s3.list_objects(Bucket=archive_bucket, Prefix=prefix)['Contents']
    key = objs[-1]['Key']
    return key

def read_from_aws(bucket, key, format_dict):
    from run_aws import s3
    import io
    obj = s3.get_object(Bucket=bucket, Key=key)
    #df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    
    if format_dict:
        df = pd.read_csv(
            io.BytesIO(obj['Body'].read()),
            header=0,
            usecols=list(format_dict.keys()),
            dtype=format_dict
        )
    else:
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    
    return df

def get_monthly_sla_scores(airline, year, month):
    date = dt.datetime(year, month, 1)
    prefix = f"{ref[airline]['abbr'].upper()}_monthly_{year}_{str(month).zfill(2)}" 
    monthly_format = {
            'SlaRuleID'     : str,
            'Successes'     : np.int64,  
            'TotalAttempts' : np.int64, 
        }
    try:
        sla_files = s3.list_objects(Bucket=archive_bucket,Prefix=f"{prefix}_sla_data")['Contents']
    except KeyError:
        print(f'No records in AWS for {date}')
        return None
    sla = read_from_aws(archive_bucket, sla_files[-1]['Key'], monthly_format)
    sla = sla[sla.Successes != 0]
    sla = sla.groupby('SlaRuleID').sum()
    sla['Score'] = sla.Successes / sla.TotalAttempts
    sla['Airline'] = ref[airline]['abbr'].upper()
    sla['Date'] = date
    try:
        sla_config_files = s3.list_objects(Bucket=archive_bucket,Prefix=f"{prefix}_slaconfig_data")['Contents']
    except KeyError:
        temp_prefix = f"{ref[airline]['abbr'].upper()}_monthly_{year}_{str(month - 1).zfill(2)}" 
        sla_config_files = s3.list_objects(Bucket=archive_bucket,Prefix=f"{temp_prefix}_slaconfig_data")['Contents']
        #return None
    sla_config = read_from_aws(archive_bucket, sla_config_files[-1]['Key'], None)
    sla_config = sla_config[['RuleId', 'Category']].set_index('RuleId')
    sla_summary = sla.merge(sla_config, how='left', left_index=True, right_index=True)
    monthly_summary = sla_summary.reset_index()[['Airline', 'Date', 'Category', 'SlaRuleID', 'Score']].fillna(0)
    return monthly_summary


