import re
import os
import boto3
import configparser
import timeit

import numpy as np
import pandas as pd
import psycopg2


def immigration_data(config, bucket):
    """
    This function creates the immigration fact table and time dimension table from raw immigration data. Config file and bucket object is taken as input to this function.
    """
    
    # Define relevant lists
    drop_null_columns = ['entdepu', 'occup', 'insnum']
    drop_useless_columns = ['dtaddto' ,'count', 'entdepa', 'entdepd', 'matflag', 'admnum', 'dtadfile']
    remove_na_cols = ['i94bir', 'biryear', 'i94mode']
    to_int_cols = ["cicid" , "i94cit", "i94res", "i94mode", "i94bir", "i94visa", "i94yr", "i94mon", "biryear"]
    
    out_path = 's3://' + bucket.name + '/' + config['S3']['staging'] + '/'
    
    # Get immigration files
    print("Loading Immigration data")
    files = []
    keyword = config['S3']['rawdata'] + '/' + config['RAWDATA']['immigration_raw'] + '/'
    keyword = r'%s' % keyword
    for my_bucket in bucket.objects.all():
        string = re.compile(keyword) 
        check = string.search(my_bucket.key) 
        if check is not None:
            string = re.compile(r'.crc')
            remove = string.search(my_bucket.key) ## .crc files were created in S3 bucket. Thus removing those 
            if remove is None:
                files.append(my_bucket.key)
    
    # Load immigration data
    df = list()
    for i in files:
        tmp = pd.read_parquet('s3://' + bucket.name + '/' + i)
        df.append(tmp)
    
    df = pd.concat(df)
    print("Immigration data loaded.")
    
    df = df.drop(drop_null_columns, axis = 1)
    df = df.drop(drop_useless_columns, axis = 1)
    
    df.loc[:,remove_na_cols] = df.loc[:,remove_na_cols].fillna(-10)
    df.loc[:,to_int_cols] = df.loc[:,to_int_cols].astype(int)
    
    # Convert SAS dates
    df.loc[:,'arrdate'] = pd.to_timedelta(df.loc[:,'arrdate'], unit='D') + pd.Timestamp('1960-1-1')
    df.loc[:,'depdate'] = pd.to_timedelta(df.loc[:,'depdate'], unit='D') + pd.Timestamp('1960-1-1')
    df.loc[:,'arrdate'] = df.loc[:,'arrdate'].dt.date
    df.loc[:,'depdate'] = df.loc[:,'depdate'].dt.date
    
    print("Cleaning of Immigration data completed. Writing to S3")
    
    print(f"Data quality check: Immigration data has {df.shape[0]} rows and {df.shape[1]} columns")
    
    df.to_csv(out_path + config['STAGING']['fact_table'] + '/immigration.csv', index = False)
    
    print("Immigration data written to S3")
    
    print("Creating time dimension table")
    
    time = df['arrdate'].unique()
    time = np.append(time, df['depdate'].unique())
    time = pd.DataFrame(time)
    time.columns = ['date']
    time = time.dropna()
    time = time.drop_duplicates()
    time['date'] = pd.to_datetime(time['date'], errors = 'coerce')
    
    time['day'] = time['date'].dt.day.astype(int)
    time['month'] = time['date'].dt.month.astype(int)
    time['year'] = time['date'].dt.year.astype(int)
    time['weekofyear'] = time['date'].dt.week.astype(int)
    time['dayofweek'] = time['date'].dt.dayofweek.astype(int)
    time['date'] = time['date'].dt.date
    
    print(f"Data quality check: Time data has {time.shape[0]} rows and {time.shape[1]} columns")
    
    time.to_csv(out_path + config['STAGING']['time_dim_table'] + '/time.csv', index = False)
    
    print("Time dimension table written to S3")
    
def demogs_dim_table(config, bucket):
    """
    This function creates the demogs dimesnion table from demographics data and lookup tables. Config file and bucket object is taken as input to this function.
    """
    out_path = 's3://' + bucket.name + '/' + config['S3']['staging'] + '/'
    
    print("Loading Demogs data and state lookup table")
    file = 's3://' + bucket.name + '/' + config['S3']['RAWDATA'] + '/' + config['RAWDATA']['demogs']
    df = pd.read_csv(file, sep = ";")
    
    file = 's3://' + bucket.name + '/' + config['S3']['RAWDATA'] + '/' + config['RAWDATA']['lookup'] + '/' + config['RAWDATA']['state_lookup']
    state = pd.read_csv(file)
    
    print("Data loaded")
    print("Cleaning Demographics data")
    
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    ## Extract race level columns
    tmp = pd.pivot_table(df, values = 'count', columns = ['race'] , index = 'state_code')
    tmp.columns = tmp.columns.str.lower().str.replace(' ', '_')
    tmp.columns = ['american_indian_and_alaska_native', 'asian','black_or_african', 'hispanic_or_latino', 'white']
    tmp = tmp.reset_index()
    
    ## Summarizing Demographics table at State level
    df = df.groupby(['state_code', 'state', 'city']).agg({'median_age': np.mean,
                                                     'male_population': np.mean,
                                                     'female_population': np.mean,
                                                     'total_population': np.mean,
                                                     'number_of_veterans': np.mean,
                                                     'foreign-born': np.mean,
                                                     'average_household_size': np.mean}).reset_index(). \
                                                    groupby(['state_code', 'state']). \
                                                    agg({'median_age': np.mean,
                                                     'male_population': np.sum,
                                                     'female_population': np.sum,
                                                     'total_population': np.sum,
                                                     'number_of_veterans': np.sum,
                                                     'foreign-born': np.sum,
                                                     'average_household_size': np.mean}).reset_index()
    
    
    tmp.index = tmp['state_code']
    df.index = df['state_code']
    
    df = df.drop(['state_code','state'], axis = 1)
    tmp = tmp.drop('state_code', axis = 1)
    
    df = df.join(tmp)
    df = df.reset_index()
    df = df.fillna(0)
    
    state = state.drop('Unnamed: 0', axis = 1)
    df = df.merge(state, left_on = 'state_code', right_on = 'id', how = "right")
    df = df.fillna(-10)
    df = df.drop('id', axis = 1)
    # Change data type
    to_int_cols = ['american_indian_and_alaska_native', 'asian', 'black_or_african', 'hispanic_or_latino', 'white',
                     'male_population', 'female_population', 'total_population', 'number_of_veterans', 'foreign-born']
    df.loc[:,to_int_cols] = df.loc[:,to_int_cols].astype(int)
    
    print("Data Cleaning completed")
    
    print(f"Data quality check: Demogs data has {df.shape[0]} rows and {df.shape[1]} columns")
    
    df.to_csv(out_path + config['STAGING']['demogs_dim_table'] + '/demogs.csv', index = False)
    
    print("Demogs dimension table written to S3")
    
    
def lookup_dim_table(config, bucket):
    """
    This function creates the remaining dimesnion table from lookup tables. Config file and bucket object is taken as input to this function.
    """
    out_path = 's3://' + bucket.name + '/' + config['S3']['staging'] + '/'
    file = 's3://' + bucket.name + '/' + config['S3']['RAWDATA'] + '/' + config['RAWDATA']['lookup'] + '/'
    drop_col = ['Unnamed: 0']
    
    print("Reading data")    
    country = pd.read_csv(file + config['RAWDATA']['country_lookup']).drop_duplicates()
    visa = pd.read_csv(file + config['RAWDATA']['visa_lookup']).drop_duplicates()
    mode = pd.read_csv(file + config['RAWDATA']['mode_lookup']).drop_duplicates()
    
    print("Data loaded")
    
    country = country.drop('Unnamed: 0', axis = 1)
    visa = visa.drop('Unnamed: 0', axis = 1)
    mode = mode.drop('Unnamed: 0', axis = 1)
    
    print("Data cleaned")
    print(f"Data quality check: Country data has {country.shape[0]} rows and {country.shape[1]} columns")
    print(f"Data quality check: Visa data has {visa.shape[0]} rows and {visa.shape[1]} columns")   
    print(f"Data quality check: Mode data has {mode.shape[0]} rows and {mode.shape[1]} columns")
    
    print("Writing data to S3")
    country.to_csv(out_path + config['STAGING']['country_dim_table'] + '/country.csv', index = False)
    visa.to_csv(out_path + config['STAGING']['visa_dim_table'] + '/visa.csv', index = False)    
    mode.to_csv(out_path + config['STAGING']['mode_dim_table'] + '/mode.csv', index = False)
    print("Data written to S3")
    

def load_data_redshift(config, bucket, cur, conn):
    """
    This function loads the data into dimension and fact tables in the data warehouse. Config file and bucket object is taken as input to this function. 
    curr, conn: cursor and connection objects to DB
    """
    command = f"""copy {{}}
    from '{{}}'
    iam_role '{{}}'
    CSV
    IGNOREHEADER 1  
    BLANKSASNULL 
    EMPTYASNULL
    ACCEPTINVCHARS
    TRUNCATECOLUMNS
    TRIMBLANKS NUll 'None' 
    DELIMITER ',';
    """
    schema='ml_analytics'
             
    for i in config['STAGING'].values():
        start = timeit.default_timer()
        print(f"Loading {i} table to Redshift")
        table = i
        if i == 'immigration':
            table = 'fact_' + table
        else:
            table = 'dim_' + table
        table = schema + '.' + table
        location = 's3://' + bucket.name + '/' + config['S3']['staging'] + '/' + i + '/'
        cur.execute(command.format(table, location, config['AWS']['IAM_ROLE']))
        conn.commit()
        print(f"Table {i} loaded to Redshift")  
        end = timeit.default_timer()
        print(f"Data Quality check: Loading {table} table to Redshift took {int((end-start) / 60)} minutes")

    
def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    # If AWS credentials not defined in environment
    #os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    #os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

    #s3_creds = {'region_name':"us-west-2",
    #            'aws_access_key_id': config['AWS']['AWS_ACCESS_KEY_ID'],
    #            'aws_secret_access_key': config['AWS']['AWS_SECRET_ACCESS_KEY']}

    #client = boto3.client('s3')#, **s3_creds)
    resource = boto3.resource('s3')#, **s3_creds)
    bucket = resource.Bucket(config['S3']['BUCKET'])
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['REDSHIFT'].values()))
    cur = conn.cursor()
    
    immigration_data(config, bucket)
    demogs_dim_table(config, bucket)
    lookup_dim_table(config, bucket)
    load_data_redshift(config, bucket, cur, conn)

    conn.close()
    
if __name__ == "__main__":
    main()