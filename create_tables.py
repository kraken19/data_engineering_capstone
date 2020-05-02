
# CREATE TABLES
schema = 'mlanalytics'

fact_immigration_table_create= (f"""
CREATE TABLE IF NOT EXISTS {{schema}}.fact_immigration(
cicid INTEGER PRIMARY KEY,
i94yr INTEGER,
i94mon INTEGER,
i94cit INTEGER,
i94res INTEGER,
i94port VARCHAR,
arrdate TIMESTAMP sortkey,
i94mode INTEGER,
i94addr VARCHAR,
depdate TIMESTAMP,
i94bir INTEGER,
i94visa INTEGER,
visapost VARCHAR,
biryear INTEGER,
gender VARCHAR,
airline VARCHAR,
fltno VARCHAR,
visatype VARCHAR
);
""")

dim_time_table_create = (f"""
CREATE TABLE IF NOT EXISTS {{schema}}.dim_time(
date TIMESTAMP PRIMARY KEY sortkey,
day INTEGER,
month INTEGER,
year INTEGER,
weekofyear INTEGER,
dayofweek INTEGER
);
""")

dim_demogs_table_create = (f"""
CREATE TABLE IF NOT EXISTS {{schema}}.dim_demogs(
id VARCHAR PRIMARY KEY,
state VARCHAR,
median_age FLOAT,
male_population INTEGER,
female_population INTEGER,
total_population INTEGER,
number_of_veterans INTEGER,
foreign_born INTEGER,
average_household_size FLOAT,
american_indian_and_alaska_native INTEGER,
asian INTEGER,
black_or_african INTEGER,
hispanic_or_latino INTEGER,
white INTEGER
)
DISTSTYLE ALL;
""")

dim_port_table_create = (f"""
CREATE TABLE IF NOT EXISTS {{schema}}.dim_port(
id VARCHAR PRIMARY KEY,
port VARCHAR,
state VARCHAR
)
DISTSTYLE ALL;
""")

dim_country_table_create = (f"""
CREATE TABLE IF NOT EXISTS {{schema}}.dim_country(
id INTEGER PRIMARY KEY,
country VARCHAR
)
DISTSTYLE ALL;
""")

dim_mode_table_create = (f"""
CREATE TABLE IF NOT EXISTS {{schema}}.dim_mode(
id INTEGER PRIMARY KEY,
mode VARCHAR
)
DISTSTYLE ALL;
""")

dim_visa_table_create = (f"""
CREATE TABLE IF NOT EXISTS {{schema}}.dim_visa(
id INTEGER PRIMARY KEY,
visa VARCHAR
)
DISTSTYLE ALL;
""")


