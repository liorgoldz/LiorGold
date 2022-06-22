pip install mysql-connector-python-rf
pip install randomuser

from randomuser import RandomUser
import requests
import csv
import io
import pandas as pd
import json
import mysql.connector

from mysql.connector import connection

conn = connection.MySQLConnection(user='interview_user',password='aviv_2021_07_06_!!@@QQ',host='104.197.7.195',
database='interview')

data = requests.get('https://randomuser.me/api/?results=4500')

jsondata = data.json()

df = pd.json_normalize(jsondata['results'],
                       meta=[['info','seed'], ['info','results'],['info','page'],['info','version']])
df.columns = df.columns.str.replace('.','_')

df_male = df.loc[df['gender'] == 'male']
df_female = df.loc[df['gender'] == 'female']

cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS LIOR_GOLD_test_female")
sql ='''CREATE TABLE LIOR_GOLD_test_female(gender CHAR(255) NOT NULL,
email CHAR(255) NOT NULL,
phone CHAR(255) NOT NULL,
cell CHAR(255) NOT NULL,
nat CHAR(255) NOT NULL,
name_title CHAR(255) NOT NULL,
name_first CHAR(255) NOT NULL,
name_last CHAR(255) NOT NULL,
location_street_number INT,
location_street_name CHAR(255) NOT NULL,
location_city CHAR(255) NOT NULL, 
location_state CHAR(255) NOT NULL, 
location_country CHAR(255) NOT NULL,
location_postcode CHAR(255) NOT NULL, 
location_coordinates_latitude CHAR(255) NOT NULL,
location_coordinates_longitude CHAR(255) NOT NULL, 
location_timezone_offset CHAR(255) NOT NULL,
location_timezone_description CHAR(255) NOT NULL, 
login_uuid CHAR(255) NOT NULL,
login_username CHAR(255) NOT NULL,
login_password CHAR(255) NOT NULL, 
login_salt CHAR(255) NOT NULL, 
login_md5 CHAR(255) NOT NULL, 
login_sha1 CHAR(255) NOT NULL,
login_sha256 CHAR(255) NOT NULL, 
dob_date CHAR(255) NOT NULL, 
dob_age INT, 
registered_date CHAR(255) NOT NULL,
registered_age INT, 
id_name CHAR(255) NOT NULL, 
id_value CHAR(255) , 
picture_large CHAR(255) NOT NULL,
picture_medium CHAR(255) NOT NULL, 
picture_thumbnail CHAR(255) NOT NULL,
age_group INT
)'''

cursor.execute(sql)

cols = "`,`".join([str(i) for i in df_female.columns.tolist()])

for i,row in df_male.iterrows():
    sql = "INSERT INTO `LIOR_GOLD_test_20` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql, tuple(row))

df["age_group"] = pd.cut(x=df['dob_age'], bins=[10,20,30,40,50,60,70,80,90,100], labels=['10','20','30','40','50','60','70','80','90'])

df10 = df[(df["age_group"] == 10)]
df20 = df[(df["age_group"] == 20)]
df30 = df[(df["age_group"] == 30)]
df40 = df[(df["age_group"] == 40)]
df50 = df[(df["age_group"] == 50)]
df60 = df[(df["age_group"] == 60)]
df70 = df[(df["age_group"] == 70)]
df80 = df[(df["age_group"] == 80)]
df90 = df[(df["age_group"] == 90)]

mycursor = conn.cursor()

top_20_m = mycursor.execute("SELECT * FROM LIOR_GOLD_test_male ORDER BY registered_date DESC limit 20 ")



top_20_fe = mycursor.execute("SELECT * FROM LIOR_GOLD_test_female ORDER BY registered_date DESC limit 20 ")