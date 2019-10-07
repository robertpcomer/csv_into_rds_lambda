from __future__ import print_function
import pymysql
import csv
import boto3
import urllib
import uuid
import datetime

## Specify the databaase credendtials, RDS host and the database name
rds_host = ''
db_username = ''
db_password = ''
db_name = ''


## Lamda Handler is what will be triggered by AWS Lambda

def lambda_handler( event, context ):


    s3 = boto3.client('s3')
    bucket =event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)


    path = ("s3://" + bucket + "/" + key)


  



    s3.download_file(bucket,key,download_path)

    


    conn = pymysql.connect(host=rds_host, port=3306, user=db_username, passwd=db_password, db=db_name, database=db_name)
    conn.select_db('gl1414')
    cur = conn.cursor()
    cur.execute("select * from gl1414.jpfitbit2;")

    #f = open(path, 'rt')
    csv_data = csv.reader(file(download_path))
    next(csv_data)
    username, filename = key.split('.')
    for row in csv_data:
        newdate=row[0].replace("-","")
        cur.execute("insert into jpfitbit (dateTime, foods_log_caloriesIn, foods_log_water, activities_calories, activities_caloriesBMR, activities_steps, activities_distance, activities_floors, activities_elevation, activities_minutesSedentary, activities_minutesLightlyActive, activities_minutesFairlyActive, activities_minutesVeryActive, sleep_timeInBed, sleep_minutesAsleep, sleep_awakeningsCount, sleep_minutesAwake, sleep_minutesToFallAsleep, sleep_minutesAfterWakeup, sleep_efficiency, body_weight, body_bmi, body_fat, Name) \
                 VALUES \
             (" + str(newdate) + "," + str(row[1]) + "," + str(row[2]) + "," + str(row[3]) + "," + str(row[4]) + "," + str(
            row[5]) + "," + str(row[6]) + "," + str(row[7]) + "," + str(row[8]) + "," + str(row[9]) + "," + str(
            row[10]) + "," + str(row[11]) + "," + str(row[12]) + "," + row[14] + "," + str(row[15]) + "," + str(
            row[16]) + "," + str(row[17]) + "," + str(row[18]) + "," + str(row[19]) + "," + str(row[20]) + "," + str(
            row[21]) + "," + str(row[22]) + "," + str(row[23]) + ", '"+str(username)+"')")
        conn.commit()

    count = 0

    cur.close()

    conn.close()

