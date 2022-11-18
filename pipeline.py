import psycopg2
import json
import localstack_client.session as boto3
from datetime import date

# # Connect to your postgres DB
conn = psycopg2.connect("dbname=postgres user=postgres password=postgres host=localhost port=5432")
cur = conn.cursor()

sqs = boto3.client('sqs')
queue_url = 'http://localhost:4566/000000000000/login-queue'
response = sqs.receive_message(
    QueueUrl=queue_url,
    MaxNumberOfMessages=100
)

msg_lst = response['Messages']
today = date.today().strftime('%m/%d/%Y')
repldict = {'0':'a','1':'b','2':'c','3':'d','4':'e','5':'f','6':'g','7':'h','8':'i','9':'j'}
# print(msg_lst[0])
# print(type(msg_lst[0]['Body']))
for i in range(0,len(msg_lst)):
    res = json.loads(msg_lst[i]['Body'])
    for k, v in repldict.items():
        res['ip'] = res['ip'].replace(k, v)
        res['device_id'] =res['device_id'].replace(k, v)
        res['app_version']=res['app_version'].replace('.','')
    datalist = list(res.values())
    datalist.append(today)
    cur.execute("INSERT INTO user_logins(user_id, app_version, device_type, masked_ip, locale, masked_device_id, create_date) VALUES(%s, %s, %s, %s, %s, %s, %s)", datalist)

cur.execute("SELECT * FROM user_logins")
records = cur.fetchall()
print("here's the records you asked for... ",records)

conn.commit()
conn.close()