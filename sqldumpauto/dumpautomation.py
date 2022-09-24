import os, os.path, time, sys
import subprocess

import boto3
from datetime import datetime
import shutil

sqlcred = {
    'HOST': '127.0.0.1',
    'PORT': '3307',
    'DB_USER': 'test',
    'DB_PASS': 'test',
    'database': 'pcbudge'}

mongocred = {
    'HOST': '127.0.0.1',
    'PORT': '27017',
    'username': '',
    'password': '',
    'database': 'PCBUDGE'
}

session = boto3.session.Session()
client = session.client('s3',
                        endpoint_url='https://onepercentstartups-001.sgp1.digitaloceanspaces.com/',
                        region_name='sgp1',
                        aws_access_key_id=' DO00YTW42UNAGUETY9L9',
                        aws_secret_access_key='1ysV9psqtOXGd9DNguezV6JwrnW0w8NpPvJGnCwPGf8')


def create_folder_backup():
    dt = datetime.now()
    directory = ('backups/bk_%s-%s-%s__%s_%s_%s' % (dt.month, dt.day, dt.year, dt.hour, dt.minute, dt.second))
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory


def get_dump(database, dir):
    filestamp = str(datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))
    command = f'mysqldump -h {sqlcred["HOST"]} -P {sqlcred["PORT"]} -u {sqlcred["DB_USER"]} -p{sqlcred["DB_PASS"]} "{sqlcred["database"]}" --result-file={os.getcwd() + "/" + dir + "/" + database[0] + "_" + filestamp}.sql'
    mysql_resp=subprocess.Popen(command)
    (output, err)=mysql_resp.communicate()
    status=mysql_resp.wait()

def get_mongo(database, collection=None, folder=None):
    filestamp = str(datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))

    if collection:
        command = f'mongodump --host={mongocred["HOST"]} --db={database} '\
                  f'--collection={collection} --username={mongocred["username"]}' +\
                  f' --password={mongocred["password"]} --gzip  --out={folder + "/" + database + "_" + filestamp}'
        mongo_resp=subprocess.Popen(command)
        (output, err) = mongo_resp.communicate()
        status = mongo_resp.wait()
    else:
        command = f'mongodump --host={mongocred["HOST"]} --db={database} '\
                  f' --username={mongocred["username"]}'\
                  f' --password={mongocred["password"]} --gzip  --out={folder + "/" + database + "_" + filestamp}'
        mongo_resp=subprocess.Popen(command)
        (output, err) = mongo_resp.communicate()
        status = mongo_resp.wait()

if __name__ == "__main__":
    dir = create_folder_backup()

    get_dump(sqlcred['database'], dir)

    if len(sys.argv) > 1:
        get_mongo(mongocred['database'], sys.argv[1], folder=dir)
    else:
        get_mongo(mongocred['database'], folder=dir)
    shutil.make_archive(dir, 'zip', dir)

    client.put_object(Bucket='dump',
                      Key=f'backups/{dir}.zip',
                      ACL='private',
                      )
