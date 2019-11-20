import configparser
import logging.config
import yaml
import os
import concurrent.futures
import requests
import threading
import sqlite3
from sqlite3 import Error

thread_local = threading.local()
conn = ""


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file, check_same_thread=False)
        return conn
    except Error as e:
        logger.error(e)
    return conn


def create_table(conn):
    create_table_sql = """ CREATE TABLE IF NOT EXISTS job_data (
                                pk_id integer PRIMARY KEY,
                                job_id text NOT NULL,
                                app_name text NOT NULL,
                                state text NOT NULL,
                                date_created text
                                ); """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        logger.error(e)


def insert_data(conn, data):
    sql = '''INSERT INTO job_data(pk_id, job_id, app_name, state, date_created)
                VALUES(:pk_id,:job_id,:app_name,:state,:date_created)'''
    c = conn.cursor
    print(data)
    c.execute(sql, data)
    conn.commit()


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def download_data(url):
    global conn
    session = get_session()
    with session.get(url) as response:
        info = response.json()
        insert_data(conn, info)
        logger.info(info)


def download_all_data(reqs, threads):
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(download_data, reqs)


def get_data(url, ids):
    data = []
    for id in ids:
        data.append(url+'/'+id)
    return data


def read_file(file):
    logger.info('Reading file..')
    try:
        job_ids = []
        with open(file) as txt_file:
            for line in txt_file:
                job_ids.append(line.strip())
    except:
        logger.error('Unable to read file.')
    return job_ids


def setup_logging(default_path='logging_listdir.yaml',
                  default_level=logging.INFO,
                  env_key='Log_CFG'):
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
    return 'Setting up logging.'


def main():
    config = configparser.ConfigParser()
    o = os.path.dirname(__file__)
    config.read(o+'config.ini')
    setup_logging()
    global conn
    try:
        conn = create_connection(rf"{config['main']['db_name']}")
        if conn is not None:
            try:
                create_table(conn)
            except Error as e:
                logger.error(e)
        else:
            logger.error("Unable to connect to DB.")
    except Error as e:
        logger.error(e)

    reqs = get_data(config['main']['api_url'], read_file(config['main']['file_name']))

    download_all_data(reqs, int(config['main']['threads']))


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    main()