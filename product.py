from flask import Flask
from flask import jsonify
from pymongo import MongoClient
import requests

import copy
import os
import threading
import time
import uuid

DB_ADDR = 'DB_ADDR'
DB_PORT = 'DB_PORT'
DB_USER = 'DB_USER'
DB_PW = 'DB_PW'

DB_NAME = 'bbthe90s'
COL_NAME = 'products'

PRODUCT_PORT = 'PRODUCT_PORT'
PRODUCT_ADDR = 'PRODUCT_ADDR'

def connect_to_db():
    db_addr = os.environ.get(DB_ADDR)
    db_port = int(os.environ.get(DB_PORT))
    db_username = os.environ.get(DB_USER)
    db_pw = os.environ.get(DB_PW)

    if not db_addr or not db_port:
        # try default connection settings
        client = MongoClient()
    else:
        client = MongoClient(db_addr, db_port)
    return client


## Consul Support
class ProductConfig(object):
    def __init__(self):
        super().__init__()
        self.data = dict()
        self.lock = threading.Lock()

    def get(self):
        with self.lock:
            data = copy.deepcopy(self.data)

        return data

    def set(self, value):
        with self.lock:
            self.data = value


def watch_config(config):
    import consul
    index = None
    config.set(value=dict())
    c = None

    while True:
        try:
            c = consul.Consul()
            break
        except:
            time.sleep(1)


    while True:
        try:
            index,data = c.kv.get("product/", index=index, recurse=True)
            if data == None:
                config.set(dict())
            else:
                new_data = dict()
                for datum in data:
                    key = datum['Key'].replace('product/', '', 1)
                    value = datum['Value']
                    if value is not None:
                        value = value.decode('utf-8')
                        new_data[key] = value
                new_data['datacenter']=c.agent.self().get('Config').get('Datacenter')
                config.set(new_data)
        except (requests.exceptions.RequestException, consul.base.ConsulException):
            time.sleep(1)
            pass


db_client = connect_to_db()
config = ProductConfig()
thread = threading.Thread(target=watch_config, args=(config,))
thread.start()

app = Flask(__name__)

# these can be seeded into the DB for testing if necessary
prods = [{ 'inv_id': 1, 'name':'jncos', 'cost':35.57, 'img':None},
         { 'inv_id': 2, 'name':'denim vest', 'cost':22.50, 'img':None},
         { 'inv_id': 3, 'name':'pooka shell necklace', 'cost':12.37, 'img':None},
         { 'inv_id': 4, 'name':'shiny shirt', 'cost':17.95, 'img':None}]

@app.route("/product", methods=['GET'])
def get_products():
    res = get_products_from_db()
    return jsonify({'res': res, 'instance_id': app.config['INSTANCE_ID'], 'conf': app.config['CONFIG'].get()})

@app.route("/product/healthz", methods=['GET'])
def get_health():
    global_health = app.config['CONFIG'].get().get('run', None)
    my_health = app.config['CONFIG'].get().get('stop-{}'.format(
        app.config['INSTANCE_ID']), None)

    if my_health is not None:
        return "UNHEALTHY", 503
    elif global_health == None or global_health == 'true':
        return "OK"
    else:
        return "UNHEALTHY", 503


def get_products_from_db():
    return [rec for rec in db_client[DB_NAME][COL_NAME].find({}, {'_id': False})]

if __name__ == '__main__':
    PORT = os.environ.get(PRODUCT_PORT)
    ADDR = os.environ.get(PRODUCT_ADDR)
    INSTANCE_ID=str(uuid.uuid4())
    app.config.update(
            CONFIG=config,
            INSTANCE_ID=INSTANCE_ID)
    app.run(host=ADDR, port=PORT)
