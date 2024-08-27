# For running the server:
# 1) Start the redis server
# 2) Run the flask app (python app.py)

from flask import Flask, request, jsonify, abort
from uuid import uuid4
from datetime import datetime, timedelta
import threading
import time
import redis

app = Flask(__name__)

r = redis.Redis(host='localhost', port=6379, db=0)

KEY_TTL = 300
BLOCK_TTL = 60
KEEP_ALIVE_INTERVAL = 300

def current_time():
    return datetime.utcnow()

def expired(key_id):
    data = r.hgetall(key_id)
    if not data:
        return True
    created_time = datetime.strptime(data[b'created_at'].decode(), '%Y-%m-%d %H:%M:%S.%f')
    return current_time() > created_time + timedelta(seconds=KEY_TTL)

def unblock(key_id):
    r.hdel(key_id, 'blocked_at')
    r.sadd('available_keys', key_id)
    r.srem('blocked_keys', key_id)

def delete_key(key_id):
    r.delete(key_id)
    r.srem('available_keys', key_id)
    r.srem('blocked_keys', key_id)

def auto_release():
    while True:
        for key_id in r.smembers('blocked_keys'):
            data = r.hgetall(key_id)
            if not data:
                continue
            blocked_at = datetime.strptime(data[b'blocked_at'].decode(), '%Y-%m-%d %H:%M:%S.%f')
            if current_time() > blocked_at + timedelta(seconds=BLOCK_TTL):
                unblock(key_id.decode())
        time.sleep(1)

@app.route('/keys', methods=["POST"])
def create():
    key_id = str(uuid4())
    r.hset(key_id, 'created_at', str(current_time()))
    r.sadd('available_keys', key_id)
    return jsonify({'keyId': key_id}), 201

@app.route('/keys', methods=["GET"])
def retrieve():
    key_id = r.srandmember('available_keys')
    if not key_id:
        return abort(404)
    key_id = key_id.decode()
    r.srem('available_keys', key_id)
    r.hset(key_id, 'blocked_at', str(current_time()))
    r.sadd('blocked_keys', key_id)
    return jsonify({'keyId': key_id}), 200

@app.route('/keys/<key_id>', methods=['GET'])
def get_info(key_id):
    data = r.hgetall(key_id)
    if not data:
        return abort(404)
    is_blocked = b'blocked_at' in data
    blocked_at = data.get(b'blocked_at').decode() if is_blocked else None
    created_at = data[b'created_at'].decode()
    return jsonify({
        'isBlocked': is_blocked,
        'blockedAt': blocked_at,
        'createdAt': created_at
    }), 200

@app.route('/keys/<key_id>', methods=['DELETE'])
def remove(key_id):
    if not r.exists(key_id):
        return abort(404)
    delete_key(key_id)
    return '', 200

@app.route('/keys/<key_id>', methods=['PUT'])
def unblock_endpoint(key_id):
    if not r.exists(key_id):
        return abort(404)
    unblock(key_id)
    return '', 200

@app.route('/keepalive/<key_id>', methods=['PUT'])
def keep_alive(key_id):
    if not r.exists(key_id):
        return abort(404)
    r.hset(key_id, 'created_at', str(current_time()))
    return '', 200

if __name__ == '__main__':
    threading.Thread(target=auto_release, daemon=True).start()
    app.run(debug=True)
