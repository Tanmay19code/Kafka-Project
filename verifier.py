import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
import base64
import hashlib
from Crypto.Cipher import AES
from Crypto import Random
import requests


def json_deserializer(v):
    return 
    if v is None:
        try:
            return json.loads(v.decode('utf-8'))
        except json.decoder.JSONDecodeError:
            log.exception('Unable to decode: %s', v)
            return None

def serializer(message):
    return json.dumps(message).encode('utf-8')


value = []
consumer = KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='latest', value_deserializer=json_deserializer)
consumer.subscribe(['Leader_Topic'])    
for message in consumer :
    message_out = message
    value = message.value
BLOCK_SIZE = 16
pad = lambda s: s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * chr(BLOCK_SIZE - len(s) % BLOCK_SIZE)
unpad = lambda s: s[:-ord(s[len(s) - 1:])]

public_key = 'PUBLIC_KEY OF LEADER'
enc = base64.b64decode(value[leader_hash])
iv = enc[:16]  
cipher = AES.new(private_key, AES.MODE_CBC, iv)
leader_hash = unpad(cipher.decrypt(enc[16:]))             # what to do with it??


for transaction in value[transactions]:
    for txn in txnip:
        txnid = txn[transactionId]
        reciever = txn[publickey]

##Generate verifier HASH 

data = {('txnid',txn[transactionId]),('reciever',txn[publickey])}
verifier_hash = hashlib.sha256(data).hexdigest

private_key = 'PRIVATE_KEY OF VERIFIER'
hash = pad(verifier_hash)
iv = Random.new().read(AES.block_size)
cipher = AES.new(private_key, AES.MODE_CBC, iv)
verifier_hash_enc = base64.b64encode(iv + cipher.encrypt(hash))
value[verifier_hash] = verifier_hash_enc


latest_id = requests.get("http://localhost:20231/api/v1/publickey/transiD")

if txnid == latest_id:
    success_flag = 1
else:
    success_flag = 0


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)

producer.send('verifier',{('success_flag', success_flag),('block', message_out)})