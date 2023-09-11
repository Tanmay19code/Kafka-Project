import json

from kafka import KafkaProducer
from json import dumps


# def serializer(message):
#     return json.dumps(message).encode('utf-8')

def serialize_transaction_input_list(input_list_obj):
    input_list_dict = {
        "serialVersionUID": input_list_obj.serialVersionUID,
        "transactionId": input_list_obj.transactionId,
        "transactionOutputIndex": input_list_obj.transactionOutputIndex,
        "signature": input_list_obj.signature,
        "publicKey": input_list_obj.publicKey,
        "data": input_list_obj.data
    }
    return json.dumps(input_list_dict)

def serialize_transaction_output_list(output_list_obj):
    output_list_dict = {
        "serialVersionUID": output_list_obj.serialVersionUID,
        "value": output_list_obj.value,
        "publicKey": output_list_obj.publicKey
    }
    return json.dumps(output_list_dict)

# Define a custom serializer for the list of Transaction objects within Leader
def serialize_transaction_list(transaction_list):
    serialized_transaction = {
        "serialVersionUID": transaction_list.serialVersionUID,
        "transactionId": transaction_list.transactionId,
        "transactionInputList": serialize_transaction_input_list(transaction_list.transactionInputList),
        "transactionOutputList": serialize_transaction_output_list(transaction_list.transactionOutputList)
    }
    json.dumps(serialized_transaction).encode('utf-8')



def serialize_leader(leader_obj):
    leader_dict = {
        "serialVersionUID": leader_obj.serialVersionUID,
        "hashVal": leader_obj.hashVal,
        "previousBlockHash": leader_obj.previousBlockHash,
        "nonce": leader_obj.nonce,
        "transactions": serialize_transaction_list(leader_obj.transactions),
        "leaderHash": leader_obj.leaderHash,
        "verifierHash": leader_obj.verifierHash
    }
    return json.dumps(leader_dict).encode('utf-8')


class TransactionInputList:
    def __init__(self, serialVersionUID, transactionId, transactionOutputIndex, signature, publicKey, data):
        """
        long serialversionUID = -1L; String transactionid; int transactionoutputIndex; byte[] signature; String publickey; string data;
        """
        self.serialVersionUID = serialVersionUID if serialVersionUID !=None else -1
        self.transactionId = transactionId
        self.transactionOutputIndex = transactionOutputIndex
        self.signature = signature
        self.publicKey = publicKey
        self.data = data;

    def __str__(self):
        return f"[ serialVersionUID: {self.serialVersionUID}, transactionId: {self.transactionId}, transactionOutputIndex: {self.transactionOutputIndex}, signature: {self.signature}, publicKey: {self.publicKey}, data: {self.data} ]"

class TransactionOutputList:
    def __init__(self, serialVersionUID, value, publicKey):
        """
        long serialVersionUID = -1L; Double value; String PublicKey;
        """
        self.serialVersionUID = serialVersionUID  if serialVersionUID !=None else -1
        self.value = value
        self.publicKey = publicKey

    def __str__(self):
        return f"[ serialVersionUID:{self.serialVersionUID}, value:{self.value}, publicKey:{self.publicKey} ]"

class Transaction:
    def __init__(self,serialVersionUID, transactionId, transactionInputList, transactionOutputList):
        """ 
        long serialversionUID = -1L; string transactionid; list <TransactionInput> transactioninputList; list <TransactionOutput> transactionOutputList;
        """
        self.serialVersionUID = serialVersionUID  if serialVersionUID !=None else -1
        self.transactionId = transactionId
        self.transactionInputList = transactionInputList
        self.transactionOutputList = transactionOutputList

    def __str__(self):
        return f"[ serialVersionUID:{self.serialVersionUID}, transactionId:{self.transactionId}, transactionInputList:{self.transactionInputList}, transactionOutputList:{self.transactionOutputList} ]"

class Leader:
    def __init__(self, serialVersionUID, hashVal, previousBlockHash, nonce, transactions, leaderHash, verifierHash):
        """
        long serialVersionUID = -1L; String hash; String previousBlockHash; long nonce; list <Transaction> transactions; String leaderHash; String verifierHash;
        """
        self.serialVersionUID = serialVersionUID  if serialVersionUID !=None else -1
        self.hashVal = hashVal
        self.previousBlockHash = previousBlockHash
        self.nonce = nonce
        self.transactions = transactions
        self.leaderHash = leaderHash
        self.verifierHash = verifierHash

    def __str__(self):
        return f"Leader => [ serialVersionUID:{self.serialVersionUID}, hashVal:{self.hashVal}, previousBlockHash:{self.previousBlockHash}, nonce:{self.nonce}, transactions:{self.transactions}, leaderHash:{self.leaderHash}, verifierHash: {self.verifierHash} ]"

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serialize_leader
)

topic_name = "leader" 


data = Leader(100, "testHash", "testPrevBlockHash", 353, Transaction(4747, "testTransactionId",TransactionInputList(74747, "testTransactionId", 244, 1, "testPublicKey", "testData" ), TransactionOutputList(8838, 77.88, "testPublicKey")), "testLeaderHash", "testVerifierHash")
print("\n\n")
print(data)
print("\n\n")

producer.send(topic_name,data)

producer2 = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)
producer2.send(topic_name, "Test data")