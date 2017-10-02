from pymongo import MongoClient
import gridfs, pickle
import paho.mqtt.client as mqtt
import httplib, json,socket
from flask import Flask, request
import datetime
app2 = Flask(__name__)
mqttc = mqtt.Client("server", clean_session=False)
mqttc.connect("10.34.15.56", 1883)
host = '127.0.0.1'
port = 6666
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host, port))
conn = httplib.HTTPConnection("localhost:5000")
client = MongoClient("10.34.0.33", 27017)

#SUBSCRIBER
def on_message(mqttc,obj,msg):
    if msg.topic=='/Gambar':
        headers = {"Content-type": "application/json"}
        params = msg.payload
        conn.request("POST", "/api/postdata", params, headers)
        response = conn.getresponse()
        print response.read()

    elif msg.topic=='home/CO' :
        headers = {"Content-type": "application/json"}
        params = msg.payload
        conn.request("POST", "/api/postdataco", params, headers)
        response = conn.getresponse()
        print response.read()

#POST DATA GAMBAR
@app2.route('/api/postdata', methods=['POST'])
def postdata():
        data = request.get_json()
        db = client.dataGambar
        fs = gridfs.GridFS(db)
        #print type(data)
        print data["Name"]
        message = json.dumps(data)
        fs.put(message,filename=data["Name"])
        baru={"data":"dataGAMBARbaru","filename":data["Name"]}
        jsonbaru = json.dumps(baru)
        s.send(jsonbaru)
        print jsonbaru
        return "DATA GAMBAR TERKIRIM"

#POST DATA CO
@app2.route('/api/postdataco', methods=['POST'])
def postdataco():
        data = request.get_json()
        db = client.dataCO
        print data
        #timestamp = str(datetime.datetime.now())
        json.dumps(data)
        db.dataCO.insert_one({
            'protocol':data['protocol'],
            'temperature':data['temperature'],
            'timestamp':data['timestamp'],
            'humidity':data['humidity'],
            'topic':data['topic']})
        baru={"data":"dataCObaru",
            "protocol":data["protocol"],
            "temperature":data["temperature"],
            "timestamp":data["timestamp"],
            "humidity":data["humidity"],
            "topic":data["topic"]}
        jsonbaru = json.dumps(baru)
        s.send(jsonbaru)
        print jsonbaru
        return "DATA CO TERKIRIM"



#GET DATA GAMBAR
@app2.route('/api/getdata/<string:name>', methods=['GET'])
def getdata(name):
    db = client.dataGambar
    fs = gridfs.GridFS(db)
    with open(name, "wb") as fInput:
            getdata=fs.find_one({"filename": name})._id
            x=fs.get(getdata).read()
            y=json.loads(x)
            z=pickle.loads(y["Data"])
            fInput.write(z)
            return "DATA GAMBAR BERHASIL DIAMBIL"


mqttc.on_message = on_message

mqttc.subscribe("/Gambar")
mqttc.subscribe("home/CO")

if __name__ ==  '__main__':

    mqttc.loop_start()
    app2.run(debug=True)
