import json
from pymongo import MongoClient
import gridfs
from twisted.internet import reactor, protocol
from txws import WebSocketFactory
client = MongoClient("10.34.0.33", 27017)
nama={}

class Sub(protocol.Protocol):
    def dataReceived(self, data):
        print data
        msg=""
        if data=="WEB":
            nama["WEB"]=self

        elif data=="/Gambar":
            db = client.dataGambar
            fs = gridfs.GridFS(db)
            print data
            dataDb= fs.find({})
            for document in dataDb :
                msg+=("</br>ID : "+str(document._id)+" "+"</br>Name : <a target='_blank' href='http://127.0.0.1:5000/api/getdata/"+str(document.filename)+"'>"+str(document.filename)+"</a></br>")
                self.transport.write(msg)

        elif data=="home/CO":
            db = client.dataCO
            print data
            dataDbCO= db.dataCO.find({})
            for document in dataDbCO :
                msg+=("</br>ID: "+str(document["_id"])+" TEMP: "+str(document["temperature"])+"</br>")
                self.transport.write(msg)

        else:
            databaru= json.loads(data)
            if databaru["data"]=="dataGAMBARbaru":
                db = client.dataGambar
                fs = gridfs.GridFS(db)
                dataDb=fs.find_one({"filename":databaru["filename"]})
                msg+=("</br>ID : "+str(dataDb._id)+" "+"</br>Name : <a target='_blank' href='http://127.0.0.1:5000/api/getdata/"+str(dataDb.filename)+"'>"+str(dataDb.filename)+"</a></br>")
                i=1
                for x in nama:
                        nama[x].message(msg)
                        print i
                        i+=1
                print msg

            elif databaru["data"]=="dataCObaru":
                print databaru["temperature"]
                msg+=("</br>ID : "+str()+str(databaru["temperature"]) +"</br>")
                i=1
                for x in nama:
                        nama[x].message(msg)
                        print i
                        i+=1
                print msg

    def message(self, message):
        self.transport.write(message)

def websocket():
    factory = protocol.ServerFactory()
    factory.protocol = Sub
    reactor.listenTCP(6666, factory)
    reactor.listenTCP(9898, WebSocketFactory(factory))
    reactor.run()

if __name__ ==  '__main__':
    websocket()