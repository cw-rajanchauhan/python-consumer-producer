from multiprocessing.managers import BaseManager
import logging
import time

class Producer():
    def __init__(self, service_name, listen_address='localhost', listen_port=5000):
        self.service_name = service_name
        self.name = 'Producer'
        self.listen_address = listen_address
        self.listen_port = listen_port
        self._job_channel = None
        self._secret_key = ''
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.DEBUG)

    def set_name(self, name):
        self.name = name
    
    def set_logger(self, logger):
        self.logger = logger

    def set_secret_key(self, key):
        self._secret_key = key

    def connect(self):
        BaseManager.register(self.service_name)
        manager = BaseManager(address=(self.listen_address, self.listen_port), authkey=bytes( self._secret_key, encoding='utf8'))
        manager.connect()
        self._job_channel = getattr(manager, self.service_name)()

    def produce(self, data):
        self._job_channel.put(data)

if __name__=='__main__':
    producer = Producer('SampleDataQueue')
    producer.set_secret_key('secret-key')
    producer.connect()
    for i in range(10):
        producer.produce("msg-{}".format(i))