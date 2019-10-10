from multiprocessing import Process, Queue, Pool, Manager, freeze_support
from multiprocessing.managers import BaseManager
import logging
import os
import signal
import sys
import uuid

class Consumer():
    def __init__(self, service_name, address='localhost', port=5000, worker_count=1, queue_size=10):
        self.worker_count = worker_count
        self.job_channel = Queue(queue_size)
        self._all_processes = []
        self._manager = None
        self.service_name = service_name
        self.port = port
        self.address = address
        self.name = 'Consumer'
        self._secret_key = ''
        self._process = None
        self.logger = logging.getLogger(self.name)

    def set_name(self, name):
        self.name = name
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.DEBUG)
    
    def set_logger(self, logger):
        self.logger = logger

    def set_secret_key(self, key):
        self._secret_key = key

    def register_process(self, process_func):
        self._process = process_func

    def process_request(self):
        if not self._process:
            raise Exception('Consumer process not registered')
        req_queue = self.job_channel
        worker_id = uuid.uuid1()
        self.logger.info('Worker {} spawned pid - {}'.format(worker_id, os.getpid()))
        self.logger.info('ProcessRequest started for worker {}'.format(worker_id))
        while True:
            try:
                if not req_queue.empty():
                    data = req_queue.get()
                    self._process(data)
                    self.logger.debug('Retrieved data {} in {}'.format(data, worker_id))
            except Exception as ex:
                self.logger.error('Exception occured in worker {} : {}'.format(worker_id, str(ex)))
        self.logger.info('Worker {} terminating pid - '.format(worker_id, os.getpid()))

    def start(self):
        BaseManager.register(self.service_name, callable=lambda:self.job_channel)
        self._manager = BaseManager(address=(self.address, self.port), authkey=bytes(self._secret_key, encoding='utf8'))
        server = self._manager.get_server()
        self.logger.info('{} started on pid - {}'.format(self.name, os.getpid()))
        for i in range(self.worker_count):
            process = Process(target=self.process_request, args=())
            # process.daemon = True
            process.start()
            self._all_processes.append(process)
        try:
            signal.signal(signal.SIGINT, self.exit_gracefully)
            signal.signal(signal.SIGTERM, self.exit_gracefully)            
            server.serve_forever()
            self.logger.log('Started manager server.')
            for process in self._all_processes:
                process.join()
        except KeyboardInterrupt:
            self.logger.info('Keyboard Interrupt received')
            self.exit_gracefully(None, None)
        self.logger.info('{} stopped'.format(self.name))

    def exit_gracefully(self, signum, frame):
        self.logger.info('Graceful termination requested')
        for process in self._all_processes:
            process.terminate()
        for process in self._all_processes:
            self.logger.debug('{} status : {}'.format(process, process.is_alive()))
        self._manager.shutdown()
        self.logger.info('Graceful shutdown completed')
        sys.exit(1)


if __name__=='__main__':
    def test_process(data):
        with open('test.txt','a') as f:
            f.write('Consumed - {}'.format(str(data)))
    consumer = Consumer('SampleDataQueue', worker_count=2)
    consumer.set_name('Example Consumer')
    consumer.set_secret_key('secret-key')
    consumer.register_process(test_process)
    consumer.start()