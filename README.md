# python-consumer-producer
Pythonic implementation of consumer producer 
Tested for Python 3

## Testing Application
**Console 1**: python consumer.py  
**Console 2**: python producer.py  
Above command produces file *test.txt* with message consumed by *consumer.py*
### Run in the same order consumer.py first and then producer.py

## Usage
### Consumer Usage
    def my_process(data):
        # some data processing
        pass
 
    consumer = Consumer('MyQueueName', address='localhost', port=5000, worker_count=2)
    # Uncomment below for adding authentication on consumer based on secret key
    # consumer.set_secret_key('mysecretkey')
    consumer.register_process(my_process)
    consumer.start() # started listening on port 5000

### Producer Usage
    producer = Producer('MyQueueName', address='localhost', port=5000)
    # Uncomment below for producing message to consumer with authentication
    # producer.set_secret_key('mysecretkey')
    producer.connect()
    producer.produce('message-1')
    producer.produce('message-2')
