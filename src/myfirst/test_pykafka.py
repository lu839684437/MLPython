'''
Created on 2017年7月6日

@author: xxlu
'''
#test pykafka
from pykafka import KafkaClient
def as_produce(topic):
    client=KafkaClient('10.43.4.57: 9092')
    topic=client.topics[topic.encode('utf-8')]
    #同步处理，创造生产者产生消息,仅仅当消息已经确认为一簇消息的时候，进行返回
    with topic.get_sync_producer() as producer:
        for i in range(4):
            message='test message '+ str(i)
            print('产生第%d文章' %i)
            producer.produce(message.encode('utf-8'))
        #停止发送消息
        producer.stop()
        
    #异步推送，producer可以有选择的推送，立即推送现有的信息
#     with topic.get_producer(delivery_reports=True) as producer:
#         for i in range(10):
#             print('产生第%d文章' %i)
#             #发送的消息
#             message='test message'+str(i**2)
#             producer.produce(message.encode('utf-8'))
def balance_consumer(topic):
    client=KafkaClient('10.43.4.57:9092')
    topic=client.topics[topic.encode('utf-8')]
    consumer=topic.get_balanced_consumer(consumer_group='testg'.encode('utf-8'),auto_commit_enable=True,
                                         zookeeper_connect='10.43.4.57:2181')
    return consumer   
#产生单个消费真
def simple_consumer(topic):
    client=KafkaClient('10.43.4.57:9092')
    topic=client.topics[topic.encode('utf-8')]
    consumer = topic.get_simple_consumer(consumer_group='test'.encode('utf-8'), auto_commit_enable=True, auto_commit_interval_ms=1,  
                                     consumer_id='test')  
    return consumer        
if __name__ == '__main__':
    as_produce('s')
#     results=simple_consumer('testMy1')
    results=balance_consumer('s')
    for result in results:
        if result is not None:
            print(result.value)
    