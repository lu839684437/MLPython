# -*- coding: utf-8 -*-
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json


class Kafka_producer():
    '''
    使用kafka的生产模块
    '''

    def __init__(self, kafkahost,kafkaport, kafkatopic):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkatopic = kafkatopic
        self.producer = KafkaProducer(bootstrap_servers = '{kafka_host}:{kafka_port}'.format(
            kafka_host=self.kafkaHost,
            kafka_port=self.kafkaPort
            ))

    def sendjsondata(self, params):
        try:
            #将参数转换为json字符串
            parmas_message = json.dumps(params)
            producer = self.producer
            #调用了生成者中send方法，发送消息
            producer.send(self.kafkatopic, parmas_message.encode('utf-8'))
            #当前面的消息处理完成以后，立即发送
            producer.flush()
        except KafkaError as e:
            print (e)
        finally:
            producer.close(60)


class Kafka_consumer():
    '''
    使用Kafka—python的消费模块
    '''

    def __init__(self, kafkahost, kafkaport, kafkatopic, groupid='test'):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkatopic = kafkatopic
        self.groupid = groupid
        #轮询查kafak里面的数据
        self.consumer = KafkaConsumer(self.kafkatopic,
                                      bootstrap_servers = '{kafka_host}:{kafka_port}'.format(
                                                            kafka_host=self.kafkaHost,
                                                            kafka_port=self.kafkaPort ),
                                      auto_offset_reset='earliest',#earliest:一次性将所有发送内容读完，smallest：
                                      #每一次只读一个字符
                                      auto_commit_interval_ms=100)
        #auto_commit_interval_ms=100，游标确认时间
        
#gggggfff
    def consume_data(self):
        try:
            for message in self.consumer:
                #反序列化，将json文件转换为pyhon字符串对象
                print(message.value)
                print(type(message.value))
                message = json.loads(message.value.decode('utf-8'))
                yield message               
        except KeyboardInterrupt as e:
            print (e)
        finally:
            self.consumer.close()

#往回丢处理完成的数据
def send_message(host,port,topic,msg):
    producer = Kafka_producer(host,port,topic)
    producer.sendjsondata(msg)
    return

def consume_message(host,port,topic,group='test'):
    consumer = Kafka_consumer(host,port,topic)
    results = consumer.consume_data()
    return results


if __name__ == '__main__':
    host = '10.43.4.57'
#     host='10.6.0.134'
    port = 9092
    topic = 'testMyfirstKafka'
    msg = {'name':'58同城','low':8,'id':1}
    send_message(host, port, topic, msg)
    ss = consume_message(host, port, topic)
    for s in ss:
        print (s)
    


# 
# def main():
#     '''
#     测试consumer和producer
#     :return:
#     '''
#     ##测试生产模块
#     producer = Kafka_producer("10.43.4.57", 9092, "kcytest")
#     for id in range(10):
#         #params = '{abetst}:{null}---'+str(id)
#         params = {'name':'太空地球人','low':'7','id':id}
#         producer.sendjsondata(params)
# 
# 
# #     ##测试消费模块
# #     #消费模块的返回格式为ConsumerRecord(topic=u'ranktest', partition=0, offset=202, timestamp=None, 
# #     #\timestamp_type=None, key=None, value='"{abetst}:{null}---0"', checksum=-1868164195, 
# #     #\serialized_key_size=-1, serialized_value_size=21)
# #     consumer = Kafka_consumer('10.43.4.57', 9092, "kcytest", 'testkcy')
# #     message = consumer.consume_data()
# # #     for i in message:
# # # #         print (i.value)
# # # #         print (type(i.value))
# # #         print ('@'*10)
