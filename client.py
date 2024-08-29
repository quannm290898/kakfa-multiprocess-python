import time
from kafka_client import *
import config as cf
import string
import random


def create_random_topics(number_topic, version):
    return ['test-topic-{}-{}'.format(version, i) for i in range(number_topic)]

class Client:
    def __init__(self, number_message_per_topic, time_send_topic):
        self.bootstrap_servers = cf.bootstrap_servers
        self.number_message_per_topic = number_message_per_topic
        self.time_send_topic = time_send_topic

    def create_random_message(self, topic):
        messages = []
        for i in range(self.number_message_per_topic):
            message = {
                "id": i,
                "topic": str(topic),
                "message": f'Message number {i}'
            }
            messages.append(message)
        return messages

    def simulate_client_send_topic(self, delete_topic=False):
        if delete_topic:
            delete_all_topic_kafka(bootstrap_servers=self.bootstrap_servers)
        else:
            print("Push message to 50 topic kafka!")
            list_topic_first = create_random_topics(50, 'first')
            create_topic_kafka(bootstrap_servers=self.bootstrap_servers, topics=list_topic_first)
            for topic in list_topic_first:
                print("Send message to topic: {}".format(topic))
                messages_topic = self.create_random_message(topic=topic)
                push_all_message_kafka(bootstrap_servers=self.bootstrap_servers, topic=topic, messages=messages_topic)
            # time.sleep(60)
            # list_topic_second = create_random_topics(20, 'second')
            # print("Push message to 10 topic kafka!")
            # create_topic_kafka(bootstrap_servers=self.bootstrap_servers, topics=list_topic_second)
            # for topic in list_topic_second:
            #     print("Send message to topic: {}".format(topic))
            #     messages_topic = self.create_random_message(topic=topic)
            #     push_all_message_kafka(bootstrap_servers=self.bootstrap_servers, topic=topic, messages=messages_topic)
            # time.sleep(30)
            # list_topic_second = create_random_topics(50, 'third')
            # print("Push message to 10 topic kafka!")
            # create_topic_kafka(bootstrap_servers=self.bootstrap_servers, topics=list_topic_second)
            # for topic in list_topic_second:
            #     print("Send message to topic: {}".format(topic))
            #     messages_topic = self.create_random_message(topic=topic)
            #     push_all_message_kafka(bootstrap_servers=self.bootstrap_servers, topic=topic, messages=messages_topic)
            # time.sleep(30)
            # print("Push message to 50 topic old kafka!")
            # for topic in list_topic_first:
            #     print("Send message to topic: {}".format(topic))
            #     messages_topic = self.create_random_message(topic=topic)
            #     push_all_message_kafka(bootstrap_servers=self.bootstrap_servers, topic=topic, messages=messages_topic)


if __name__ == '__main__':
    client = Client(number_message_per_topic=10000, time_send_topic=30)
    client.simulate_client_send_topic(delete_topic=False)