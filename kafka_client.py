from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import json
import config as cf


def create_topic_kafka(bootstrap_servers:str, topics:list):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topics_exist = admin_client.list_topics()
        topic_list = []
        for topic in topics:
            if topic not in topics_exist:
                new_topic = NewTopic(name=topic, num_partitions=1, replication_factor=1)
                topic_list.append(new_topic)
            else:
                print("Topic {} already exists!".format(topic))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        admin_client.close()
        print("Topic has been successfully created!")
    except Exception as error:
        print("Exception : {}".format(error))

def delete_topic_kafka(bootstrap_servers:str, topics:list):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topics_exist = admin_client.list_topics()
        topic_list = []
        for topic in topics:
            if topic in topics_exist:
                topic_list.append(topic)
            else:
                print("Topic {} not exists!".format(topic))
        admin_client.delete_topics(topics=topic_list)
        admin_client.close()
        print("Topic {} has been successfully deleted".format(topic_list))
    except Exception as error:
        print("Exception : {}".format(error))

def delete_all_topic_kafka(bootstrap_servers:str):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topics_exist = admin_client.list_topics()
        topic_list = list(set(topics_exist) - cf.topic_default)
        admin_client.delete_topics(topics=topic_list)
        admin_client.close()
        print("All topic has been successfully deleted: {}".format(topic_list))
    except Exception as error:
        print("Exception : {}".format(error))

def push_all_message_kafka(bootstrap_servers:str, topic:str, messages:list):
    try:
        producer_kafka = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                       value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        for message in messages:
            producer_kafka.send(topic, value=message)
        producer_kafka.flush()
        producer_kafka.close()
        print("Message list has been sent topic {} successfully!".format(topic))
    except Exception as error:
        print("Exception : {}".format(error))



if __name__ == '__main__':
    # create_topic_kafka(bootstrap_servers=cf.bootstrap_servers, topics=['test-1', 'test-2'])
    # delete_topic_kafka(bootstrap_servers=cf.bootstrap_servers, topics=['test-1', 'test-2'])
    # messages_ = [
    #     {"id": 1, "name": "A", "message": "Message 1"},
    #     {"id": 2, "name": "B", "message": "Message 2"},
    #     {"id": 3, "name": "C", "message": "Message 3"},
    #     {"id": 4, "name": "D", "message": "Message 4"},
    #     {"id": 5, "name": "E", "message": "Message 5"}
    # ]
    # push_all_message_kafka(bootstrap_servers=cf.bootstrap_servers, topic='test-1', messages=messages_)
    pass

