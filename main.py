from kafka import KafkaConsumer, KafkaAdminClient
import config as cf
import multiprocessing
import time
import math
from utils import *


# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        get_log_file_handler(),
        logging.StreamHandler()
    ]
)
logging.getLogger("kafka").setLevel(logging.ERROR)

def consume_messages(topics, group_id, bootstrap_servers, process_id, topic_info):
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset='earliest'
    )

    topic_info[process_id] = topics
    logging.info(f"Process {process_id} started for topics: {topics}")
    try:
        process_message_kafka(consumer, cf.base_directory)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def distribute_topics(topics, num_processes):
    # divide number topic to process
    avg = math.ceil(len(topics) / num_processes)
    return [topics[i:i + avg] for i in range(0, len(topics), avg)]


def distribute_new_topics_among_processes(new_topics, processes, topic_info, group_id, bootstrap_servers, max_topic_per_process):
    logging.info("Max processes reached. Adding new topics to existing processes!")
    new_topics_list = list(new_topics)
    logging.info(f"{len(new_topics_list)} new topics detected: {new_topics_list}")
    process_ids = list(processes.keys())
    unassigned_topics = []
    while new_topics_list:
        all_processes_full = True  # Giả định rằng tất cả các process đã đạt giới hạn max_topic_per_process
        for pid in process_ids:
            if len(topic_info[pid]) < max_topic_per_process:
                all_processes_full = False  # Nếu tìm thấy một process chưa đạt giới hạn, thay đổi flag
                if new_topics_list:
                    topic_info[pid].append(
                        new_topics_list.pop(0))  # Lấy một topic từ new_topics_list và gán cho process
                    processes[pid].terminate()
                    processes[pid].join()
                    p = multiprocessing.Process(target=consume_messages,
                                                args=(topic_info[pid], group_id, bootstrap_servers, pid, topic_info))
                    p.start()
                    processes[pid] = p
                    # logging.info(f"Updated Process {pid} with new topics: {topic_info[pid]}")
        if all_processes_full:
            unassigned_topics.extend(new_topics_list)
            break
    if unassigned_topics:
        logging.info(f"Remaining {len(unassigned_topics)} topics were not assigned: {unassigned_topics}")
        logging.info("=======================================================")


def distribute_topics_across_processes(existing_topics, processes, group_id, bootstrap_servers, max_processes,
                                       topic_info):
    logging.info(f"Distributing topics across {max_processes} processes")
    topic_list = list(existing_topics)
    distributed_topics = distribute_topics(topic_list, max_processes)
    for i in range(len(distributed_topics)):
        p = multiprocessing.Process(target=consume_messages,
                                    args=(distributed_topics[i],
                                          group_id,
                                          bootstrap_servers,
                                          i,
                                          topic_info))
        p.start()
        processes[p.pid] = p
        topic_info[p.pid] = distributed_topics[i]
    logging.info("=======================================================")
def start_process_for_new_topics(new_topics, processes, group_id, bootstrap_servers, topic_info):
    logging.info(f"New topics detected: {new_topics}")
    p = multiprocessing.Process(target=consume_messages,
                                args=(list(new_topics),
                                      group_id,
                                      bootstrap_servers,
                                      len(processes),
                                      topic_info))
    p.start()
    processes[p.pid] = p
    topic_info[p.pid] = list(new_topics)
    logging.info("=======================================================")

def manage_processes(existing_topics, processes, group_id, bootstrap_servers, max_processes, max_topic_per_process):
    # create admin client kafka
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    # create dictionary topic information
    topic_info = {}
    # consumer from multi topics from kafka sever
    logging.info("====================================================================")
    logging.info("============ Information Kafka Multiprocess Consumer ===============")
    while True:
        try:
            # list topics exist from kafka, attention validate topics need
            topics_metadata = admin_client.list_topics()
            # not consumer message default topic
            current_topics = set(topics_metadata) - cf.topic_default
            # get new topic
            new_topics = current_topics - existing_topics
            # get delete topic
            removed_topics = existing_topics - current_topics
            if existing_topics:
                # Run first project, distribute topics to process
                if not processes:
                    distribute_topics_across_processes(existing_topics,
                                                       processes,
                                                       group_id,
                                                       bootstrap_servers,
                                                       max_processes,
                                                       topic_info)
                # Process new list topic
                if new_topics:
                    if len(processes) < max_processes:
                        start_process_for_new_topics(new_topics, processes, group_id, bootstrap_servers, topic_info)
                    else:
                        # number process using consumer topic equal max_process
                        distribute_new_topics_among_processes(new_topics, processes, topic_info, group_id, bootstrap_servers, max_topic_per_process)
                for topic in new_topics:
                    if all(topic not in topic_info[pid] for pid in processes):
                        logging.info(f"Topic {topic} is not assigned to any process.")
            # delete topic
            for pid, topics in list(topic_info.items()):
                if any(topic in removed_topics for topic in topics):
                    logging.info(f"Removing process {pid} for topics: {topics}")
                    processes[pid].terminate()
                    processes[pid].join()
                    del processes[pid]
                    del topic_info[pid]
            # update new_topics
            existing_topics.update(new_topics)
            # delete removed_topics
            existing_topics.difference_update(removed_topics)
            # logging.info information topic
            for pid, topics in topic_info.items():
                logging.info(f"Process {pid} is consuming topics: {topics}")
            time.sleep(30)
            logging.info("==================== 30 second later detect topic ==================")
            logging.info("====================================================================")
        except Exception as e:
            logging.info(f"An error occurred: {e}")



def job():
    topics_existing = set()
    processes_run = {}
    manage_processes(existing_topics=topics_existing,
                     processes=processes_run,
                     group_id=cf.group_id,
                     bootstrap_servers=cf.bootstrap_servers,
                     max_processes=cf.max_processes,
                     max_topic_per_process = cf.max_topic_per_process)

if __name__ == '__main__':
    job()