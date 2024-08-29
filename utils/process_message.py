import os
from datetime import datetime
import config as cf

def process_message_kafka(consumer, base_directory):
    current_day = None
    file_handles = {}
    line_counts = {}

    for msg in consumer:
        topic = msg.topic
        msg_day = datetime.now().strftime('%Y-%m-%d')

        if current_day != msg_day or topic not in file_handles:
            current_day = msg_day
            for handle in file_handles.values():
                handle.close()
            file_handles.clear()
            line_counts.clear()

            directory_path = os.path.join(base_directory, topic, msg_day)
            os.makedirs(directory_path, exist_ok=True)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            file_path = os.path.join(directory_path, f"{topic}_{msg_day}_{timestamp}.txt")
            file_handles[topic] = open(file_path, 'a')
            line_counts[topic] = 0

        file_handles[topic].write(f"{msg.value.decode('utf-8')}\n")
        line_counts[topic] += 1

        if line_counts[topic] >= cf.line_max_file:
            file_handles[topic].close()
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')  # Sử dụng timestamp để đặt tên tệp mới
            file_path = os.path.join(directory_path, f"{topic}_{msg_day}_{timestamp}.txt")
            file_handles[topic] = open(file_path, 'a')
            line_counts[topic] = 0

        file_handles[topic].flush()

    for handle in file_handles.values():
        handle.close()