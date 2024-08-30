bootstrap_servers='localhost:9092'
group_id='my-consumer-group'
max_processes=10
max_topic_per_process=10
topic_default={'__consumer_offsets'}
base_directory = "topic"
log_folder = "kafka_log"
line_max_file = 1000

