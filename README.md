# Kafka Multiprocess Python

### Install Kafka on Macos
- Install Zookeeper and Kafka:
   - `brew install zookeeper`
   - `brew install kafka`
- Start Zookeeper and Kafka:
   - `brew services start zookeeper`
   - `brew services start kafka`
- Stop Zookeeper and Kafka:
   - `brew services stop zookeeper`
   - `brew services stop kafka`

### Kafka command line
- List topic: `kafka-topics --list --bootstrap-server localhost:9092`
- Read message from topic: `{path}\kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic your_topic_name --from-beginning`
- Check status group: `{path}/usr/local/Cellar/kafka/3.8.0/libexec/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group {group_name}`

### Project
``` commandline
.
├── configs    
├── .gitignore  
├── .dockerignore  
├── docker-compose.yaml  
├── Dockerfile  
├── .env            
├── utils  
│   ├─ process_message.py // Process message and save message from consumer
├── config.py 
├── kafka_client.py  // create/delete topic, push message to topic
├── client.py        // Simulate topic creation, push messages into the topic
├── main.py          // Job multiprocess consumer message automation from new topic or delete topic
├── README.md  
└── requirements.txt
```

### Run Project
- Edit file `config.py`:
```
bootstrap_servers='localhost:9092'  // adress kafka
group_id='my-consumer-group'        // name group consumer
max_processes=10                    // number process job 
max_topic_per_process=10            // max topic in a process
topic_default={'__consumer_offsets'} // topic default 
base_directory="topic"              // path save message 
log_folder="kafka-log"              // folder save log by day 
line_max_file = 1000                // max line in a file
```
- Run project using docker: `docker-compose up -- build`
- Run project using env local: `python main.py`
- Run simulate push message kafka: `python client.py`
- Check log: `{log_folder}/kakfa_process.log`

### Error
- `Connection failed to kafka`:
     - Stop Kafka: `brew services stop kafka`
     - Fix file server.properties: `vim /usr/local/Cellar/kafka/3.8.0/libexec/config/server.properties`
     - Line: `listeners=PLAINTEXT://0.0.0.0:9092`
     - Line `advertised.listeners=PLAINTEXT://localhost:9092`
     - Start Kafka: `brew services start kafka`