from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewPartitions
from kafka.cluster import ClusterMetadata
from json import dumps
import sys
import time
from time import sleep
import os
import cv2

topic_name = 'VideoStreaming'
kafka_server = 'localhost:9092'

def publish_video(folder_path, partition):
    """
    Publish all video files in the given folder to a Kafka topic.
    Each video will be published with a unique key (video file name).
    
    :param folder_path: path to folder containing video files
    :param kafka_server: Kafka bootstrap server address
    :param topic_name: Kafka topic name
    """
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_server,
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: v  # raw bytes for frames
    )

    # Filter for video files (mp4, avi, etc.)
    video_files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.mp4', '.avi', '.mov'))]

    if not video_files:
        print("No video files found in the folder.")
        return

    for video_file in video_files:
        video_path = os.path.join(folder_path, video_file)
        video_id = os.path.splitext(video_file)[0]
        print(f"Publishing video: {video_file} with key: {video_id}")

        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            print(f"Failed to open video file: {video_path}")
            continue

        while cap.isOpened():
            success, frame = cap.read()
            if not success:
                break

            ret, buffer = cv2.imencode('.jpg', frame)
            if not ret:
                continue

            producer.send(topic_name, key=video_id, value=buffer.tobytes(), partition=partition)
            time.sleep(0.1)  # simulate slower stream

        cap.release()
        print(f"Finished publishing: {video_file}")

    producer.flush()
    producer.close()
    print("All videos published.")

if __name__ == "__main__":
    # add index from 1 to 3 from terminal
    if len(sys.argv) != 2:
        print("Usage: python Camera-Producer.py <index>")
        sys.exit(1)

    path = ['.\\camera-resources-1\\', '.\\camera-resources-2\\', '.\\camera-resources-3\\']
    kafka_client = KafkaAdminClient(bootstrap_servers=kafka_server)

    # Create partitions with 3 replicas
    topic_partition_count = kafka_client.describe_topics([topic_name])[0]['partitions']
    topic_partition_count = len(topic_partition_count)

    if topic_partition_count < 3:
        kafka_client.create_partitions({topic_name: NewPartitions(total_count=3)})

    i= int(sys.argv[1])
    if os.path.exists(path[i-1]):
        publish_video(path[i-1], i-1)
    else:
        print(f"Path {i} does not exist.")
        sys.exit(1)