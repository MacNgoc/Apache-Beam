import argparse
from typing import Callable
import logging
from google.cloud import bigquery
from google.cloud import pubsub_v1
import datetime
import pytz
import time

TIME_FORMAT = '%Y-%m-%d %H:%M:%S %Z'
RFC3339_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S-00:00'

def list_topics(project_id: str) -> None:
    """ List all Pub/sub topics in the given project"""

    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    project_path = f"projects/{project_id}"


    for topic in publisher.list_topics(project_path):
        print(topic)

def create_topic(project_id:str, topic_id: str) -> None:
    """ Create a new Pub/sub topic """
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    topic = publisher.create_topic(topic_path)
    print(f" Created topic :{topic.name} ")

def delete_topic(project_id:str, topic_id: str) -> None:
    """ Deletes an existing Pub/Sub topic """

    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    publisher.delete_topic(topic_path)
    print(f"Topic deleted: {topic_path}")

def publish_message_with_batch_of_event(publisher, topics, all_events, notify_time) -> None:
    """ 
        publish a batch of message
        topics: dict : {'departed' : 'mlops/departed', 'arrived': 'mlops/arrived'}
        all_events: dict of events : {'departed': ['event1', 'event2'], 'arrived': ['event3', 'event4']} 
    """
    timestamp = notify_time.strftime(RFC3339_TIME_FORMAT)

    print("timestamp notifytime", timestamp)
    for event_type in topics:
        topic_path = topics[event_type]
        topic_events = all_events[event_type]
        for event in topic_events:
            publisher.publish(topic_path, event.encode("utf-8"), EventTimeStamp = timestamp)
        
        if len(topic_events) > 0:
            print(f"Published messages to {topic_path}.")

def notify(publisher, topics, rows, programStart,simStartTime, speedFactor):
    """
    publisher:
    topics: dict {'departed': 'mlops/departed', 'arrived': 'mlopsmac/arrived'}
    rows: data from bigquery
    programStart:
    simStarTime:
    speedFactor:

    """


    def compute_time_sleep_secs(notify_time):
        time_elapsed = (datetime.datetime.utcnow() - programStart).total_seconds()
        sim_time_elapsed = (notify_time - simStartTime).total_seconds() / speedFactor
        to_sleep_secs = sim_time_elapsed - time_elapsed
        return to_sleep_secs


    data_to_notify = {}
    for key in topics:
        data_to_notify[key] = list()

    for row in rows:
        event_type, notify_time, event_data = row
        if compute_time_sleep_secs(notify_time) > 1:
            publish_message_with_batch_of_event(publisher, topics, data_to_notify, notify_time)
            for key in topics:
                data_to_notify[key] = list()

            to_sleep_secs = compute_time_sleep_secs(notify_time)
            if to_sleep_secs > 0:
                logging.info('Sleeping {} seconds'.format(to_sleep_secs))
                print('Sleeping {} seconds'.format(to_sleep_secs))
                time.sleep(to_sleep_secs)
                

        data_to_notify[event_type].append(event_data)
        
        

    publish_message_with_batch_of_event(publisher, topics, data_to_notify, notify_time)


            





if __name__ == "__main__":

    # query data from bigquery
    query = """
    SELECT
        EVENT_TYPE
        , TIMESTAMP_ADD( EVENT_TIME, INTERVAL {} SECOND) AS EVENT_TIME
        , EVENT_DATA
    FROM
        flights.simevents
    WHERE
        EVENT_TIME >= TIMESTAMP('{}')
        AND EVENT_TIME < TIMESTAMP('{}')
    ORDER BY
        EVENT_TIME ASC
    """

    jitter = 'CAST (-LN(RAND()*0.99 + 0.01)*30 + 90.5 AS INT64)'
    start_time = "2015-01-02 05:23:00 UTC"
    end_time = "2015-01-03 05:25:00 UTC"

    
    query_job = bigquery.Client().query(
        query.format(jitter, start_time, end_time),
        location='us-central1',
    )

    # create simumation
    programStartTime = datetime.datetime.utcnow()
    start_time = "2015-01-02 05:23:00 UTC"
    end_time = "2015-01-02 05:23:50 UTC"
    speedFactor = 60
    simStartTime = datetime.datetime.strptime(start_time, TIME_FORMAT).replace(tzinfo=pytz.UTC)
    notifyTime = datetime.datetime.strptime(end_time, TIME_FORMAT).replace(tzinfo=pytz.UTC)


    publisher = pubsub_v1.PublisherClient()
    
    topics_dict = {}
    for event_type in ['wheelsoff', 'arrived', 'departed']:
        
        topic_path = publisher.topic_path('mlopsmac', event_type)
        topics_dict[event_type] = topic_path
        try:
            publisher.get_topic(topic=topic_path)
            logging.info(f"topic {topic_path} already existed ")
        except:
            logging.info(f"Creating topic {topic_path}")
            publisher.create_topic(topic_path)


    
    notify(publisher, topics_dict, query_job, programStartTime, simStartTime, speedFactor)


    #publish_message('mlopsmac', 'departed')
    #list_topics('mlopsmac')
    #publish_message_with_batch_setting('mlopsmac', query_job)
    # programStartTime = datetime.datetime.utcnow()
    # start_time = "2015-01-02 05:23:00 UTC"
    # end_time = "2015-01-02 05:23:50 UTC"
    # speedFactor = 60
    # simStartTime = datetime.datetime.strptime(start_time, TIME_FORMAT).replace(tzinfo=pytz.UTC)
    # notifyTime = datetime.datetime.strptime(end_time, TIME_FORMAT).replace(tzinfo=pytz.UTC)

    # print('Simulation start time is {}'.format(simStartTime))
    # time_to_sleep = compute_sleep_secs(notifyTime, programStartTime, simStartTime, speedFactor)
    # print("time to sleep", time_to_sleep)



               
  
    

    