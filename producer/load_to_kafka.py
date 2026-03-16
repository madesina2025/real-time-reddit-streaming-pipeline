from dotenv import load_dotenv
from extract import extract_data
from kafka import KafkaProducer
import json
import time

load_dotenv()


def stream_data():
  producer = KafkaProducer(
    # keep host listener (broker must advertise localhost:9094)
    bootstrap_servers=["localhost:9094"],         # ← list is safer
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",                                   # ← ensure delivery
    linger_ms=50,                                 # ← small batching
    retries=3                                     # ← retry on transient errors
  )
  return producer


topic = "retail_data"


def main():
  # call the api data
  data = extract_data()

  # ensure we have a list of dicts
  if not isinstance(data, list):
    raise TypeError(f"extract_data() must return a list, got {type(data).__name__}")

  if not data:
    print("No records to send.")
    return

  # extend into a list (your structure kept)
  retail_data = []
  retail_data.extend(data)

  # call kafka server connection
  producer = stream_data()

  for item in retail_data:   # ← use a different name to avoid shadowing
    # use .get() to avoid KeyError when a field is missing in some rows
    result = {
      
      "disaster_number": item.get('disasterNumber'),
      "declaration_date:": item.get('declarationdate'),
      "incident_type": item.get('incidentType'),
      "application_title": item.get('applicationTitle'),
      "applicant_id": item.get('applicantId'),
      "damage_categorycode": item.get('damageCategoryCode'),
      "damage_categorydescription": item.get('damageCategoryDescrip'),
      "project_status": item.get('projectStatus'),
      "project_processStep": item.get('projectProcessStep'),
      "project_size": item.get('projectSize'),
      "county": item.get('county'),
      "first_obligation_date": item.get('firstObligationDate'),
      "mitigation_amount": item.get('mitigationAmount'),
      "gmProject_id": item.get('gmProjectId'),
      "gmApplicantId": item.get('gmApplicantId'),
      "last_refresh": item.get('lastRefresh'),
      "hash": item.get('hash')                
    }

    # block for errors so failures are visible (instead of silently buffering)
    producer.send(topic, result).get(timeout=10)
    print("data sent to topic")
    # time.sleep(0.05)  # optional pacing

  producer.flush()
  producer.close()


if __name__ == "__main__":
  main()

# def stream_data():
#   producer = KafkaProducer(
#     boostrap_servers="localhost:9094",
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
#   )

# data = []

# def main():
#   # call the api data
#   data = extract_data()
#   # extends into a list
#   retail_data = []
#   # call kafka servers connection
#   retail_data.extend([[data]])
#   # call kafka server connections
#   producer = stream_data()

#   for data in retail_data:
#     result = {
#       "disaster_number": data['disasterNumber'],
#       "declaration_date:": data['declarationdate'],
#       "incident_type": data['incidentType'],
#       "application_title": data['applicationTitle'],
#       "applicant_id": data['applicantId'],
#       "damage_categorycode": data['damageCategoryCode'],
#       "damage_categorydescription": data['damageCategoryDescrip'],
#       "project_status": data['projectStatus'],
#       "project_processStep": data['projectProcessStep'],
#       "project_size": data['projectSize'],
#       "county": data['county'],
#       "first_obligation_date": data['firstObligationDate'],
#       "mitigation_amount": data['mitigationAmount'],
#       "gmProject_id": data['gmProjectId'],
#       "gmApplicantId": data['gmApplicantId'],
#       "last_refresh": data['lastRefresh'],
#       "hash": data['hash']                

#     }
    
#     producer.send("retail_data", result)
#     print('data sent to topic')
#     time.sleep(2)

#   producer.flush()
#   producer.close()
