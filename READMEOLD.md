# Streaming data from Reddit using Kafka, Spark and MongoDB

## Project description

DulexMall is a retail organisation operating across multiple states, with a strong focus on leveraging real-time data to improve customer engagement and business decision-making. As the company expanded, it became increasingly important to monitor consumer discussions and trends occurring online, particularly on platforms like Reddit. :contentReference[oaicite:1]{index=1}

This project implements a **real-time data streaming ETL pipeline** that:

- extracts data from the Reddit API
- streams the data into **Apache Kafka**
- processes the data in real time using **Apache Spark Structured Streaming**
- stores processed records in **MongoDB** for analytics

The goal is to provide immediate access to consumer sentiment, topic trends, and product-related discussions, enabling the retail analytics team to derive insights quickly and respond effectively.

---

## How to run

To run this project, ensure you have:

- Docker Desktop installed and running
- Python 3.11+ (optional for local execution)

### Step 1 – Clone the repository

---
git clone https://github.com/madesina2025/data-engineering-projects-portfolio.git
cd data-engineering-projects-portfolio/real_time_retail_reddit
---

## Step 2 – Start the Docker environment

This will start:

Kafka

Zookeeper

Spark

MongoDB

bash

docker compose up -d

## Step 3 – Configure Reddit API credentials

Create a .env file in the project root:
---
REDDIT_CLIENT_ID=
REDDIT_CLIENT_SECRET=
REDDIT_USER_AGENT=
REDDIT_USERNAME=
REDDIT_PASSWORD=
REDDIT_SUBREDDITS=retail,marketing,dataengineering
---

## Step 4 – Start the Kafka Producer

---
python producer/load_to_consumer.py
---

This script connects to the Reddit API and sends messages to Kafka.

## Step 5 – Start the Spark Streaming Consumer

---

spark-submit consumer/spark_streaming.py
---

This script reads messages from Kafka, processes them, and writes formatted JSON records to MongoDB.


Project structure

From the project directory:

---
real_time_retail_reddit/
│
├── compose.yml              # Docker services (Kafka, Spark, MongoDB)
├── consumer/                # Spark Streaming Consumer
│   └── spark_streaming.py
├── producer/                # Reddit Kafka Producer
│   └── reddit_producer.py
├── requirements.txt         # Python dependencies
├── README.md                # Current file
└── venv/                    # Local virtual environment (ignored in Git)
---


Database schema design

This project uses MongoDB as the target storage layer.

MongoDB Collection: reddit_posts

Example fields:

post_id

subreddit

title

body

created_utc

author

score

sentiment_score

language

keywords


Streaming Architecture

As shown in the architecture diagram in the project documentation: 

Reddit API → Kafka Producer → Kafka Topic → Spark Structured Streaming → MongoDB


MongoDB stores each processed Reddit post as a JSON document, enabling:

flexible querying

trend analysis

sentiment monitoring

analytics dashboarding


Steps followed on this project
1. Designed the streaming architecture


Identified business need for real-time insight Project 2_Sept_DE. Main.pptx


Defined ingestion, processing, and storage components


2. Built Kafka Producer


Connected to Reddit API


Retrieved posts from configured subreddits


Published messages into Kafka topics


3. Built Spark Streaming Consumer


Subscribed to Kafka


Parsed and transformed messages


Added processing logic, including:


text cleaning


sentiment indicators


metadata extraction




4. Loaded processed data into MongoDB


Stored JSON documents in a persistent collection


Enabled downstream analytics access


5. Containerised the solution


Created Docker environment using compose.yml


Automated deployment of:


Kafka


Zookeeper


Spark


MongoDB




6. Testing and validation


Verified end-to-end flow:
Reddit → Kafka → Spark → MongoDB


Validated stored records via MongoDB Compass



Author
Mukaila Adesina
Data Engineer | BI Developer | Data Analyst




                
