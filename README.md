# Streaming Data from Reddit Using Kafka, Spark and MongoDB

## Project Description

DulexMall is a retail organisation operating across multiple states, with a strong focus on leveraging real-time data to improve customer engagement and business decision-making.

As the company expanded, it became increasingly important to monitor consumer discussions and trends occurring online, particularly on platforms like Reddit.

This project implements a real-time data streaming ETL pipeline that:

- extracts data from the Reddit API
- streams the data into Apache Kafka
- processes the data in real time using Apache Spark Structured Streaming
- stores processed records in MongoDB for analytics

## Project Goal

The goal of this project is to provide immediate access to consumer sentiment, topic trends, and product-related discussions, enabling the retail analytics team to derive insights quickly 
and respond effectively

## How to Run

To run this project, ensure you have:

- Docker Desktop installed and running
- Python 3.11+ (optional for local execution)

### Step 1 – Clone the repository

```bash
git clone https://github.com/madesina2025/real-time-reddit-streaming-pipeline.git
cd real-time-reddit-streaming-pipeline
'''

### Step 2 – Start the Docker Environment

This command will start the following services:

- Kafka
- Zookeeper
- Spark
- MongoDB

---
docker compose up -d
---

### Step 3 – Configure Reddit API Credentials

Create a `.env` file in the project root and add the following variables:

'''
REDDIT_CLIENT_ID=
REDDIT_CLIENT_SECRET=
REDDIT_USER_AGENT=
REDDIT_USERNAME=
REDDIT_PASSWORD=
REDDIT_SUBREDDITS=retail,marketing,dataengineering


### Step 4 – Start the Kafka Producer

---
python producer/load_to_consumer.py
---
### Step 5 – Start the Spark Streaming Consumer

---
spark-submit consumer/spark_streaming.py

'''text
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


## Database Schema Design

This project uses **MongoDB** as the target storage layer.

**MongoDB Collection:** `reddit_posts`

### Example Fields

- post_id
- subreddit
- title
- body
- created_utc
- author
- score
- sentiment_score
- language
- keywords

## Streaming Architecture

The real-time streaming pipeline follows the architecture below:


MongoDB stores each processed Reddit post as a JSON document, enabling:

- flexible querying
- trend analysis
- sentiment monitoring
- analytics dashboarding

---

## Steps Followed in This Project

### 1. Designed the Streaming Architecture

- Identified the business need for real-time insight
- Defined ingestion, processing, and storage components

### 2. Built the Kafka Producer

- Connected to the Reddit API
- Retrieved posts from configured subreddits
- Published messages into Kafka topics

### 3. Built the Spark Streaming Consumer

- Subscribed to Kafka topics
- Parsed and transformed incoming messages
- Added processing logic including:
  - text cleaning
  - sentiment indicators
  - metadata extraction

### 4. Loaded Processed Data into MongoDB

- Stored processed Reddit posts as JSON documents
- Enabled persistent storage for analytics and downstream consumption

### 5. Containerised the Solution

Created a Docker-based environment using `compose.yml` to automate deployment of:

- Kafka
- Zookeeper
- Spark
- MongoDB

### 6. Testing and Validation

- Verified the end-to-end data pipeline:  
  **Reddit → Kafka → Spark → MongoDB**
- Validated stored records using **MongoDB Compass**

---

## Author

**Mukaila Adesina**  
Data Engineer | BI Developer | Data Analyst

