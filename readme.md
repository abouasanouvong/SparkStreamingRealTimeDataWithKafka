# Spark Streaming Real-Time Data with Kafka

This project demonstrates a real-time data processing pipeline using Apache Spark Structured Streaming and Apache Kafka. It consumes news articles from a Kafka topic, performs Named Entity Recognition (NER) to extract entities, and streams the aggregated entity counts to another Kafka topic.

## Features

- **Real-time Data Ingestion**: A Python script fetches news from the [News API](https://newsapi.org) and produces it to a Kafka topic.
- **Stream Processing**: A Spark Structured Streaming application consumes the data from Kafka.
- **Named Entity Recognition (NER)**: Extracts entities (like people, organizations, locations) from the news text using spaCy.
- **Stateful Aggregation**: Maintains a running count of the occurrences of each named entity.
- **Data Output**: Publishes the aggregated counts to a separate Kafka topic.

## Prerequisites

- Java 11 or newer
- Python 3.9+
- An active Apache Kafka instance
- Two Kafka topics created (e.g., `topic1` and `topic2`)

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Create and activate a Python virtual environment:**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install the required Python packages:**
    ```bash
    pip install --upgrade pip
    pip install -r requirements.txt
    ```

4.  **Download the spaCy model:**
    For Named Entity Recognition, the application uses the `en_core_web_sm` model from spaCy. Download it by running:
    ```bash
    python -m spacy download en_core_web_sm
    ```

## How to Run

### 1. Configure the News Producer

The news producer requires an API key from the [News API](https://newsapi.org).

1.  Create a `.env` file by copying the example file:
    ```bash
    cp .env.example .env
    ```

2.  Open the `.env` file and add your News API key:
    ```
    NEWS_API_KEY=your_api_key_here
    ```

3.  Run the producer to start sending news articles to Kafka. Choose a query keyword to search for news.
    ```bash
    python app/news_producer.py --topic topic1 --query "AI"
    ```

### 2. Run the Spark Streaming Job

The Spark application reads from the input topic, processes the data, and writes to the output topic.

Execute the following `spark-submit` command in your terminal. This command is configured for a Spark 4.x environment.

**Note on Java versions:** If you are using Java 17 or newer, you may need to include `--add-opens` flags in the Spark configuration as shown below to avoid reflection errors.

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  --conf "spark.driver.extraJavaOptions=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED" \
  --conf "spark.executor.extraJavaOptions=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED" \
  app/spark_entity_counter.py \
  --bootstrap-servers localhost:9092 \
  --input-topic topic1 \
  --output-topic topic2 \
  --trigger "30 seconds"
```

#### Command-line Arguments

You can customize the behavior of the Spark job using the following arguments:

-   `--bootstrap-servers`: The address of your Kafka brokers (default: `localhost:9092`).
-   `--input-topic`: The Kafka topic to read news articles from (default: `topic1`).
-   `--output-topic`: The Kafka topic to write entity counts to (default: `topic2`).
-   `--trigger`: The processing trigger interval (default: `"30 seconds"`).

Once running, the application will continuously process incoming data. You can consume the output from `topic2` to see the real-time entity counts.