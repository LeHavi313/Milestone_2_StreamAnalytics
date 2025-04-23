# Milestone 2: Stream Analytics - Real-Time Ride-Hailing Data Processing

This repository contains the code and components developed for Milestone 2 of the Stream Analytics project. The project focuses on building real-time data processing pipelines using Apache Spark Streaming and Azure Event Hubs to analyze simulated ride-hailing service data.

## Project Objectives

*   Implement a real-time data ingestion pipeline using Azure Event Hubs as a Kafka-compatible message broker.
*   Develop Apache Spark Structured Streaming applications to consume and process data streams from Event Hubs.
*   Implement specific analytics use cases relevant to ride-hailing services based on the processed stream data.
*   Store the results of the stream processing to create data at rest (e.g., in Azure Blob Storage).
*   (Advanced Challenge) Develop a real-time dashboard to visualize insights derived from the streaming data.

## Technologies Used

*   Python
*   Apache Spark (Structured Streaming)
*   Azure Event Hubs (as Kafka endpoint)
*   Azure Blob Storage (for data at rest)
*   Streamlit (for real-time dashboard)
*   Jupyter Notebooks

## Repository Structure and Components [1]

*   `eventhub_producer.py`: Script responsible for sending data (likely simulated ride events) to Azure Event Hubs.
*   `ride_hailing_generator.py`: Generates simulated ride-hailing data used by the producer. (Note: Multiple versions might exist, e.g., `.cpython-3...` are compiled files).
*   `milestone_2_Stream_Processing...` (file name likely truncated): The core Spark Structured Streaming application that connects to Azure Event Hubs, processes the incoming data stream, performs analytics, and potentially writes results to storage.
*   `schemas.py`: Contains data schemas used for structuring the streaming data (e.g., for Spark DataFrames). (Note: Multiple versions might exist, e.g., `.cpython-3...` are compiled files).
*   `BlobReading + Analytics spark.ip...` / `BlobReading + Analytics.ipynb`: Jupyter notebooks likely used for reading the processed data stored at rest (e.g., from Blob Storage) and performing further batch analytics or validation.
*   `dashboard_streamlit.py`: The Streamlit application code for the real-time visualization dashboard (Advanced Challenge).
*   `Streamlit_initiator.ipynb`: A helper notebook, possibly for setting up or launching the Streamlit dashboard environment.
*   `city_grid.py` / `conmapita.py`: Helper Python modules, potentially for geographical calculations, grid mapping, or visualization support.
*   `README.md`: This file.

## Setup and Execution

1.  **Azure Event Hubs:**
    *   Ensure your assigned Azure Event Hub Namespace is accessible.
    *   Create the required Event Hubs (topics) within your namespace (up to 2 allowed). Determine the necessary partition count. *Do not change Namespace settings like TUs.*
    *   Obtain the Event Hub Namespace connection string.
2.  **Configure Spark Connection:**
    *   Update the Spark application (`milestone_2_Stream_Processing...`) with your Azure Event Hub connection details. The configuration should resemble:
        ```
        eventhub_connection_str = "YOUR_EVENT_HUB_NAMESPACE_CONNECTION_STRING"
        event_hub_namespace = "YOUR_EVENT_HUB_NAMESPACE_NAME"

        kafka_options = {
            "kafka.bootstrap.servers": f"{event_hub_namespace}.servicebus.windows.net:9093",
            "subscribe": "your_eventhub_topic_name", # Replace with your topic
            # Below settings required for Azure Event Hubs:
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{eventhub_connection_str}";',
            # Optional: starting position
            # "startingOffsets": "earliest" # or "latest"
        }
        ```
3.  **Run the Pipeline:**
    *   Execute the data producer (`eventhub_producer.py` or `ride_hailing_generator.py` if it includes producing logic) to start sending data to your Event Hub.
    *   Submit/Run the Spark Structured Streaming application (`milestone_2_Stream_Processing...`).
4.  **Visualize (Advanced Challenge):**
    *   Run the Streamlit dashboard application: `streamlit run dashboard_streamlit.py`.
5.  **Analyze Data at Rest:**
    *   Use the `BlobReading + Analytics...` notebooks to analyze the output data stored by the Spark job.

## Deliverables

The primary deliverables for this milestone are submitted via a single `.pptx` document containing:

*   **Section 1:** Real-Time Data Processing & Storage Architecture.
*   **Section 2:** Analytics & Insights description, use cases, and Spark Streaming implementation details.
*   **Section 3:** Team Reflection and Discussion Summary.

An in-class presentation and demonstration of the working real-time pipeline and dashboard (if attempting the advanced challenge) are also required.

