# Milestone 2: Stream Analytics - Real-Time Ride-Hailing Data Processing

**Group:** [Insert Your Group Name/Number Here]
**Repository:** LeHavi313/Milestone_2_StreamAnalytics [1]

## Project Overview

This repository contains the code developed by our group for Milestone 2 of the Stream Analytics course. The project implements a real-time data pipeline simulating a ride-hailing service. Data is generated, sent to Azure Event Hubs, processed in real-time using Apache Spark Structured Streaming, and insights are visualized on a Streamlit dashboard (Advanced Challenge). This README provides the necessary instructions to understand the project structure and execute the components.

## Core Objectives Achieved

*   Established a real-time data ingestion pipeline using Azure Event Hubs (configured as a Kafka endpoint).
*   Developed a Spark Structured Streaming application (Jupyter Notebook) to consume and process ride-hailing data streams.
*   Implemented analytics use cases relevant to ride-hailing services.
*   Persisted processed data for potential batch analysis (data at rest).
*   (Advanced Challenge) Created a real-time dashboard using Streamlit for visualization.

## Technologies Used

*   Python
*   Apache Spark (Structured Streaming via `pyspark`)
*   Azure Event Hubs (as Kafka endpoint)
*   Azure Storage Blob (Implicitly for data at rest, based on project spec)
*   Streamlit
*   Jupyter Notebooks
*   FastAvro (for data serialization)

## Repository Structure [1]

*   `README.md`: This explanatory file.
*   `eventhub_producer.py`: Python script that generates simulated ride-hailing data (using `ride_hailing_generator.py`) and sends it in Avro format to the designated Azure Event Hub topic.
*   `ride_hailing_generator.py`: Python module responsible for creating realistic simulated ride event data.
*   `schemas.py`: Contains the Avro schema definition used for the ride-hailing data.
*   `milestone_2_Stream_Processing_with_Kafka_an...ipynb`: The core Jupyter Notebook containing the Spark Structured Streaming logic. It connects to Azure Event Hubs, consumes the Avro data stream, performs transformations and analytics, and likely writes results (e.g., to memory, console, or potentially Blob Storage).
*   `city_grid.py`: A utility module, likely used for spatial operations or mapping related to the analytics.
*   `dashboard_streamlit.py`: The Python script for the Streamlit application, which visualizes the real-time analytics results.
*   `Streamlit_initiator.ipynb`: A helper notebook potentially used for setting up or initializing components needed for the Streamlit dashboard.

## Prerequisites for Execution

1.  **Azure Resources:** Access to the appropriate Azure Event Hub Namespace assigned to your group (e.g., `iesstsabbadbaa-grp-XX-XX`). The necessary Event Hub topic(s) should already be created within this namespace.
2.  **Connection Strings:** The Azure Event Hub Namespace connection string is required.
3.  **Python Environment:** Python 3.x installed.

## Execution Instructions

1.  **Clone the Repository:**
    ```
    git clone <repository_url>
    cd Milestone_2_StreamAnalytics
    ```
2.  **Install Dependencies:**
    Open a terminal or command prompt in the project directory and install the required packages:
    ```
    pip install azure azure-eventhub azure-storage-blob fastavro pyspark streamlit
    ```
    *(Note: Ensure your environment has Java installed for PySpark compatibility.)*
3.  **Configure Connections:**
    *   **Crucially, update the Azure Event Hub connection string and Event Hub topic name(s)** within both:
        *   `eventhub_producer.py`
        *   `milestone_2_Stream_Processing_with_Kafka_an...ipynb` (specifically in the Spark session configuration and readStream options).
    *   Refer to the original project specification document for the correct Spark `kafka.sasl.jaas.config` format using the connection string.
4.  **Run the Data Producer:**
    Execute the producer script to start sending simulated data to Azure Event Hubs:
    ```
    python eventhub_producer.py
    ```
    Leave this script running in its terminal.
5.  **Run the Spark Streaming Application:**
    *   Open the `milestone_2_Stream_Processing_with_Kafka_an...ipynb` Jupyter Notebook.
    *   Execute the cells sequentially. This will initialize the Spark session, connect to Event Hubs, and start the streaming query processing. Observe the output within the notebook or wherever the stream sink is configured (e.g., console).
6.  **Run the Streamlit Dashboard (Advanced Challenge):**
    *   Open a *new* terminal in the project directory.
    *   (Optional: If `Streamlit_initiator.ipynb` performs necessary setup, run its cells first).
    *   Launch the Streamlit application:
        ```
        streamlit run dashboard_streamlit.py
        ```
    *   Access the dashboard via the URL provided in the terminal output (usually `http://localhost:8501`).

## Project Deliverables

The formal submission for this milestone consists of:

1.  A single `.pptx` document detailing:
    *   Architecture and Technologies (Section 1)
    *   Analytics Use Cases and Spark Implementation (Section 2)
    *   Team Reflection and Discussion Summary (Section 3)
2.  An in-class presentation and demonstration of the working pipeline and dashboard.

This repository contains the code supporting these deliverables.
