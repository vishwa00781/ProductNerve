# ProductNerve
**ProductNerve** is a Data Engineering pipeline that _extracts, transforms and loads_ the Clickstream Data from the official data set made available by **Zerve. Ai.** This pipeline uses **Python, SQL, Apache Spark (PySpark), Apache Airflow, Amazon S3 (Data Lake), Docker Snowflake, Tableau.**

Tasks Performed by the pipeline:
- It loads the raw data from **Local Machine** to **AWS S3** using the Python Script. [Bronze Layer]
- It preprocess and performs **transformation on the data using PySpark and loads back to S3** [Silver Layer]
- Performs Data Modelling and creates the **Star Schema by creating fact and dimension tables by PySpark** [Gold Layer]
- Tables are loaded into **Snowflake by AWS integration** is used further Analytics
- **Orchestration** is performed by Airflow 

# Datasource
The Datasource used in the project is taken from Zerve. Ai website, where the Dataset was made public for the analysis purpose and also used as a resource in Hackearth Hackathon.

It contains:
- **409k Posthog events (ClickStream Data)** performed by the users and **107 Columns** to map the specific details.
- It contains the columns which contains: **User, Device, Session, Page, Demographic details** for the event performed


Post understanding the Data the aim is to
- Find the **granularity of the data** ( user events).
- Think about the **business questions** this Data can answer, would help in Data Modelling.
- Remove the unwanted columns which would not be used for analysis (Silver Layer).
- Perform **normalisation** (Silver Layer) .
- Create a **Star Schema** in Snowflake (Gold Layer)

# Tech Stack:
- **Programming Language:** Python, SQL
- **Data Storage:** AWS S3 (S3 Bucket)
- **Data Processing & Transformation:** Apache Spark (PySpark)
- **Workflow Orchestration:** Apache Airflow
- **Warehouse:** Snowflake
- **Architecture:** Medallion (Bronze–Silver–Gold), Star Schema

# Pipeline Architecture
![Pipeline Architecture](images/Architecture.png)

# Data Model
![Data Model](images/Data_Model.jpg)

# Setting up the Airflow Pipeline

# Improvements:
- Implementation of **error handling/ retry logic.**
- Better **Monitoring (Email Triggers)** can be implemented
- **Cluster mode** in Pyspark for large amounts of Data.
- **Visualisation** using Tableau


# Conclusion:
- This project gave us the understanding of the pipeline, if this has sparked the curiosity in you about Data Engineering, you can connect with me.
Linkedin : [Vishwajeet Rupnar](https://www.linkedin.com/in/vishwajeetrupnar)




