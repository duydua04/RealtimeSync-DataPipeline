
## Data Synchronization Project

### **Overview**

This project, developed from Nov 2025 to Feb 2026, provides a robust solution for near real-time data synchronization across two databases: MySQL and MongoDB. Designed for warehousing applications, it uses MongoDB for deployment. The system detects schema changes, synchronizes data in near real-time, and includes validation for data accuracy.

### **Features**





- Real-time Synchronization: Ensures data consistency across MySQL and MongoDB.



- Change Detection: Uses SQL triggers on MySQL to capture data changes (INSERT, UPDATE, DELETE).



- Data Validation: Verifies data integrity and accuracy across all databases.



- Scalable Architecture: Leverages Apache Spark for data processing and Docker for deployment.

### **Technical Stack**





- Languages: Python



- Frameworks & Tools: Apache Spark, Apache Kafka, Docker


**Databases:**





  - MySQL (Relational Database)


  - MongoDB (NoSQL Database)



### **Architecture**

The project follows a modular design for near real-time data synchronization:

Database Setup:





- MySQL, MongoDB are containerized using Docker.



- Configured with secure user credentials and predefined schemas.



- Schema validation ensures consistency across databases.

Data Ingestion:





- Apache Spark inserts data simultaneously into MySQL and MongoDB.



- Validation ensures no data is missing and all inserted data is accurate.

Change Detection:





- SQL triggers on MySQL capture INSERT, UPDATE, and DELETE operations.



- Triggered changes are sent to Kafka topics for further processing.

Streaming and Processing:





- Spark Streaming reads change events from Kafka.



- Schema is applied to extract only relevant before-change and after-change data.

Synchronization:





- Spark Streaming applies changes to MongoDB and Redis in near real-time.



- Each modification is validated for accuracy and consistency.

### **Setup Instructions**

Prerequisites





- Docker



- Python 3.8+



- Apache Spark



- Apache Kafka
