# Kafka Vitals → FHIR Demo Pipeline

An end-to-end, Docker Compose–driven demo that:

1. **Generates** synthetic ECG vitals (`random-vitals-producer`)  
2. **Streams** them into Apache Kafka  
3. **Consumes** each record in Python (`fhir-consumer`), transforming it into a FHIR `Observation`  
4. **Persists** the resulting HL7-FHIR JSON documents into MongoDB  

---

## Relevance to Healthcare Interoperability

Modern healthcare systems must seamlessly exchange patient data across EMRs, analytics platforms, and population-health tools. This demo illustrates a microservices pipeline where:

- **Kafka** handles high-throughput ingestion of real-time vitals  
- **FHIR** provides a standardized data model for observations  
- **MongoDB** stores those observations as JSON documents  

In a real organization, such a pipeline could power:

- Live monitoring dashboards  
- Automated alerting (e.g. drops in SpO₂)  
- Population-health analytics  
- Cross-system data exchange via FHIR APIs  

---

## Prerequisites

- Docker & Docker Compose (v2+)  
- Git  
- (Optional) MongoDB Compass or mongo-express for UI  

---

## Setup & Deployment

1. **Clone the repo**  
   ```bash
   git clone https://github.com/mjones3/kafka-vitals-fhir-pipeline.git
   cd kafka-vitals-to-fhir

2. **Configure environment variables**
    Create a `.env` file in the project root (or export these in your shell):
    ```dot.env
    KAFKA_BROKER=kafka:9093
    KAFKA_TOPIC=vitals-json
    PATIENT_ID=p010124
    INTERVAL_MS=10
    MONGO_URI=mongodb://admin:s3cr3tPassw0rd@mongo:27017/admin
    MONGO_DB=ehr
    MONGO_COLLECTION=observations

3. **Start everything**
    ```bash
    docker-compose up -d

4. **Verify services**
    Kafka Manager: http://localhost:9000
    Mongo-Express UI: http://localhost:8081
    Producer & Consumer logs:
    ```bash
    docker compose logs -f random-vitals-producer fhir-consumer
