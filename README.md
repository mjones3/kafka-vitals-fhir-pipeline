# kafka-vitals-fhir-pipeline
An end-to-end demo pipeline that generates synthetic ECG vitals in Kafka, streams them through a Python consumer to transform each record into a FHIR Observation resource, and persists the results in MongoDB - all wired up with Docker Compose for easy local testing and experimentation.
