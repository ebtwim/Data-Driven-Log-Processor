Data-Driven Log Processor: Event-Driven Microservices

A robust, production-ready Data Pipeline built with an **Event-Driven Architecture**. This project demonstrates the orchestration of multiple microservices to handle, process, and persist real-time sensor data using industry-standard DevOps tools.

---

 Architecture Overview

The system is designed to be decoupled and highly scalable, consisting of five core services:

1.  FastAPI (Producer): A high-performance web API that receives sensor data and publishes messages to Kafka.
2.  Apache Kafka (Message Broker): Acts as the backbone of the system, ensuring data persistence and reliability between services.
3.  Zookeeper:Manages and coordinates the Kafka cluster.
4.  Python Worker (Consumer): An asynchronous service that consumes messages from Kafka, processes them, and handles database persistence.
5. PostgreSQL (Storage):The relational database where processed logs are stored for long-term analysis.



---
 Tech Stack

- Backend: Python 3.11, FastAPI
- Messaging:Apache Kafka, AIOKafka (Async)
- Database:PostgreSQL 15
- Infrastructure:Docker, Docker Compose
- Automation:Ansible (Deployment Orchestration)

---

 Getting Started

The entire infrastructure is containerized and automated. You can deploy the full stack using two methods:

 Method 1: Automated Deployment (Recommended)
This uses Ansible to ensure a consistent and idempotent deployment environment.
```bash
Install Ansible
pip install ansible

Run the Playbook
ansible-playbook ansible/site_deploy.yml

```

Method 2: Manual Docker Compose

```bash
docker-compose up --build -d

```

---

Testing the Pipeline

 1. Send Sample Data (API)

Once the stack is up, access the Swagger UI at `http://localhost:8000/docs` and send a `POST` request to `/send`:

```json
{
  "sensor": "SITE-Server-Room",
  "reading": 22.5,
  "status": "stable"
}

```

2. Verify Persistence (Database)

Check if the data successfully traveled through Kafka and was saved to PostgreSQL:

```bash
docker exec -it $(docker ps -qf "name=postgres") psql -U myuser -d log_db -c "SELECT * FROM sensor_logs;"

```

---

 DevOps Features Implemented

Containerization: Every service is isolated in a Docker container with optimized `slim` images.
Resiliency: Implemented `restart: always` policies and connection retry logic in the worker to handle service dependencies (Race Conditions).
Network Isolation: Services communicate via a private Docker bridge network using Service Discovery (DNS).
IaC (Infrastructure as Code): Deployment is fully described and automated, reducing manual configuration errors.

---
Developer

Role: DevOps Engineer & backend developer
Focus:Cloud Infrastructure, Automation, and CI/CD Pipelines

```

---
