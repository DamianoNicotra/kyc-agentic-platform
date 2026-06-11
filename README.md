# KYC Agentic Platform

**Real-time KYC anomaly detection with Kafka, Go, and Snowflake. Immutable audit trail with SHA256 hash chain.**

[![Go Version](https://img.shields.io/badge/Go-1.22-blue.svg)](https://golang.org/)
[![Kafka](https://img.shields.io/badge/Kafka-Redpanda-23A8F0.svg)](https://redpanda.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Audit%20Logs-29B5E8.svg)](https://www.snowflake.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 🚀 Overview

KYC Agentic Platform is a real-time anomaly detection system for KYC (Know Your Customer) processes. It combines:

- **Go API** with SHA256 hash chain for tamper-evident audit logs
- **Kafka (Redpanda)** for event streaming
- **Python anomaly detector** for real-time rule evaluation
- **Snowflake** for persistent, immutable audit storage

**Detects:**
- Too many verification failures (>3 in 1 minute)
- Suspicious night operations (2:00-5:00 AM)
- Invalid document uploads

## 🏗️ Architecture
Client → Go API (KYC Ingestor) → Snowflake (Audit Logs)

↓
Kafka Topic (kyc-events)

↓
Python Anomaly Detector → Alerts / Actions


## 📦 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/kyc/event` | Submit a KYC event (verification, doc upload, approval) |
| `GET` | `/kyc/events` | Retrieve all events from in-memory cache |
| `GET` | `/health` | Health check |

### Example: Submit a KYC Event

```bash
curl -X POST http://localhost:8080/kyc/event \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user123",
    "action": "verification",
    "details": {"status": "approved"},
    "ip_address": "192.168.1.100"
  }'
