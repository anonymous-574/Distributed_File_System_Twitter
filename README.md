# Distributed Social Media Platform with Hadoop HDFS

A comprehensive implementation of a distributed social media platform demonstrating the practical application of distributed file systems using Hadoop HDFS, Docker containerization, and microservices architecture.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)
- [System Components](#system-components)
- [License](#license)

## Overview

This project demonstrates how distributed file systems can address the complex requirements of modern social media platforms. The implementation showcases key distributed systems concepts including:

- **Data Replication** across multiple nodes for fault tolerance
- **Load Balancing** to distribute requests across server instances
- **Fault Tolerance** through automatic failover and recovery mechanisms
- **Scalability** via horizontal scaling of application servers
- **Consistency** through coordinated data management across the cluster

The system handles user posts, comments, and multimedia content while ensuring high availability and data integrity through Hadoop HDFS's distributed storage capabilities.

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   Frontend      │    │   API Gateway    │    │   App Servers       │
│   (React/HTML)  │───▶│  Load Balancer   │───▶│  Server 1,2,3      │
│   Port: 3000    │    │   Port: 8080     │    │  Ports: 5001-5003   │
└─────────────────┘    └──────────────────┘    └─────────────────────┘
                                                          │
                                                          ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                        Hadoop HDFS Cluster                                                   │
│                                                                                              │
│                                   ┌─────────────┐                                            │
│                                   │  NameNode   │                                            │
│                                   │ Port: 9870  │                                            │
│                                   │ (Metadata)  │                                            │
│                                   └─────────────┘                                            │
│                                                                                              │
│   ┌──────────────────────────┐   ┌──────────────────────────┐   ┌──────────────────────────┐ │
│   │   Hot Storage Tier       │   │   Warm Storage Tier      │   │   Cold Storage Tier      │ │
│   │   DataNode1              │   │   DataNode2              │   │   DataNode3              │ │
│   │   Port: 9864             │   │   Port: 9865             │   │   Port: 9866             │ │
│   │   High Replication       │   │   Standard Replication   │   │   Minimal Replication    │ │
│   │   Low Latency            │   │   Balanced Performance   │   │   Cost Efficient         │ │
│   └──────────────────────────┘   └──────────────────────────┘   └──────────────────────────┘ │
│                                                                                              │
└──────────────────────────────────────────────────────────────────────────────────────────────┘

```

## Features

### Core Functionality
- **User Posts**: Create, read, and manage user-generated content
- **Comments System**: Threaded comments with full CRUD operations
- **Real-time Updates**: Dynamic content loading and updates
- **Load Distribution**: Intelligent request routing across multiple servers

### Distributed Systems Features
- **Automatic Replication**: Data stored across multiple HDFS DataNodes
- **Fault Tolerance**: Automatic failover and recovery from node failures
- **Horizontal Scaling**: Easy addition of new application servers
- **Geographic Distribution**: Support for multi-region deployments
- **Health Monitoring**: Real-time system health checks and metrics

### Technical Features
- **RESTful API**: Clean API design with comprehensive endpoints
- **Containerized Deployment**: Complete Docker-based infrastructure
- **Persistent Storage**: HDFS-backed persistent data storage
- **Logging & Monitoring**: Comprehensive system observability
- **Security**: Basic authentication and authorization mechanisms

## Prerequisites

- **Docker Desktop** (v4.0 or higher)
- **Docker Compose** (v2.0 or higher)
- **8GB RAM** minimum (16GB recommended)
- **20GB** available disk space
- **Linux/macOS/Windows** with WSL2

## Installation & Setup

### Quick Start

1. **Clone the Repository**
   ```bash
   git clone https://github.com/your-username/distributed-social-media.git
   cd distributed-social-media
   ```

2. **Start the System**
   ```bash
   ./setup.sh
   ```

### Accessing the Application

Once deployed, access the system through:

- **Frontend Application**: http://localhost:3000
- **API Gateway**: http://localhost:8080
- **HDFS NameNode UI**: http://localhost:9870
- **Individual App Servers**: 
  - Server 1: http://localhost:5001
  - Server 2: http://localhost:5002
  - Server 3: http://localhost:5003

## Usage

### Creating Posts

1. Navigate to http://localhost:3000
2. Enter your username and post content
3. Click "Post" to submit
4. The system automatically distributes the request to the least loaded server

### Adding Comments

1. View existing posts on the main page
2. Scroll to any post and use the comment form
3. Enter your username and comment text
4. Comments are stored with full replication across HDFS nodes

## System Components

### Frontend (Port 3000)
- **Technology**: HTML5, CSS3, JavaScript, Nginx
- **Responsibilities**: User interface, API communication, real-time updates
- **Features**: Responsive design, error handling, loading states

### API Gateway (Port 8080)
- **Technology**: Python Flask, Request routing
- **Responsibilities**: Load balancing, request aggregation, health monitoring
- **Features**: Round-robin distribution, automatic failover, health checks

### Application Servers (Ports 5001-5003)
- **Technology**: Python Flask, HDFS integration
- **Responsibilities**: Business logic, data processing, HDFS communication
- **Features**: Individual scaling, fault isolation, comprehensive logging

### Hadoop HDFS Cluster
- **NameNode (Port 9870)**: Metadata management, namespace operations
- **DataNode 1-3 (Ports 9864-9866)**: Data storage, replication, block management
- **Features**: Automatic replication, failure detection, data integrity


## Project Structure

```
distributed-social-media/
├── docker-compose.yml          # Main orchestration file
├── setup.sh                    # Automated setup script
├── monitor.sh                  # System monitoring script
├── troubleshoot.sh             # Troubleshooting utilities
├── requirements.txt            # Python dependencies
├── test_sys.py                 # System test suite
├── frontend/
│   ├── index.html              # Main web interface
│   └── nginx.conf              # Web server configuration
├── app_server.py               # Flask application server
├── gateway.py                  # API gateway and load balancer
├── hdfs_client.py              # HDFS integration client
├── models.py                   # Data models
├── hadoop-config/              # Hadoop configuration files
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── mapred-site.xml
│   └── yarn-site.xml
├── Dockerfile.app              # Application server container
├── Dockerfile.gateway          # Gateway container
├── Dockerfile.frontend         # Frontend container
└── README.md                   # This file
```


### Scalability
- **Horizontal**: Linear scaling with additional app servers
- **Storage**: HDFS scales to petabyte capacity
- **Geographic**: Support for multi-region deployments



## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

