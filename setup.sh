#!/bin/bash

echo "🚀 Setting up Distributed Social Media System..."

# Create necessary directories
mkdir -p frontend
mkdir -p data/{posts,comments}

# Create the HTML file if it doesn't exist
if [ ! -f "frontend/index.html" ]; then
    echo "Creating frontend files..."
    # The HTML content is already included in the docker-compose.yml above
fi

set -e

echo "✅ Creating Hadoop environment file..."
cat > hadoop.env << 'EOL'
CORE_CONF_fs_defaultFS=hdfs://namenode:9000
CORE_CONF_hadoop_http_staticuser_user=root
CORE_CONF_hadoop_proxyuser_hue_hosts=*
CORE_CONF_hadoop_proxyuser_hue_groups=*
CORE_CONF_io_compression_codecs=org.apache.hadoop.io.compress.SnappyCodec

HDFS_CONF_dfs_webhdfs_enabled=true
HDFS_CONF_dfs_permissions_enabled=false
HDFS_CONF_dfs_replication=2
HDFS_CONF_dfs_blocksize=134217728
HDFS_CONF_dfs_datanode_use_datanode_hostname=false
HDFS_CONF_dfs_client_use_datanode_hostname=false
HDFS_CONF_dfs_datanode_data_dir=file:///opt/hadoop/dfs/data

YARN_CONF_yarn_log_aggregation_enable=true
YARN_CONF_yarn_resourcemanager_recovery_enabled=true
YARN_CONF_yarn_resourcemanager_store_class=org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore
YARN_CONF_yarn_resourcemanager_scheduler_class=org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler
YARN_CONF_yarn_nodemanager_resource_memory_mb=16384
YARN_CONF_yarn_nodemanager_resource_cpu_vcores=8

MAPRED_CONF_mapreduce_framework_name=yarn
MAPRED_CONF_mapred_child_java_opts=-Xmx4096m
MAPRED_CONF_mapreduce_map_memory_mb=4096
MAPRED_CONF_mapreduce_reduce_memory_mb=8192
EOL

echo "🐳 Starting Docker containers..."
docker-compose up --build -d

echo "⏳ Waiting for services to start..."
sleep 30

echo "📊 HDFS NameNode: http://localhost:9870"
echo "🌐 Frontend: http://localhost:3000"
echo "🔗 API Gateway: http://localhost:8080"
echo "🖥️  App Servers: 5001, 5002, 5003"