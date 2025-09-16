import json
import os
import logging
import time
from datetime import datetime
import requests
from hdfs import InsecureClient

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s [Server-%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class HDFSClient:
    def __init__(self, hdfs_url, server_id):
        self.hdfs_url = hdfs_url
        self.server_id = server_id
        self.namenode_host = 'namenode'
        self.namenode_port = 9000
        self.webui_port = 9870
        
        # Set up logger for this server
        self.logger = logging.getLogger(f"{server_id}")
        
        self.logger.info(f"🚀 Initializing HDFS Client for Server {server_id}")
        self.logger.info(f"📡 HDFS URL: {hdfs_url}")
        self.logger.info(f"🖥️  NameNode: {self.namenode_host}:{self.namenode_port}")

        # Initialize HDFS client using hdfs library instead of fsspec
        self.hdfs = self._connect_to_hdfs()

    def _connect_to_hdfs(self, max_retries=10, retry_delay=15):
        """Connect to HDFS with retries and detailed logging"""
        for attempt in range(max_retries):
            try:
                self.logger.info(f"🔄 Attempt {attempt + 1}/{max_retries}: Connecting to HDFS...")
                
                # Check if namenode web UI is accessible
                namenode_url = f'http://{self.namenode_host}:{self.webui_port}'
                self.logger.info(f"🌐 Testing NameNode Web UI: {namenode_url}")
                
                try:
                    response = requests.get(f"{namenode_url}/jmx", timeout=10)
                    if response.status_code == 200:
                        self.logger.info("✅ NameNode Web UI is accessible")
                    else:
                        self.logger.warning(f"⚠️  NameNode Web UI returned status: {response.status_code}")
                except requests.exceptions.RequestException as e:
                    self.logger.warning(f"⚠️  NameNode Web UI not accessible yet: {e}")

                # Connect using hdfs library (WebHDFS)
                client = InsecureClient(f'http://{self.namenode_host}:{self.webui_port}')
                
                # Test the connection by listing root directory
                self.logger.info("🔍 Testing HDFS connection by listing root directory...")
                try:
                    root_files = client.list('/')
                    self.logger.info(f"📂 HDFS root directory contents: {root_files}")
                except Exception as e:
                    # Root might not be accessible, try creating our directory structure
                    self.logger.info("📂 Root not accessible, will create directories as needed")

                # Test write capability
                test_dir = f'/test_server_{self.server_id}'
                self.logger.info(f"📝 Testing HDFS write capability: {test_dir}")
                
                try:
                    client.makedirs(test_dir)
                    self.logger.info(f"✅ Created test directory: {test_dir}")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        self.logger.info(f"📁 Test directory already exists: {test_dir}")
                    else:
                        self.logger.warning(f"⚠️  Could not create test directory: {e}")

                # Test file write
                test_file = f'{test_dir}/connection_test.txt'
                test_content = f"Connection test from server {self.server_id} at {datetime.now()}"
                
                try:
                    with client.write(test_file, overwrite=True) as writer:
                        writer.write(test_content.encode('utf-8'))
                    
                    # Verify we can read it back
                    with client.read(test_file) as reader:
                        read_content = reader.read().decode('utf-8')
                    
                    if test_content == read_content:
                        self.logger.info("✅ HDFS read/write test successful")
                    else:
                        self.logger.error("❌ HDFS read/write test failed - content mismatch")
                except Exception as e:
                    self.logger.warning(f"⚠️  HDFS read/write test failed: {e}")

                self.logger.info("🎉 HDFS connection established successfully!")
                self.ensure_directories(client)
                return client

            except Exception as e:
                self.logger.error(f"❌ HDFS connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    self.logger.info(f"⏳ Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error("💥 All HDFS connection attempts failed, falling back to local storage")
                    
        return None

    def ensure_directories(self, client):
        """Ensure required directories exist in HDFS"""
        directories = [
            f'/social_media/server_{self.server_id}/posts',
            f'/social_media/server_{self.server_id}/comments',
            '/social_media/global/posts',
            '/social_media/global/comments'
        ]

        for directory in directories:
            try:
                client.makedirs(directory)
                self.logger.info(f"✅ Created/verified directory: {directory}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    self.logger.info(f"📂 Directory already exists: {directory}")
                else:
                    self.logger.error(f"❌ Error creating directory {directory}: {e}")

    def store_post(self, post):
        """Store a post in HDFS with replication and detailed logging"""
        try:
            server_path = f'/social_media/server_{self.server_id}/posts/{post.id}.json'
            global_path = f'/social_media/global/posts/{post.id}.json'

            post_data = {
                'id': post.id,
                'user': post.user,
                'content': post.content,
                'timestamp': post.timestamp.isoformat(),
                'server_id': post.server_id,
                'comments': []
            }

            post_json = json.dumps(post_data, indent=2)
            
            self.logger.info(f"💾 Storing post {post.id} by user '{post.user}'")
            self.logger.info(f"📝 Post content preview: '{post.content[:50]}{'...' if len(post.content) > 50 else ''}'")

            if self.hdfs:
                # Store in server-specific directory
                self.logger.info(f"📂 Writing to server path: {server_path}")
                with self.hdfs.write(server_path, overwrite=True) as writer:
                    writer.write(post_json.encode('utf-8'))
                self.logger.info(f"✅ Stored post in server directory")

                # Store in global directory for replication
                self.logger.info(f"📂 Writing to global path: {global_path}")
                with self.hdfs.write(global_path, overwrite=True) as writer:
                    writer.write(post_json.encode('utf-8'))
                self.logger.info(f"✅ Stored post in global directory (replication)")

                # Verify the file was written correctly
                try:
                    file_status = self.hdfs.status(server_path)
                    self.logger.info(f"🔍 Verified post storage - file size: {file_status['length']} bytes")
                except Exception as e:
                    self.logger.warning(f"⚠️  Could not verify post storage: {e}")

                return True
            else:
                self.logger.warning("⚠️  HDFS not available, using local storage fallback")
                self.store_local(f'posts/{post.id}.json', post_json)
                return True

        except Exception as e:
            self.logger.error(f"💥 Error storing post {post.id}: {e}")
            return False

    def store_comment(self, comment):
        """Store a comment in HDFS with detailed logging"""
        try:
            comment_path = f'/social_media/server_{self.server_id}/comments/{comment.id}.json'
            global_comment_path = f'/social_media/global/comments/{comment.id}.json'

            comment_data = {
                'id': comment.id,
                'post_id': comment.post_id,
                'user': comment.user,
                'content': comment.content,
                'timestamp': comment.timestamp.isoformat()
            }

            comment_json = json.dumps(comment_data, indent=2)
            
            self.logger.info(f"💬 Storing comment {comment.id} by user '{comment.user}' on post {comment.post_id}")
            self.logger.info(f"📝 Comment preview: '{comment.content[:30]}{'...' if len(comment.content) > 30 else ''}'")

            if self.hdfs:
                with self.hdfs.write(comment_path, overwrite=True) as writer:
                    writer.write(comment_json.encode('utf-8'))
                self.logger.info(f"✅ Stored comment in server directory")

                with self.hdfs.write(global_comment_path, overwrite=True) as writer:
                    writer.write(comment_json.encode('utf-8'))
                self.logger.info(f"✅ Stored comment in global directory (replication)")

                return True
            else:
                self.logger.warning("⚠️  HDFS not available, using local storage fallback")
                self.store_local(f'comments/{comment.id}.json', comment_json)
                return True

        except Exception as e:
            self.logger.error(f"💥 Error storing comment {comment.id}: {e}")
            return False

    def get_all_posts(self):
        """Get all posts from this server with detailed logging"""
        try:
            posts = []
            posts_path = f'/social_media/server_{self.server_id}/posts'
            
            self.logger.info(f"🔍 Retrieving posts from: {posts_path}")

            if self.hdfs:
                try:
                    files = self.hdfs.list(posts_path)
                    self.logger.info(f"📂 Found {len(files)} files in posts directory")

                    for filename in files:
                        if filename.endswith('.json'):
                            file_path = f"{posts_path}/{filename}"
                            try:
                                self.logger.debug(f"📖 Reading post file: {file_path}")
                                with self.hdfs.read(file_path) as reader:
                                    post_data = json.loads(reader.read().decode('utf-8'))
                                    posts.append(post_data)
                            except Exception as e:
                                self.logger.error(f"❌ Error reading post file {file_path}: {e}")
                                
                    self.logger.info(f"✅ Successfully loaded {len(posts)} posts from HDFS")
                except Exception as e:
                    self.logger.warning(f"⚠️  Could not access HDFS posts directory: {e}")
                    posts = self.get_local_posts()
            else:
                self.logger.warning("⚠️  HDFS not available, using local fallback")
                posts = self.get_local_posts()

            posts.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            self.logger.info(f"📊 Returning {len(posts)} posts (sorted by timestamp)")
            return posts

        except Exception as e:
            self.logger.error(f"💥 Error getting posts: {e}")
            return []

    def get_comments(self, post_id):
        """Get comments for a specific post with detailed logging"""
        try:
            comments = []
            comments_path = f'/social_media/server_{self.server_id}/comments'
            
            self.logger.info(f"🔍 Retrieving comments for post {post_id} from: {comments_path}")

            if self.hdfs:
                try:
                    files = self.hdfs.list(comments_path)
                    
                    for filename in files:
                        if filename.endswith('.json'):
                            file_path = f"{comments_path}/{filename}"
                            try:
                                with self.hdfs.read(file_path) as reader:
                                    comment_data = json.loads(reader.read().decode('utf-8'))
                                    if comment_data.get('post_id') == post_id:
                                        comments.append(comment_data)
                            except Exception as e:
                                self.logger.error(f"❌ Error reading comment file {file_path}: {e}")
                                
                    self.logger.info(f"✅ Found {len(comments)} comments for post {post_id} in HDFS")
                except Exception as e:
                    self.logger.warning(f"⚠️  Could not access HDFS comments directory: {e}")
                    comments = self.get_local_comments(post_id)
            else:
                self.logger.warning("⚠️  HDFS not available, using local fallback")
                comments = self.get_local_comments(post_id)

            comments.sort(key=lambda x: x.get('timestamp', ''))
            return comments

        except Exception as e:
            self.logger.error(f"💥 Error getting comments for post {post_id}: {e}")
            return []

    def get_post_count(self):
        """Get the number of posts on this server with logging"""
        try:
            posts_path = f'/social_media/server_{self.server_id}/posts'

            if self.hdfs:
                try:
                    files = self.hdfs.list(posts_path)
                    count = len([f for f in files if f.endswith('.json')])
                    self.logger.info(f"📊 Post count from HDFS: {count}")
                    return count
                except Exception as e:
                    self.logger.warning(f"⚠️  Could not get HDFS post count: {e}")
                    count = len(self.get_local_posts())
                    return count
            else:
                count = len(self.get_local_posts())
                self.logger.info(f"📊 Post count from local storage: {count}")
                return count

        except Exception as e:
            self.logger.error(f"💥 Error getting post count: {e}")
            return 0

    def get_stats(self):
        """Get HDFS statistics with detailed info"""
        try:
            if self.hdfs:
                # Try to get actual HDFS cluster info
                cluster_info = {}
                try:
                    namenode_url = f'http://{self.namenode_host}:{self.webui_port}'
                    response = requests.get(f"{namenode_url}/jmx", timeout=5)
                    if response.status_code == 200:
                        jmx_data = response.json()
                        for bean in jmx_data.get('beans', []):
                            if 'FSNamesystem' in bean.get('name', ''):
                                cluster_info = {
                                    'total_files': bean.get('FilesTotal', 'N/A'),
                                    'total_blocks': bean.get('BlocksTotal', 'N/A'),
                                    'capacity_used': bean.get('CapacityUsed', 'N/A'),
                                    'capacity_remaining': bean.get('CapacityRemaining', 'N/A'),
                                }
                                break
                except Exception as e:
                    self.logger.warning(f"Could not fetch cluster stats: {e}")
                
                return {
                    'hdfs_available': True,
                    'namenode_url': f'http://{self.namenode_host}:{self.webui_port}',
                    'server_directory': f'/social_media/server_{self.server_id}/',
                    'cluster_info': cluster_info
                }
            else:
                return {
                    'hdfs_available': False,
                    'fallback_mode': 'local_storage'
                }
        except Exception as e:
            return {'error': str(e)}

    def store_local(self, path, data):
        """Fallback local storage method with logging"""
        try:
            full_path = f'/app/data/{path}'
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, 'w') as f:
                f.write(data)
            self.logger.info(f"💾 Stored data locally: {full_path}")
        except Exception as e:
            self.logger.error(f"❌ Error storing data locally: {e}")

    def get_local_posts(self):
        """Fallback method to get posts from local storage"""
        posts = []
        posts_dir = '/app/data/posts'
        if os.path.exists(posts_dir):
            for filename in os.listdir(posts_dir):
                if filename.endswith('.json'):
                    try:
                        with open(os.path.join(posts_dir, filename), 'r') as f:
                            posts.append(json.load(f))
                    except Exception as e:
                        self.logger.error(f"❌ Error reading local post {filename}: {e}")
        return posts

    def get_local_comments(self, post_id):
        """Fallback method to get comments from local storage"""
        comments = []
        comments_dir = '/app/data/comments'
        if os.path.exists(comments_dir):
            for filename in os.listdir(comments_dir):
                if filename.endswith('.json'):
                    try:
                        with open(os.path.join(comments_dir, filename), 'r') as f:
                            comment = json.load(f)
                            if comment.get('post_id') == post_id:
                                comments.append(comment)
                    except Exception as e:
                        self.logger.error(f"❌ Error reading local comment {filename}: {e}")
        return comments