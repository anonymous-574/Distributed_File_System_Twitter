import os
import json
import time
import uuid
import logging
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from hdfs_client import HDFSClient
from models import Post, Comment

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s [App-Server-%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class SocialMediaServer:
    def __init__(self, server_id):
        self.app = Flask(__name__)
        CORS(self.app)
        self.server_id = server_id
        self.port = int(os.getenv('SERVER_PORT', 5000 + server_id))
        
        # Set up logger for this server
        self.logger = logging.getLogger(f"{server_id}")
        
        self.logger.info("=" * 60)
        self.logger.info(f"INITIALIZING SOCIAL MEDIA SERVER {server_id}")
        self.logger.info("=" * 60)
        self.logger.info(f"Server Port: {self.port}")
        
        # Initialize HDFS client
        hdfs_url = os.getenv('HDFS_URL', 'hdfs://namenode:9000')
        self.logger.info(f"HDFS URL: {hdfs_url}")
        
        # Initialize HDFS client (this will handle connection and logging)
        self.hdfs_client = HDFSClient(hdfs_url, server_id)
        
        # Data statistics for load balancing
        self.post_count = 0
        self.request_count = 0
        self.load_posts_count()
        
        self.setup_routes()
        self.logger.info(f"Server {server_id} initialization complete!")
    
    def load_posts_count(self):
        """Load current post count from HDFS"""
        try:
            count = self.hdfs_client.get_post_count()
            self.post_count = count
            self.logger.info(f"Current post count loaded: {count}")
        except Exception as e:
            self.logger.error(f"Error loading post count: {e}")
            self.post_count = 0
    
    def setup_routes(self):
        @self.app.before_request
        def log_request():
            self.request_count += 1
            self.logger.info("-" * 50)
            self.logger.info(f"REQUEST #{self.request_count}: {request.method} {request.path}")
            self.logger.info(f"Client IP: {request.remote_addr}")
            if request.method in ['POST', 'PUT'] and request.is_json:
                self.logger.info(f"Request Data: {json.dumps(request.get_json(), indent=2)}")
        
        @self.app.after_request
        def log_response(response):
            self.logger.info(f"RESPONSE: Status {response.status_code}")
            if response.is_json and response.status_code < 400:
                try:
                    response_data = json.loads(response.data.decode())
                    self.logger.info(f"Response Preview: {str(response_data)[:200]}...")
                except:
                    pass
            self.logger.info("-" * 50)
            return response

        @self.app.route('/health', methods=['GET'])
        def health():
            self.logger.info("Health check requested")
            health_data = {
                'server_id': self.server_id,
                'status': 'healthy',
                'post_count': self.post_count,
                'request_count': self.request_count,
                'timestamp': datetime.now().isoformat(),
                'hdfs_status': self.hdfs_client.get_stats()
            }
            self.logger.info(f"Health status: {health_data}")
            return jsonify(health_data)
        
        @self.app.route('/stats', methods=['GET'])
        def stats():
            self.logger.info("Stats requested")
            stats_data = {
                'server_id': self.server_id,
                'post_count': self.post_count,
                'request_count': self.request_count,
                'hdfs_stats': self.hdfs_client.get_stats()
            }
            return jsonify(stats_data)
        
        @self.app.route('/posts', methods=['GET'])
        def get_posts():
            """Get all posts from this server"""
            try:
                self.logger.info(f"GET POSTS request received")
                posts = self.hdfs_client.get_all_posts()
                self.logger.info(f"Retrieved {len(posts)} posts from storage")
                
                response_data = {
                    'server_id': self.server_id,
                    'posts': posts,
                    'total_count': len(posts)
                }
                
                return jsonify(response_data)
            except Exception as e:
                self.logger.error(f"Error in get_posts: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/posts', methods=['POST'])
        def create_post():
            """Create a new post"""
            try:
                data = request.get_json()
                
                self.logger.info(f"CREATE POST request received")
                self.logger.info(f"User: {data.get('user', 'Unknown')}")
                self.logger.info(f"Content: '{data.get('content', '')[:100]}{'...' if len(data.get('content', '')) > 100 else ''}'")
                
                if not data or 'content' not in data or 'user' not in data:
                    self.logger.error("Missing content or user in request")
                    return jsonify({'error': 'Missing content or user'}), 400
                
                # Create new post
                post_id = str(uuid.uuid4())
                post = Post(
                    id=post_id,
                    user=data['user'],
                    content=data['content'],
                    timestamp=datetime.now(),
                    server_id=self.server_id
                )
                
                self.logger.info(f"Generated Post ID: {post_id}")
                self.logger.info(f"Assigned to Server: {self.server_id}")
                
                # Store in HDFS
                self.logger.info(" Storing post in HDFS...")
                success = self.hdfs_client.store_post(post)
                
                if success:
                    self.post_count += 1
                    self.logger.info(f"POST CREATED SUCCESSFULLY!")
                    self.logger.info(f"Updated post count: {self.post_count}")
                    
                    response_data = {
                        'message': 'Post created successfully',
                        'post_id': post.id,
                        'server_id': self.server_id,
                        'timestamp': post.timestamp.isoformat()
                    }
                    
                    return jsonify(response_data), 201
                else:
                    self.logger.error("Failed to store post in HDFS")
                    return jsonify({'error': 'Failed to store post'}), 500
                    
            except Exception as e:
                self.logger.error(f"Error in create_post: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/posts/<post_id>/comments', methods=['POST'])
        def add_comment(post_id):
            """Add a comment to a post"""
            try:
                data = request.get_json()
                
                self.logger.info(f"ADD COMMENT request received")
                self.logger.info(f"Post ID: {post_id}")
                self.logger.info(f"User: {data.get('user', 'Unknown')}")
                self.logger.info(f"Comment: '{data.get('content', '')[:50]}{'...' if len(data.get('content', '')) > 50 else ''}'")
                
                if not data or 'content' not in data or 'user' not in data:
                    self.logger.error("Missing content or user in comment request")
                    return jsonify({'error': 'Missing content or user'}), 400
                
                # Create new comment
                comment_id = str(uuid.uuid4())
                comment = Comment(
                    id=comment_id,
                    post_id=post_id,
                    user=data['user'],
                    content=data['content'],
                    timestamp=datetime.now()
                )
                
                self.logger.info(f"Generated Comment ID: {comment_id}")
                
                # Store comment in HDFS
                self.logger.info("Storing comment in HDFS...")
                success = self.hdfs_client.store_comment(comment)
                
                if success:
                    self.logger.info(f"COMMENT ADDED SUCCESSFULLY!")
                    
                    response_data = {
                        'message': 'Comment added successfully',
                        'comment_id': comment.id,
                        'post_id': post_id,
                        'server_id': self.server_id,
                        'timestamp': comment.timestamp.isoformat()
                    }
                    
                    return jsonify(response_data), 201
                else:
                    self.logger.error("Failed to store comment in HDFS")
                    return jsonify({'error': 'Failed to store comment'}), 500
                    
            except Exception as e:
                self.logger.error(f"Error in add_comment: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/posts/<post_id>/comments', methods=['GET'])
        def get_comments(post_id):
            """Get comments for a post"""
            try:
                self.logger.info(f"GET COMMENTS request for post: {post_id}")
                comments = self.hdfs_client.get_comments(post_id)
                self.logger.info(f"Retrieved {len(comments)} comments for post {post_id}")
                
                response_data = {
                    'post_id': post_id,
                    'comments': comments,
                    'server_id': self.server_id
                }
                
                return jsonify(response_data)
            except Exception as e:
                self.logger.error(f" Error in get_comments: {e}")
                return jsonify({'error': str(e)}), 500
    
    def run(self):
        self.logger.info("=" * 60)
        self.logger.info(f" STARTING SOCIAL MEDIA SERVER {self.server_id}")
        self.logger.info(f" Listening on: 0.0.0.0:{self.port}")
        self.logger.info("=" * 60)
        
        self.app.run(host='0.0.0.0', port=self.port, debug=False)

if __name__ == '__main__':
    server_id = int(os.getenv('SERVER_ID', 1))
    server = SocialMediaServer(server_id)
    server.run()