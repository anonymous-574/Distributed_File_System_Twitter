import os
import json
import time
import uuid
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from hdfs_client import HDFSClient
from models import Post, Comment

class SocialMediaServer:
    def __init__(self, server_id):
        self.app = Flask(__name__)
        CORS(self.app)
        self.server_id = server_id
        self.port = int(os.getenv('SERVER_PORT', 5000 + server_id))
        
        # Initialize HDFS client
        hdfs_url = os.getenv('HDFS_URL', 'hdfs://namenode:9000')
        self.hdfs_client = HDFSClient(hdfs_url, server_id)
        
        # Data statistics for load balancing
        self.post_count = 0
        self.load_posts_count()
        
        self.setup_routes()
    
    def load_posts_count(self):
        """Load current post count from HDFS"""
        try:
            count = self.hdfs_client.get_post_count()
            self.post_count = count
        except Exception as e:
            print(f"Error loading post count: {e}")
            self.post_count = 0
    
    def setup_routes(self):
        @self.app.route('/health', methods=['GET'])
        def health():
            return jsonify({
                'server_id': self.server_id,
                'status': 'healthy',
                'post_count': self.post_count,
                'timestamp': datetime.now().isoformat()
            })
        
        @self.app.route('/stats', methods=['GET'])
        def stats():
            return jsonify({
                'server_id': self.server_id,
                'post_count': self.post_count,
                'hdfs_stats': self.hdfs_client.get_stats()
            })
        
        @self.app.route('/posts', methods=['GET'])
        def get_posts():
            """Get all posts from this server"""
            try:
                posts = self.hdfs_client.get_all_posts()
                return jsonify({
                    'server_id': self.server_id,
                    'posts': posts,
                    'total_count': len(posts)
                })
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/posts', methods=['POST'])
        def create_post():
            """Create a new post"""
            try:
                data = request.get_json()
                
                if not data or 'content' not in data or 'user' not in data:
                    return jsonify({'error': 'Missing content or user'}), 400
                
                # Create new post
                post = Post(
                    id=str(uuid.uuid4()),
                    user=data['user'],
                    content=data['content'],
                    timestamp=datetime.now(),
                    server_id=self.server_id
                )
                
                # Store in HDFS
                success = self.hdfs_client.store_post(post)
                
                if success:
                    self.post_count += 1
                    return jsonify({
                        'message': 'Post created successfully',
                        'post_id': post.id,
                        'server_id': self.server_id
                    }), 201
                else:
                    return jsonify({'error': 'Failed to store post'}), 500
                    
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/posts/<post_id>/comments', methods=['POST'])
        def add_comment(post_id):
            """Add a comment to a post"""
            try:
                data = request.get_json()
                
                if not data or 'content' not in data or 'user' not in data:
                    return jsonify({'error': 'Missing content or user'}), 400
                
                # Create new comment
                comment = Comment(
                    id=str(uuid.uuid4()),
                    post_id=post_id,
                    user=data['user'],
                    content=data['content'],
                    timestamp=datetime.now()
                )
                
                # Store comment in HDFS
                success = self.hdfs_client.store_comment(comment)
                
                if success:
                    return jsonify({
                        'message': 'Comment added successfully',
                        'comment_id': comment.id
                    }), 201
                else:
                    return jsonify({'error': 'Failed to store comment'}), 500
                    
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/posts/<post_id>/comments', methods=['GET'])
        def get_comments(post_id):
            """Get comments for a post"""
            try:
                comments = self.hdfs_client.get_comments(post_id)
                return jsonify({
                    'post_id': post_id,
                    'comments': comments
                })
            except Exception as e:
                return jsonify({'error': str(e)}), 500
    
    def run(self):
        print(f"Starting Social Media Server {self.server_id} on port {self.port}")
        self.app.run(host='0.0.0.0', port=self.port, debug=False)

if __name__ == '__main__':
    server_id = int(os.getenv('SERVER_ID', 1))
    server = SocialMediaServer(server_id)
    server.run()
