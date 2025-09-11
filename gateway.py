import os
import requests
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
import time
import threading
from typing import List, Dict

class LoadBalancer:
    def __init__(self):
        self.app = Flask(__name__)
        CORS(self.app)
        
        # Parse server URLs from environment
        servers_env = os.getenv('APP_SERVERS', 'app_server1:5001,app_server2:5002,app_server3:5003')
        self.servers = []
        
        for server_url in servers_env.split(','):
            host, port = server_url.strip().split(':')
            self.servers.append({
                'host': host,
                'port': int(port),
                'url': f'http://{host}:{port}',
                'healthy': True,
                'post_count': 0,
                'last_check': 0
            })
        
        # Start health check thread
        self.start_health_checks()
        self.setup_routes()
    
    def start_health_checks(self):
        """Start background thread for health checks"""
        def health_check_loop():
            while True:
                self.check_server_health()
                time.sleep(30)  # Check every 30 seconds
        
        health_thread = threading.Thread(target=health_check_loop, daemon=True)
        health_thread.start()
    
    def check_server_health(self):
        """Check health of all servers and update their statistics"""
        for server in self.servers:
            try:
                response = requests.get(f"{server['url']}/health", timeout=5)
                if response.status_code == 200:
                    health_data = response.json()
                    server['healthy'] = True
                    server['post_count'] = health_data.get('post_count', 0)
                    server['last_check'] = time.time()
                else:
                    server['healthy'] = False
            except Exception as e:
                print(f"Health check failed for {server['url']}: {e}")
                server['healthy'] = False
    
    def get_least_loaded_server(self):
        """Get server with least number of posts for load balancing"""
        healthy_servers = [s for s in self.servers if s['healthy']]
        
        if not healthy_servers:
            return None
        
        # Return server with least posts
        return min(healthy_servers, key=lambda s: s['post_count'])
    
    def get_all_healthy_servers(self):
        """Get all healthy servers"""
        return [s for s in self.servers if s['healthy']]
    
    def setup_routes(self):
        @self.app.route('/health', methods=['GET'])
        def gateway_health():
            return jsonify({
                'gateway_status': 'healthy',
                'servers': [{
                    'url': s['url'],
                    'healthy': s['healthy'],
                    'post_count': s['post_count'],
                    'last_check': s['last_check']
                } for s in self.servers]
            })
        
        @self.app.route('/api/posts', methods=['GET'])
        def get_all_posts():
            """Aggregate posts from all servers"""
            all_posts = []
            healthy_servers = self.get_all_healthy_servers()
            
            if not healthy_servers:
                return jsonify({'error': 'No healthy servers available'}), 503
            
            # Fetch posts from all servers
            for server in healthy_servers:
                try:
                    response = requests.get(f"{server['url']}/posts", timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        all_posts.extend(data.get('posts', []))
                except Exception as e:
                    print(f"Error fetching posts from {server['url']}: {e}")
            
            # Sort all posts by timestamp (newest first)
            all_posts.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            
            return jsonify({
                'posts': all_posts,
                'total_count': len(all_posts),
                'servers_queried': len(healthy_servers)
            })
        
        @self.app.route('/api/posts', methods=['POST'])
        def create_post():
            """Route post to server with least load"""
            target_server = self.get_least_loaded_server()
            
            if not target_server:
                return jsonify({'error': 'No healthy servers available'}), 503
            
            try:
                # Forward request to selected server
                response = requests.post(
                    f"{target_server['url']}/posts",
                    json=request.get_json(),
                    timeout=10
                )
                
                if response.status_code == 201:
                    # Update server's post count
                    target_server['post_count'] += 1
                
                return jsonify(response.json()), response.status_code
                
            except Exception as e:
                return jsonify({'error': f'Failed to create post: {str(e)}'}), 500
        
        @self.app.route('/api/posts/<post_id>/comments', methods=['GET'])
        def get_comments(post_id):
            """Get comments from all servers for a specific post"""
            all_comments = []
            healthy_servers = self.get_all_healthy_servers()
            
            for server in healthy_servers:
                try:
                    response = requests.get(
                        f"{server['url']}/posts/{post_id}/comments",
                        timeout=5
                    )
                    if response.status_code == 200:
                        data = response.json()
                        all_comments.extend(data.get('comments', []))
                except Exception as e:
                    print(f"Error fetching comments from {server['url']}: {e}")
            
            # Sort comments by timestamp (oldest first)
            all_comments.sort(key=lambda x: x.get('timestamp', ''))
            
            return jsonify({
                'post_id': post_id,
                'comments': all_comments
            })
        
        @self.app.route('/api/posts/<post_id>/comments', methods=['POST'])
        def add_comment(post_id):
            """Add comment - route to server with least load"""
            target_server = self.get_least_loaded_server()
            
            if not target_server:
                return jsonify({'error': 'No healthy servers available'}), 503
            
            try:
                response = requests.post(
                    f"{target_server['url']}/posts/{post_id}/comments",
                    json=request.get_json(),
                    timeout=10
                )
                
                return jsonify(response.json()), response.status_code
                
            except Exception as e:
                return jsonify({'error': f'Failed to add comment: {str(e)}'}), 500
        
        @self.app.route('/api/stats', methods=['GET'])
        def get_system_stats():
            """Get aggregated system statistics"""
            stats = {
                'total_servers': len(self.servers),
                'healthy_servers': len(self.get_all_healthy_servers()),
                'total_posts': sum(s['post_count'] for s in self.servers if s['healthy']),
                'server_details': []
            }
            
            for server in self.servers:
                try:
                    response = requests.get(f"{server['url']}/stats", timeout=5)
                    if response.status_code == 200:
                        server_stats = response.json()
                        stats['server_details'].append(server_stats)
                except Exception as e:
                    stats['server_details'].append({
                        'server_id': 'unknown',
                        'error': str(e)
                    })
            
            return jsonify(stats)
    
    def run(self):
        print("Starting Load Balancer/Gateway on port 8080")
        self.app.run(host='0.0.0.0', port=8080, debug=False)

if __name__ == '__main__':
    lb = LoadBalancer()
    lb.run()