import json
import os
from datetime import datetime
import fsspec


class HDFSClient:
    def __init__(self, hdfs_url, server_id):
        self.hdfs_url = hdfs_url
        self.server_id = server_id
        self.namenode_host = 'namenode'
        self.namenode_port = 9000   # HDFS port for RPC
        self.webui_port = 9870      # HDFS Web UI port

        # Initialize HDFS connection
        try:
            self.hdfs = fsspec.filesystem(
                "hdfs",
                host=self.namenode_host,
                port=self.namenode_port
            )
            self.ensure_directories()
        except Exception as e:
            print(f"Warning: Could not connect to HDFS: {e}")
            self.hdfs = None

    def ensure_directories(self):
        """Ensure required directories exist in HDFS"""
        directories = [
            f'/social_media/server_{self.server_id}/posts',
            f'/social_media/server_{self.server_id}/comments',
            '/social_media/global/posts',
            '/social_media/global/comments'
        ]

        for directory in directories:
            try:
                if not self.hdfs.exists(directory):
                    self.hdfs.mkdirs(directory)
            except Exception as e:
                print(f"Error creating directory {directory}: {e}")

    def store_post(self, post):
        """Store a post in HDFS with replication"""
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

            if self.hdfs:
                with self.hdfs.open(server_path, 'w') as f:
                    f.write(post_json)

                with self.hdfs.open(global_path, 'w') as f:
                    f.write(post_json)

                return True
            else:
                self.store_local(f'posts/{post.id}.json', post_json)
                return True

        except Exception as e:
            print(f"Error storing post: {e}")
            return False

    def store_comment(self, comment):
        """Store a comment in HDFS"""
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

            if self.hdfs:
                with self.hdfs.open(comment_path, 'w') as f:
                    f.write(comment_json)

                with self.hdfs.open(global_comment_path, 'w') as f:
                    f.write(comment_json)

                return True
            else:
                self.store_local(f'comments/{comment.id}.json', comment_json)
                return True

        except Exception as e:
            print(f"Error storing comment: {e}")
            return False

    def get_all_posts(self):
        """Get all posts from this server"""
        try:
            posts = []
            posts_path = f'/social_media/server_{self.server_id}/posts'

            if self.hdfs and self.hdfs.exists(posts_path):
                files = self.hdfs.ls(posts_path, detail=False)

                for file_path in files:
                    if file_path.endswith('.json'):
                        try:
                            with self.hdfs.open(file_path, 'r') as f:
                                post_data = json.load(f)
                                posts.append(post_data)
                        except Exception as e:
                            print(f"Error reading post file {file_path}: {e}")
            else:
                posts = self.get_local_posts()

            posts.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            return posts

        except Exception as e:
            print(f"Error getting posts: {e}")
            return []

    def get_comments(self, post_id):
        """Get comments for a specific post"""
        try:
            comments = []
            comments_path = f'/social_media/server_{self.server_id}/comments'

            if self.hdfs and self.hdfs.exists(comments_path):
                files = self.hdfs.ls(comments_path, detail=False)

                for file_path in files:
                    if file_path.endswith('.json'):
                        try:
                            with self.hdfs.open(file_path, 'r') as f:
                                comment_data = json.load(f)
                                if comment_data.get('post_id') == post_id:
                                    comments.append(comment_data)
                        except Exception as e:
                            print(f"Error reading comment file {file_path}: {e}")
            else:
                comments = self.get_local_comments(post_id)

            comments.sort(key=lambda x: x.get('timestamp', ''))
            return comments

        except Exception as e:
            print(f"Error getting comments: {e}")
            return []

    def get_post_count(self):
        """Get the number of posts on this server"""
        try:
            posts_path = f'/social_media/server_{self.server_id}/posts'

            if self.hdfs and self.hdfs.exists(posts_path):
                files = self.hdfs.ls(posts_path, detail=False)
                return len([f for f in files if f.endswith('.json')])
            else:
                return len(self.get_local_posts())

        except Exception as e:
            print(f"Error getting post count: {e}")
            return 0

    def get_stats(self):
        """Get HDFS statistics"""
        try:
            if self.hdfs:
                return {
                    'hdfs_available': True,
                    'namenode_url': f'http://{self.namenode_host}:{self.webui_port}',
                    'server_directory': f'/social_media/server_{self.server_id}/'
                }
            else:
                return {
                    'hdfs_available': False,
                    'fallback_mode': 'local_storage'
                }
        except Exception as e:
            return {'error': str(e)}

    def store_local(self, path, data):
        """Fallback local storage method"""
        os.makedirs(os.path.dirname(f'/app/data/{path}'), exist_ok=True)
        with open(f'/app/data/{path}', 'w') as f:
            f.write(data)

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
                        print(f"Error reading local post {filename}: {e}")
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
                        print(f"Error reading local comment {filename}: {e}")
        return comments
