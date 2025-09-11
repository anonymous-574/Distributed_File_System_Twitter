import requests
import json
import time
import random

def test_system():
    """Test the distributed social media system"""
    
    gateway_url = "http://localhost:8080/api"
    
    print("🧪 Testing Distributed Social Media System")
    print("=" * 50)
    
    # Test 1: Check system health
    print("\n1️⃣ Testing System Health...")
    try:
        response = requests.get(f"{gateway_url}/../health")
        if response.status_code == 200:
            health_data = response.json()
            print(f"✅ Gateway healthy")
            print(f"📊 Healthy servers: {len([s for s in health_data['servers'] if s['healthy']])}/3")
        else:
            print(f"❌ Gateway health check failed")
    except Exception as e:
        print(f"❌ Failed to connect to gateway: {e}")
        return
    
    # Test 2: Create test posts
    print("\n2️⃣ Testing Post Creation (Load Balancing)...")
    test_users = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
    test_posts = [
        "Hello from the distributed system! 🌐",
        "Testing fault tolerance and replication 🔄",
        "This is stored across multiple servers! 🖥️",
        "Hadoop HDFS ensures data reliability ⚡",
        "Load balancing in action! 🚀"
    ]
    
    created_posts = []
    for i in range(5):
        user = random.choice(test_users)
        content = test_posts[i]
        
        try:
            response = requests.post(f"{gateway_url}/posts", json={
                "user": user,
                "content": content
            })
            
            if response.status_code == 201:
                result = response.json()
                created_posts.append(result.get('post_id'))
                print(f"✅ Post created by {user} on server {result.get('server_id', 'unknown')}")
            else:
                print(f"❌ Failed to create post: {response.text}")
        except Exception as e:
            print(f"❌ Error creating post: {e}")
        
        time.sleep(1)  # Small delay between requests
    
    # Test 3: Retrieve all posts
    print("\n3️⃣ Testing Post Retrieval (Data Aggregation)...")
    try:
        response = requests.get(f"{gateway_url}/posts")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Retrieved {data['total_count']} posts from {data['servers_queried']} servers")
            
            # Display recent posts
            for post in data['posts'][:3]:
                print(f"   📝 {post['user']}: {post['content'][:50]}...")
        else:
            print(f"❌ Failed to retrieve posts: {response.text}")
    except Exception as e:
        print(f"❌ Error retrieving posts: {e}")
    
    # Test 4: Add comments
    print("\n4️⃣ Testing Comments (Cross-server functionality)...")
    if created_posts:
        test_post_id = created_posts[0] if created_posts[0] else "test_post"
        
        comments = [
            {"user": "Commenter1", "content": "Great post! 👍"},
            {"user": "Commenter2", "content": "Very interesting system design!"},
            {"user": "Commenter3", "content": "Love the distributed architecture 🏗️"}
        ]
        
        for comment in comments:
            try:
                response = requests.post(f"{gateway_url}/posts/{test_post_id}/comments", json=comment)
                if response.status_code == 201:
                    print(f"✅ Comment added by {comment['user']}")
                else:
                    print(f"❌ Failed to add comment: {response.text}")
            except Exception as e:
                print(f"❌ Error adding comment: {e}")
            
            time.sleep(0.5)
        
        # Retrieve comments
        try:
            response = requests.get(f"{gateway_url}/posts/{test_post_id}/comments")
            if response.status_code == 200:
                comments_data = response.json()
                print(f"✅ Retrieved {len(comments_data['comments'])} comments")
            else:
                print(f"❌ Failed to retrieve comments: {response.text}")
        except Exception as e:
            print(f"❌ Error retrieving comments: {e}")
    
    # Test 5: System statistics
    print("\n5️⃣ Testing System Statistics...")
    try:
        response = requests.get(f"{gateway_url}/stats")
        if response.status_code == 200:
            stats = response.json()
            print(f"✅ System Stats:")
            print(f"   🖥️  Total Servers: {stats['total_servers']}")
            print(f"   💚 Healthy Servers: {stats['healthy_servers']}")
            print(f"   📝 Total Posts: {stats['total_posts']}")
            print(f"   📊 Server Details: {len(stats['server_details'])} servers reporting")
        else:
            print(f"❌ Failed to get system stats: {response.text}")
    except Exception as e:
        print(f"❌ Error getting system stats: {e}")
    
    print("\n" + "=" * 50)
    print("🎉 System testing completed!")
    print("🌐 Access the frontend at: http://localhost:3000")
    print("📊 View HDFS status at: http://localhost:9870")

if __name__ == "__main__":
    test_system()