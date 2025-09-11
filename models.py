from datetime import datetime
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class Post:
    id: str
    user: str
    content: str
    timestamp: datetime
    server_id: int
    comments: List['Comment'] = None
    
    def __post_init__(self):
        if self.comments is None:
            self.comments = []
    
    def to_dict(self):
        return {
            'id': self.id,
            'user': self.user,
            'content': self.content,
            'timestamp': self.timestamp.isoformat(),
            'server_id': self.server_id,
            'comments': [comment.to_dict() for comment in self.comments]
        }

@dataclass
class Comment:
    id: str
    post_id: str
    user: str
    content: str
    timestamp: datetime
    
    def to_dict(self):
        return {
            'id': self.id,
            'post_id': self.post_id,
            'user': self.user,
            'content': self.content,
            'timestamp': self.timestamp.isoformat()
        }