"""
SqlAlchemy model for RedditSubmission along with some database helper functions.
To populate the database see pushshift_to_sqlite.py.
"""

import os
import configparser

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Text, DateTime
from sqlalchemy import create_engine
from sqlalchemy.schema import Index
from sqlalchemy.orm import sessionmaker

base = declarative_base()

class RedditSubmission(base):
    __tablename__ = "reddit_submission"
    id = Column(Integer, primary_key=True) # Converted from Base36
    url = Column(Text)
    score = Column(Integer)
    title = Column(Text)
    subreddit = Column(Text)
    created_utc = Column(DateTime, index=True)

# Index('idx_url_created', RedditSubmission.url, RedditSubmission.created_utc)
    
def get_db_session():
    config = configparser.ConfigParser()
    config.read('alembic.ini')
    db_url = config["alembic"]["sqlalchemy.url"]    

    fresh_db = False
    db_file_path = db_url.replace("sqlite:///","")
    if not os.path.exists(db_file_path):
        fresh_db = True

    engine = create_engine(db_url)
    if fresh_db:
        base.metadata.create_all(engine)
 
    Session = sessionmaker(bind=engine)
    db_session = Session()
    return db_session

def recreate_db():
    config = configparser.ConfigParser()
    config.read('alembic.ini')
    db_url = config["alembic"]["sqlalchemy.url"]

    db_file_path = db_url.replace("sqlite:///","")    
    if os.path.exists(db_file_path):
        os.remove(db_file_path) # sqlite doesn't truncate on drop_all

    engine = create_engine(db_url)
    base.metadata.create_all(engine)

if __name__ == '__main__':
    recreate_db()