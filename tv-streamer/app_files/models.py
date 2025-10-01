from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class Stream(db.Model):
    __tablename__ = "streams"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    hls_url = db.Column(db.String(500), nullable=False)
    photo_url = db.Column(db.String(500))
    last_processed = db.Column(db.DateTime)
