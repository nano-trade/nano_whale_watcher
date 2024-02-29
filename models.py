from extensions import db
from datetime import datetime

class Transaction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sender = db.Column(db.String(120), nullable=False)  # Sender's account
    receiver = db.Column(db.String(120), nullable=True)  # Receiver's account (nullable for flexibility)
    amount = db.Column(db.Float, nullable=False)
    time = db.Column(db.DateTime, default=datetime.utcnow)
    hash = db.Column(db.String(120), nullable=False)

    def __repr__(self):
        return f'<Transaction {self.sender} to {self.receiver}, {self.amount_decimal}>'
