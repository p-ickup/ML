from flask import Flask, jsonify
from datetime import datetime
import random

from flask_cors import CORS

app = Flask(__name__)
CORS(app)

SERVICE_NAME = "pickup-ml-"
SERVICE_VERSION = "0.1.0"
START_TIME = datetime.now()

# Simulate an ML model prediction function
def predict():
    return random.randint(0, 100)  # Simulate prediction with random number

@app.route('/predict', methods=['GET'])
def predict_route():
    prediction = predict()
    return jsonify({"prediction": prediction})

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for monitoring and container orchestration"""
    uptime = (datetime.now() - START_TIME).total_seconds()
    
    return jsonify({
        "status": "healthy",
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "uptime_seconds": uptime,
        "timestamp": datetime.now().isoformat()
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)  # Running on a different port
