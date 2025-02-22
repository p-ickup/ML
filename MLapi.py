from flask import Flask, jsonify
import random

from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Simulate an ML model prediction function
def predict():
    return random.randint(0, 100)  # Simulate prediction with random number

@app.route('/predict', methods=['GET'])
def predict_route():
    prediction = predict()
    return jsonify({"prediction": prediction})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)  # Running on a different port
