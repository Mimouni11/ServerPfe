# -*- coding: utf-8 -*-
"""
Created on Tue May 7 19:33:45 2024

@author: mimou
"""

from flask import Flask, request, jsonify

app = Flask(__name__)

# Endpoint to send notifications
@app.route('/send_notification', methods=['POST'])
def send_notification():
    data = request.json
    recipient = data.get('recipient')
    message = data.get('message')

    # Here you can implement the logic to send the notification to the recipient
    # This could involve using websockets, sending emails, or using push notification services

    return jsonify({'success': True})

if __name__ == '__main__':
    app.run(host='192.168.175.158', port=5002, debug=True)
