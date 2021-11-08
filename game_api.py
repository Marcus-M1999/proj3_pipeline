#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())


@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"


@app.route("/request_group")
def request_friend():
    add_event = {'event_type': 'request_group'}
    log_to_kafka('MessageHistory', add_event)
    return "Request Submitted!\n"

@app.route("/accept_member")
def accept_member():
    accept_event = {'event_type': 'accept_member'}
    log_to_kafka('MessageHistory', accept_event)
    return "Member Approved!\n"

@app.route("/decline_member")
def decline_member():
    accept_event = {'event_type': 'decline_member'}
    log_to_kafka('MessageHistory', decline_event)
    return "Member Declined!\n"

@app.route("/message")
def message():
    message_event = {'event_type': 'message'}
    log_to_kafka('MessageHistory', message_event)
    return "Message sent!\n"