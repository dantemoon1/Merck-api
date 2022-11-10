#!/bin/sh

export FLASK_APP=api.py
pipenv run flask --debug run --host=localhost --port=8080