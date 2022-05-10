#!/usr/bin/bash

# Run this from the lambda directory
pip3 install -t packages -r requirements.txt
rm deployment.zip; cd packages; zip -r ../deployment.zip .; cd ..; zip -g deployment.zip lambda_function.py