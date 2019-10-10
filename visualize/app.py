#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 22:19:40 2019

@author: tzlearning
"""

from flask import Flask
app = Flask(__name__)

@app.route('/')
def hello():
   return 'Hello World'

if __name__ == '__main__':
   app.run()