# main.py
from app import app
import topics
import agents
import logging

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.main()
