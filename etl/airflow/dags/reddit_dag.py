from urllib import request
from bs4 import BeautifulSoup

from airflow.providers.standard.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta, timezone
from airflow.models import Variable

from pymongo import MongoClient, UpdateOne
import psycopg
import urllib



