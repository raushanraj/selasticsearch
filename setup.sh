apt-get install redis-server
pip install -r requirements.txt
service redis-server start
python -u server.py &
