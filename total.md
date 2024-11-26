
env\Scripts\activate
pip install -r requirements.txt
docker-compose up -d
./migrate.sh
python3 async_requests.py