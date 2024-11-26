export FLASK_APP=async_requests.py
flask db init
flask db migrate
flask db upgrade