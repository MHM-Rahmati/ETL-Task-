FROM python:3.8-slim
RUN apt-get update \
    && apt-get -y install apt-utils \
    && apt-get -y install python3-pymysql \
    && apt-get -y install libpq-dev gcc \
    && apt-get install python3.8-dev \
    && pip install numpy \
    && pip install PyMySQL \
    && pip install cryptography \
    && pip install sqlalchemy psycopg2
WORKDIR /app
COPY . .
CMD [ "python", "./analytics.py" ]
