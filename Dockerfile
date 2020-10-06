FROM python:3.7-alpine
WORKDIR /app
ENV FLASK_APP app.py
ENV FLASK_RUN_HOST 0.0.0.0
RUN apk update && apk add gcc libc-dev make git libffi-dev openssl-dev python3-dev libxml2-dev libxslt-dev musl-dev linux-headers
COPY requirements.txt requirements.txt
#RUN pip install cryptography
RUN pip install -r requirements.txt
COPY . .
CMD ["flask","run"]