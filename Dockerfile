FROM python:3.14

WORKDIR /app
ADD requirements.txt /app

RUN pip3 install -r ./requirements.txt

ADD manager /app/manager

ENTRYPOINT ["python3", "-m", "manager"]