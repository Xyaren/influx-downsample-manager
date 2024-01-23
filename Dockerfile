FROM python:3

WORKDIR /app
ADD requirements.txt /app

RUN pip3 install -r ./requirements.txt

ADD manager /app/manager
ADD main.py /app

CMD ["/app/main.py"]
ENTRYPOINT ["python3"]