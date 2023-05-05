FROM python:3

WORKDIR /app
ADD requirements.txt /app

RUN pip3 install -r ./requirements.txt
ADD manager.py /app

CMD ["/app/manager.py"]
ENTRYPOINT ["python3"]