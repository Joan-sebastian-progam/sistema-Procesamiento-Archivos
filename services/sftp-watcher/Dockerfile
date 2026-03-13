FROM python:3.11-slim
WORKDIR /app
RUN pip install paramiko kafka-python
COPY watcher.py .
CMD ["python", "watcher.py"]