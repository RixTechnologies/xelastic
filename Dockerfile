FROM python:3.10-slim

RUN apt-get update && apt-get install -y git

# ENV GITHUB_TOKEN=''

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1
# Turns off buffering for easier container logging
#ENV PYTHONUNBUFFERED=1

# Install pip requirements
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

COPY ./ /

WORKDIR /tests

# Creates a non-root user with an explicit UID and adds permission to access
# For more info, please refer to https://aka.ms/vscode-docker-python-configure-containers
# RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /src \
#     chown -R appuser /howto
# USER appuser

RUN git init && git remote add xelastic 'https://github.com/RixTechnologies/xelastic'

# Keep container up
ENTRYPOINT ["tail", "-f", "/dev/null"]
