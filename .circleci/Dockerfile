FROM circleci/openjdk:8-jdk

USER root

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git

# Install Node.js
RUN curl --silent --location https://deb.nodesource.com/setup_12.x | sudo bash - && \
    apt-get install --yes nodejs && \
    apt-get install --yes build-essential

RUN npm install -g create-openapi-repo --unsafe-perm=true --allow-root
