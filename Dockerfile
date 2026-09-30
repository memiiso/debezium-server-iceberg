FROM maven:3.9.9-eclipse-temurin-21 AS builder
ARG RELEASE_VERSION
RUN apt-get -qq update && apt-get -qq install unzip
COPY . /app
WORKDIR /app
RUN mvn clean package -Passembly -Dmaven.test.skip --quiet -Drevision=${RELEASE_VERSION}
RUN unzip /app/debezium-server-iceberg-dist/target/debezium-server-iceberg-dist*.zip -d appdist
RUN mkdir -p /app/appdist/debezium-server-iceberg/data && \
    chown -R 185 /app/appdist/debezium-server-iceberg && \
    chmod -R g+w,o+w /app/appdist/debezium-server-iceberg/data

# Stage 2: Final image
FROM registry.access.redhat.com/ubi9/openjdk-21-runtime:latest

ENV SERVER_HOME=/debezium

USER 185

COPY --from=builder --chown=185 /app/appdist/debezium-server-iceberg $SERVER_HOME

# Set the working directory to the Debezium Server home directory
WORKDIR $SERVER_HOME

#
# Expose the ports and set up volumes for the data, transaction log, and configuration
#
EXPOSE 8080 9000
VOLUME ["/debezium/config","/debezium/data"]

CMD ["/debezium/run.sh"]