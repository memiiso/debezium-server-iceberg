FROM maven:3.9.9-eclipse-temurin-21 as builder
ARG RELEASE_VERSION
RUN apt-get -qq update && apt-get -qq install unzip
COPY . /app
WORKDIR /app
RUN mvn clean package -Passembly -Dmaven.test.skip --quiet -Drevision=${RELEASE_VERSION}
RUN unzip /app/debezium-server-iceberg-dist/target/debezium-server-iceberg-dist*.zip -d appdist

# Stage 2: Final image
FROM registry.access.redhat.com/ubi8/openjdk-21

ENV SERVER_HOME=/debezium

USER root
RUN microdnf clean all

USER jboss

COPY --from=builder /app/appdist/debezium-server-iceberg $SERVER_HOME

# Set the working directory to the Debezium Server home directory
WORKDIR $SERVER_HOME

#
# Expose the ports and set up volumes for the data, transaction log, and configuration
#
EXPOSE 8080
VOLUME ["/debezium/config","/debezium/data"]

CMD ["/debezium/run.sh"]