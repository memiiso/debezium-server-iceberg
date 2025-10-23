FROM maven:3.9.9-eclipse-temurin-21 as builder
ARG RELEASE_VERSION
RUN apt-get -qq update && apt-get -qq install unzip
COPY . /app
WORKDIR /app
RUN mvn clean package -Passembly -Dmaven.test.skip --quiet -Drevision=${RELEASE_VERSION}
RUN unzip /app/debezium-server-iceberg-dist/target/debezium-server-iceberg-dist*.zip -d appdist

FROM eclipse-temurin:21-jre
COPY --from=builder /app/appdist/debezium-server-iceberg/ /debezium/

WORKDIR /debezium
EXPOSE 8080 8083
VOLUME ["/debezium/config", "/debezium/data"]

ENTRYPOINT ["/debezium/run.sh"]