FROM maven:3.9.9-eclipse-temurin-21 as builder
RUN apt-get -qq update && apt-get -qq install unzip
COPY . /app
WORKDIR /app
RUN mvn clean package -Passembly -Dmaven.test.skip --quiet
RUN unzip /app/debezium-server-iceberg-dist/target/debezium-server-iceberg-dist*.zip -d appdist

FROM eclipse-temurin:21-jre
COPY --from=builder /app/appdist/debezium-server-iceberg/ /app/

WORKDIR /app
EXPOSE 8080 8083
VOLUME ["/app/conf", "/app/data"] 

ENTRYPOINT ["/app/run.sh"]