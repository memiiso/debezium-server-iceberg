FROM eclipse-temurin:11-jre
RUN apt-get -qq update && apt-get install unzip
# NOTE: make sure to run `mvn clean package -Passembly -Dmaven.test.skip=true` before docker build
COPY ./debezium-server-dist/target/debezium-server-dist*.zip /tmp/
RUN unzip /tmp/debezium-server-dist*.zip -d /tmp
RUN mv /tmp/debezium-server-iceberg /app
RUN rm -rf /tmp/*

WORKDIR /app
EXPOSE 8080 8083
VOLUME ["/app/conf", "/app/data"] 

ENTRYPOINT ["/app/run.sh"]