FROM eclipse-temurin:11-jdk as builder

# Downloading and installing Maven
ARG MAVEN_VERSION=3.8.3
ARG USER_HOME_DIR="/root"
ARG SHA=1c12a5df43421795054874fd54bb8b37d242949133b5bf6052a063a13a93f13a20e6e9dae2b3d85b9c7034ec977bbc2b6e7f66832182b9c863711d78bfe60faa
ARG BASE_URL=https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries

RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
  && echo "Downlaoding maven" \
  && curl -fsSL -o /tmp/apache-maven.tar.gz ${BASE_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
  \
  && echo "Checking download hash" \
  && echo "${SHA}  /tmp/apache-maven.tar.gz" | sha512sum -c - \
  \
  && echo "Unziping maven" \
  && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
  \
  && echo "Cleaning and setting links" \
  && rm -f /tmp/apache-maven.tar.gz \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG "$USER_HOME_DIR/.m2"

COPY . /app
WORKDIR /app
RUN mvn clean package -Passembly -Dmaven.test.skip

FROM eclipse-temurin:11-jre

COPY --from=builder /app/debezium-server-dist/target /app/target
RUN cp /app/target/debezium-server-dist-*-runner.jar /app/target/debezium-server-dist-runner.jar
ENTRYPOINT ["java", "-cp", "/app/target/debezium-server-dist-runner.jar:/opt/conf:/app/target/lib/*", "io.debezium.server.Main"]