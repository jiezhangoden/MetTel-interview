FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM flink:1.20.1-java17
COPY --from=build /src/target/network-incidents-pass-through.jar /opt/flink/usrlib/network-incidents.jar
COPY submit.sh /opt/flink/submit.sh
RUN chmod +x /opt/flink/submit.sh
ENTRYPOINT ["/opt/flink/submit.sh"]
