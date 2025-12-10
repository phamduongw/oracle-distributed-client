FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /app

COPY pom.xml .

RUN mvn -q -B dependency:go-offline

COPY src ./src

RUN mvn -q -B clean package -DskipTests


FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY hosts.txt /tmp/hosts-extra
RUN cat /tmp/hosts-extra >> /etc/hosts && rm /tmp/hosts-extra

RUN addgroup -S app && adduser -S app -G app
USER app

COPY --from=build /app/target/oracle-load-generator-1.0.0-jar-with-dependencies.jar app.jar

ENV JAVA_OPTS=""

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
