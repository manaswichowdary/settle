# Build inside Docker, so running the stack needs only Docker.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src src
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/settle.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
