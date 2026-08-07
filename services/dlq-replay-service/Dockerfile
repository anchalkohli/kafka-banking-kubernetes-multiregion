FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY pom.xml .
COPY src ./src
RUN mvn -B -DskipTests clean package
FROM eclipse-temurin:17-jre
RUN useradd --system --uid 10001 --create-home appuser
WORKDIR /app
COPY --from=build /workspace/target/dlq-replay-service-*.jar app.jar
USER 10001:10001
EXPOSE 8085
ENTRYPOINT ["java","-XX:MaxRAMPercentage=75.0","-XX:+ExitOnOutOfMemoryError","-Djava.security.egd=file:/dev/urandom","-jar","/app/app.jar"]
