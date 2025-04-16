FROM openjdk:11-jdk-slim
WORKDIR /app
COPY target/ingestion-0.0.1-SNAPSHOT.jar /app/ingestion.jar
CMD ["java", "-jar", "ingestion.jar"]