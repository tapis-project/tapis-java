FROM maven:3-jdk-12
WORKDIR /app
RUN ls -la
RUN pwd
# Copy main pom file and shared libs
COPY . .

RUN mvn -pl tapis-files clean install --also-make -DskipTests

CMD ["java", "-cp", "/app/tapis-files,/app/tapis-files/lib", "edu.utexas.tacc.tapis.files.api.FilesApplication"]