FROM maven:3-jdk-12
WORKDIR /api
RUN ls -la
RUN pwd
# Copy main pom file and shared libs
COPY . .

RUN mvn -pl tapis-files clean install --also-make -DskipTests

CMD ["java", "-cp", "/api/tapis-files,/api/tapis-files/lib", "edu.utexas.tacc.tapis.files.api.FilesApplication"]