# tapis-services
Texas Advanced Computing Center APIs


## Setup

    docker-compose up
    
    mvn flyway:clean flyway:migrate\
        -Dflyway.url=jdbc:postgresql://localhost:5432/dev \
        -Dflyway.user=dev\
        -Dflyway.password=dev\
        -Dflyway.configFiles=src/main/resources/db/migration/files/flyway.config