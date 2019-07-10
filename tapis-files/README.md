### Migrations

Install flyway command line tool from here:
    https://flywaydb.org/download/
    
Inside the tapis-files directory run:

    flyway -user=dev -password=dev -url=jdbc:postgresql://localhost:5432/dev  -schemas=files -locations=filesystem:src/main/resources/db/migration clean migrate
    
    docker-compose -f docker-compose.yml -f ../docker-compose.yml up
    


Now you should be able to hit http://localhost:8080/files/systems/ (with a JWT in 
the `x-jwt` header field)
