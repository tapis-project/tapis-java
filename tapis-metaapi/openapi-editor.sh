#!/usr/bin/env bash

# docker pull swaggerapi/swagger-editor
# docker run -d -p 80:8080 swaggerapi/swagger-editor

# filepath=/Users/sterry1/development/projects/1Tapis-RH-projects/tapis-dev-projects/tapis_projects/tapis-client-java/meta-client/src/main/resources/metav3-openapi.yaml
# docker run -d -p 80:8080 -e URL=/v3/meta/metav3-openapi.yaml -v "$filepath":/usr/share/nginx/html/v3/meta swaggerapi/swagger-editor


# tapis-metaapi/src/main/resources
docker run -d -p 80:8080 -v $(pwd):/tmp -e SWAGGER_FILE=/tmp/tapis-metaapi/src/main/resources/metav3-openapi.yaml swaggerapi/swagger-editor