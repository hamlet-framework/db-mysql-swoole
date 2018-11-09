#!/usr/bin/env bash

docker run --name mysql \
           --rm -e MYSQL_ROOT_PASSWORD=123456 \
                -e MYSQL_DATABASE=test \
                -p 3306:3306 \
                -v `pwd`/schema.sql:/docker-entrypoint-initdb.d/schema.sql mysql:5.7
