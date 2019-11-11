#!/usr/bin/env bash

docker run --name mysql \
           --rm -e MYSQL_ALLOW_EMPTY_PASSWORD=1 \
                -e MYSQL_DATABASE=test \
                -p 3306:3306 \
                -v `pwd`/schema.sql:/docker-entrypoint-initdb.d/schema.sql mysql:5.7
