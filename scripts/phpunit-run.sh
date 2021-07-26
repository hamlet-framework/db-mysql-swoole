#!/usr/bin/env bash

php phpunit-coroutine.php --bootstrap `pwd`/../vendor/autoload.php --verbose ../tests
