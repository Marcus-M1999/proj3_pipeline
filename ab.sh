#!/bin/bash
while true;
do
docker-compose exec mids ab -n 10 -H "Host: test_connection.game.com" http://localhost:5000/
docker-compose exec mids \
    ab -n 10 -H "Host: requester.game.com" \
      http://localhost:5000/request_group
  sleep 10;
  docker-compose exec mids \
    ab -n 10 -H "Host: acceptor.game.com" \
      http://localhost:5000/accept_member
  sleep 10;
done
