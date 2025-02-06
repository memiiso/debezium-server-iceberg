#!/usr/bin/env bash

echo "starting load"

for n in $(seq 1 1000); do
  psql postgres://postgres:postgres@localhost:5432 -c \
    "
    insert into inventory.orders (order_date,purchaser,quantity,product_id) values (NOW(), 1002, 5, 107);
    INSERT INTO conditions VALUES(NOW(), 'Prague', 22.8,  53.3);
    "
done

echo "finished load"




