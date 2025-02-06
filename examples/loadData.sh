#!/usr/bin/env bash

echo "starting load"

for n in $(seq 1 1000); do
  psql postgres://postgres:postgres@localhost:5432 -c "insert into inventory.orders (order_date,purchaser,quantity,product_id) values (NOW(), 1002, 5, 107);"
done

echo "finished load"




