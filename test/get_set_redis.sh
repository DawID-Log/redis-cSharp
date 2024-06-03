redis-cli SET var "valore salvato"
redis-cli GET var

redis-cli SET foo "valore salvato" px 2000
sleep 1 && redis-cli GET foo

valueToShow=10
redis-cli SET bar "valore numerico $valueToShow" px 1000
sleep 0.2 && redis-cli GET bar
sleep 0.2 && redis-cli GET bar
sleep 0.2 && redis-cli GET bar
sleep 0.2 && redis-cli GET bar
echo "After a second"
sleep 0.2 && redis-cli GET bar
sleep 0.2 && redis-cli GET bar


redis-cli SET foo bar px 400
sleep 0.2 && redis-cli GET foo
echo "After two milliseconds"
sleep 0.2 && redis-cli GET foo