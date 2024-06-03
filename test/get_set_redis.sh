redis-cli SET foo "valore salvato"
redis-cli GET foo

valueToShow=10
redis-cli SET valueNum "valore numerico $valueToShow"
redis-cli GET valueNum

# sleep 0.2 && redis-cli GET foo