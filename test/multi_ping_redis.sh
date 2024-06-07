# Uncomment this part if you start the server on the port: 6399
# for i in {1..5}; do redis-cli -p 6399 PING; done

for i in {1..5}; do redis-cli PING; done