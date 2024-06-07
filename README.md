# Stage 1
The entry point for your Redis implementation is in `src/Server.cs`. 

# Stage 2
1. Ensure you have `dotnet (8.0)` installed locally
1. Run `./spawn_redis_server.sh` to run your Redis server, which is implemented
   in `src/Server.cs`.

# Stage 3
After downloading the dependencies to run `./spawn_redis_server.sh`, you will be able to start the server through the parameter: --port <_portNumber_>

_If the --port parameter should not be there, the server will be started on port 6379_

# Stage 4
The possible commands that can be launched are:
### ping
He will respond with pong

### echo <_string/number_>
It will print the marked text on the screen

### set <_variableName_> <_value_> px <_number_>
It will add to db a variable called <_variableName_> with the value <_value_>.
If added px, then after <_number_> milliseconds the variable will be automatically deleted from db

### get <_variableName_>
Retrieves the value from the variable: <_variableName_>