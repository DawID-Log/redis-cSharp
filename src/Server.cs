using System.Net;
using System.Text;
using System.Net.Http;
using System.Text.Json;
using System.Net.Sockets;
using System.Runtime.Caching;
using System.Reflection.Metadata;
using System.Collections.Concurrent;

const string CRLF = "\r\n";
const string CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
const int ACCOUNT_FOR_ENDING = 2;
const int DEFAULT_PORT = 6379;
string CreateBulkString(string msg) { return $"${msg.Length}{CRLF}{msg}{CRLF}"; }
ConcurrentBag<Socket> replicas = [];

Dictionary<string, string> getElement = new Dictionary<string, string>();
Dictionary<string, long> setElement = new Dictionary<string, long>();

var db = MemoryCache.Default;
int port = DEFAULT_PORT;
var replicaOfIndex = args.ToList().IndexOf("--replicaof");
var portIndex = args.ToList().IndexOf("--port");
var masterAddressWithPort = replicaOfIndex > -1 ? args[replicaOfIndex + 1] : string.Empty;
var masterAddress = replicaOfIndex > -1 ? masterAddressWithPort.Split(" ")[0] : string.Empty;
var masterPort = replicaOfIndex > -1 ? Convert.ToInt32(masterAddressWithPort.Split(" ")[1]) : 0;
var role = replicaOfIndex == -1 ? "master" : "slave";
var replId = RandomString(40, true);
bool isHandshakeCompleted = false;
int replicaOffset = 0;
int offsetGetAck = 0;

if (portIndex != -1)
{
  port = int.Parse(args[portIndex + 1]);
}


var ProcessPing = (String inputCmd) =>
{
  return inputCmd.ToLower() switch
  {
    "ping" => "+PONG\r\n",
    _ => string.Format("${0}\r\n{1}\r\n", inputCmd.Length - 5, inputCmd.Substring(5)),
  };
};

var ProcessEcho = (string inputCmd) =>
{
  return $"${inputCmd.Length}\r\n{inputCmd}\r\n";
};

var ProcessInfo = (string[] inputCmd) =>
{
  var values = new Dictionary<string, string> {
    { "role", role },
    { "replicaof", masterAddressWithPort },
    { "master_replid", replId },
    { "master_repl_offset",replicaOffset.ToString() }
  };

  var response = string.Join("\n", values.Select(x => $"{x.Key}:{x.Value}")) + "\n";

  if (inputCmd.Length > 4 && inputCmd[4].Equals("replication", StringComparison.OrdinalIgnoreCase))
  {
    values = new Dictionary<string, string> {
      { "master_replid", replId },
      { "master_repl_offset", replicaOffset.ToString() }
    };

    response = string.Join(CRLF, values.Select(repl => $"{repl.Key}:{repl.Value}")) + "\n";
    return CreateBulkString(response);
  }
  return CreateBulkString(response);
};

async void ProcessPsync(string[] inputCmd, Socket client)
{
  await client.SendAsync(Encoding.ASCII.GetBytes(
        "+FULLRESYNC " + replId + " " + replicaOffset + "\r\n"));
  string emptyRdbFileBase64 =
      "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
  byte[] rdbFile = System.Convert.FromBase64String(emptyRdbFileBase64);
  byte[] rdbResynchronizationFileMsg =
      Encoding.ASCII.GetBytes($"${rdbFile.Length}\r\n")
          .Concat(rdbFile)
          .ToArray();
  await client.SendAsync(rdbResynchronizationFileMsg);

  if (role == "master")
  {
    replicas.Add(client);
    Console.WriteLine($"Added replica: {client.RemoteEndPoint}");
  }
};

string ProcessSet(string[] request)
{
  if (request.Length >= 10 && request[8].ToLower() == "px")
  {
    long ttlValue;
    if (long.TryParse(request[10], out ttlValue))
    {
      setElement[request[4]] = ttlValue + GetEpochNow();
    }
    else
    {
      return "-ERR invalid expire time in set\r\n";
    }
  }
  getElement[request[4]] = request[6];
  return "+OK\r\n";
}

string ProcessGet(string[] request)
{
  string reply = "-ERR unknown command\r\n";
  if (getElement.ContainsKey(request[4]))
  {
    if (setElement.ContainsKey(request[4]))
    {
      if (setElement[request[4]] < GetEpochNow())
      {
        getElement.Remove(request[4]);
        setElement.Remove(request[4]);
        reply = "$-1\r\n";
      }
      else
      {
        var value = getElement[request[4]];
        reply = $"${value.Length}\r\n{value}\r\n";
      }
    }
    else
    {
      var value = getElement[request[4]];
      reply = $"${value.Length}\r\n{value}\r\n";
    }
  }
  else
  {
    reply = "$-1\r\n";
  }
  return reply;
}

async Task StartReplica()
{
  Console.ForegroundColor = ConsoleColor.Green;
  Console.WriteLine("________INITIATING_COMMUNICATION_______");
  Console.Write("Master: ");
  Console.ForegroundColor = ConsoleColor.White;
  Console.Write($"{masterAddress}:{masterPort}\n");
  Console.WriteLine("Connection in progress...");
  Console.ForegroundColor = ConsoleColor.Red;
  var master = new TcpClient();
  master.Connect(masterAddress, masterPort);
  var masterClient = master.Client;

  Console.ForegroundColor = ConsoleColor.White;
  await HandleHandshakeAsync(masterClient);


  Console.ForegroundColor = ConsoleColor.White;
  _ = HandleRequestAsync(masterClient, true);
  Console.ForegroundColor = ConsoleColor.Green;
  Console.WriteLine("Handshake completed");
  Console.WriteLine("_______________________________________\n");
  isHandshakeCompleted = true;
}

async Task HandleHandshakeAsync(Socket client)
{
  Console.ForegroundColor = ConsoleColor.White;
  var handshakeParts = GetHandshakeParts();
  var buffer = new byte[1024];
  foreach (var part in handshakeParts)
  {
    Console.ForegroundColor = ConsoleColor.Green;
    Console.Write("> Command: ");
    Console.ForegroundColor = ConsoleColor.White;
    Console.Write(part.Replace("\r\n", " ") + "\n");

    Console.ForegroundColor = ConsoleColor.Green;
    Console.Write("> Status: \n");
    Console.ForegroundColor = ConsoleColor.White;
    byte[] data = Encoding.ASCII.GetBytes(part);
    await client.SendAsync(data);
    Console.ForegroundColor = ConsoleColor.White;
    Console.Write(">>> sended\n");

    await client.ReceiveAsync(buffer);
    var response = Encoding.ASCII.GetString(buffer);
    Console.ForegroundColor = ConsoleColor.Green;
    Console.Write("> Buffer length: ");
    Console.ForegroundColor = ConsoleColor.White;
    Console.Write(buffer.Length + "\n");
    Console.ForegroundColor = ConsoleColor.Green;
    Console.Write("> Response: ");
    Console.ForegroundColor = ConsoleColor.White;
    Console.Write(response + "\n");

    if (response.ToLower().Contains("+fullresync"))
    {
      Console.WriteLine("Receiving full resync");
      _ = Task.Run(async () =>
      {
        var buffer = new byte[1024];
        await client.ReceiveAsync(buffer);
        response = Encoding.ASCII.GetString(buffer);
        Console.ForegroundColor = ConsoleColor.Green;
        Console.Write("> Response File: ");
        Console.ForegroundColor = ConsoleColor.White;
        Console.Write(response + "\n");
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine("_______________________________________\n\n");
        Console.ForegroundColor = ConsoleColor.White;
      });
    }
  }
  Console.Write("\n");
}

async Task HandleRequestAsync(Socket client, bool isSlave = false)
{
  // client.SetSocketOption(SocketOptionLevel.Socket,
  // SocketOptionName.KeepAlive, true);
  client.ReceiveTimeout = 5000;
  client.SendTimeout = 5000;
  // long lastRead = GetEpochNow();
  while (client.Connected)
  {
    // long lastReadMs = GetEpochNow() - lastRead;

    byte[] buffer = new byte[1024];
    int bytesRead = await client.ReceiveAsync(buffer);

    if (bytesRead > 0)
    {
      Console.ForegroundColor = ConsoleColor.Blue;
      Console.WriteLine($"> BytesRead from {client.RemoteEndPoint} {bytesRead}");
      var result = await HandleCommandsResponse(client, buffer, bytesRead, isSlave);
      Console.WriteLine($"Result: {result}");
    }
  }
}

async Task<bool> HandleCommandsResponse(Socket client, byte[] byteData,
                                        int bytesRead, bool isSlave = false)
{
  var data = Encoding.ASCII.GetString(byteData, 0, bytesRead);
  Console.WriteLine($"Data: {data}");
  var commands = data.Split("*");

  // Process each command
  foreach (var command in commands)
  {
    var cmd = "*" + command;
    Console.WriteLine("Command: " + command);
    var commandParts = cmd.Split("\r\n", StringSplitOptions.RemoveEmptyEntries);
    if (commandParts.Length > 0)
    {
      var result =
          await HandleCommandResponse(commandParts, client, byteData, isSlave);
      Console.WriteLine($"Result: {result}");
    }
  }
  return true;
}

string[] GetHandshakeParts()
{
  Console.WriteLine(">> Parsing");
  Console.ForegroundColor = ConsoleColor.Red;

  var lenListeningPort = port.ToString().Length;
  var listeningPort = port.ToString();
  var mystrings =
      new string[] { "*1\r\n$4\r\nPING\r\n",
                     "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" +
                         lenListeningPort.ToString() + "\r\n" + listeningPort +
                         "\r\n",
                     "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",
                     "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n" };

  Console.ForegroundColor = ConsoleColor.White;
  return mystrings;
}

void HandleReplicas(string[] request)
{
  if (role == "master" && replicas.Count > 0)
  {
    foreach (var replica in replicas)
    {
      var socket = replica;
      if (socket.Connected)
      {
        _ = Task.Run(async () =>
        {
          string requestToSend = HandleRespArray(request);
          await replica.SendAsync(
              Encoding.ASCII.GetBytes(requestToSend));
        });
        Console.WriteLine($"Sent data to replica: {replica.RemoteEndPoint}");
      }
      else
      {
        replicas.TryTake(out socket);
        Console.WriteLine(
            $"Replica not connected, removed from list: {socket.RemoteEndPoint}");
      }
    }
  }
  else
  {
    string requestToSend = HandleRespArray(request);
    Console.WriteLine(
        $"Not a master or no replicas available role: {role} replicas: {replicas.Count}");
    if (isHandshakeCompleted)
    {
      offsetGetAck += requestToSend.Length;
    }
  }
}

string HandleRespArray(string[] request)
{
  int lengthRequestArray = (request.Length - 1) / 2;
  var outstr = "";
  outstr += $"*{lengthRequestArray}\r\n";
  for (int i = 2; i < request.Length; i += 2)
  {
    outstr += $"${request[i].Length}\r\n{request[i]}\r\n";
  }
  return outstr;
}

async Task<bool> HandleCommandResponse(string[] request, Socket client,
                                       byte[] byteData, bool isSlave = false)
{
  string response = "-ERR unknown command\r\n";
  string json = JsonSerializer.Serialize(request);
  Console.WriteLine($"Received command: {json}");
  if (request.Length < 3)
  {
    return false;
  }


  switch (request[2].ToLower())
  {
    case "ping":
      response = ProcessPing(request[2]);
      if (isSlave && isHandshakeCompleted)
      {
        offsetGetAck += Encoding.ASCII.GetBytes(response).Length;
      }
      break;

    case "echo":
      response = ProcessEcho(request[4]);
      break;

    case "set":
      response = ProcessSet(request);
      HandleReplicas(request);
      if (!isSlave)
        await client.SendAsync(Encoding.ASCII.GetBytes(response));
      return true;

    case "get":
      response = ProcessGet(request);
      break;

    case "info":
      response = ProcessInfo(request);
      break;

    case "replconf":
      response = "+OK\r\n";
      if (request[4].ToLower() == "getack" && isSlave)
      {
        response = $"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n{offsetGetAck}\r\n";
        await client.SendAsync(Encoding.ASCII.GetBytes(response));
        if (isHandshakeCompleted)
        {
          offsetGetAck += response.Length;
        }
      }
      if (request[4].ToLower() == "ack" && !isSlave)
      {
        break;
      }
      break;

    case "psync":
      await client.SendAsync(Encoding.ASCII.GetBytes(
          "+FULLRESYNC " + replId + " " + replicaOffset + "\r\n"));
      string emptyRdbFileBase64 =
          "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
      byte[] rdbFile = System.Convert.FromBase64String(emptyRdbFileBase64);
      byte[] rdbResynchronizationFileMsg =
          Encoding.ASCII.GetBytes($"${rdbFile.Length}\r\n")
              .Concat(rdbFile)
              .ToArray();
      await client.SendAsync(rdbResynchronizationFileMsg);
      if (role == "master")
      {
        replicas.Add(client);
        Console.WriteLine($"Added replica: {client.RemoteEndPoint}");
        isHandshakeCompleted = true;
      }
      return true;

    case "wait":
      if(isSlave) {
        return false;
      }

      if(request.Length < 4) {
        response = Transform("WAIT <a> <b> ~ where a stands for the number of replicas and b stands for the expected time");
        break;
      }

      var numOfReplicasStr = request[4];
      var timeoutStr = request[6];
      Console.WriteLine("WAIT {0} {1}", numOfReplicasStr, timeoutStr);
      int timeout = int.Parse(timeoutStr ?? "0");
      var numOfReplicas = int.Parse(numOfReplicasStr ?? "1");
      ConcurrentBag<Socket> replicasLocal = [];
      int replicasCount = replicas.Count;
      int responsesReceived = 0;
      int indexReplicaToBeAdd = 0;

      if (replicasLocal.Count > 0) {
          while(indexReplicaToBeAdd < replicas.Count || indexReplicaToBeAdd < numOfReplicas) {
            replicasLocal.Add(replicas.ToArray()[indexReplicaToBeAdd]);
            indexReplicaToBeAdd++;
          }
          int index = 0;
          var req = TransformArray(new List<object> { "REPLCONF", "GETACK", "*" });
          var reqBytes = Encoding.ASCII.GetBytes(req);
          var tasks = replicasLocal.Where(replica => replica.Connected).Select(replica => replica.SendAsync(reqBytes));
          await Task.WhenAll(tasks);
          responsesReceived++;
      } else {
        responsesReceived = replicasCount;
      }
      Console.WriteLine("ACK received:{0}", responsesReceived);

      response = Transform(responsesReceived);
      Console.WriteLine("response:{0}", response);

      break;

    default:
      Console.WriteLine(string.Format("Received unknown redis command: {0}", request[2]));
      break;
  }

  if (!isSlave)
    await client.SendAsync(Encoding.ASCII.GetBytes(response));
  return true;
}

string EncodeString(string str) { return $"${str.Length}\r\n{str}\r\n"; }

string HandleInfo(string[] request)
{
  var values = new Dictionary<string, string> { { "role", role },
                                                { "replicaof", masterAddressWithPort },
                                                { "master_replid", replId },
                                                { "master_repl_offset",
                                                  replicaOffset.ToString() } };
  var msgString = string.Join("\n", values.Select(x => $"{x.Key}:{x.Value}"));
  if (request.Length > 4 && request[4] == "replication")
  {
    values = new Dictionary<string, string> { { "master_replid", replId },
                                              { "master_repl_offset",
                                                replicaOffset.ToString() } };
    return EncodeString(msgString);
  }
  return EncodeString(msgString);
}

long GetEpochNow()
{
  return (long)(DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalMilliseconds;
}

string RandomString(int length, bool isLower)
{
  Random random = new Random();
  return new string(Enumerable.Repeat(isLower ? CHARS.ToLower() : CHARS, length)
      .Select(s => s[random.Next(s.Length)]).ToArray());
}

string TransformArray(IList<object> items)
{
  var formattedStrings = items.Select(Transform);
  var formattedString = string.Join("", formattedStrings);
  var result = $"*{items.Count}\r\n{formattedString}";
  return result;
}

string Transform(object item)
{
  if (item.GetType() == "abc".GetType() || item.GetType() == 100.GetType())
  {
    var str = item.ToString()!;
    return $"${str.Length}\r\n{str}\r\n";
  }

  return string.Empty;
}



if (role == "slave")
{
  _ = Task.Run(async () => { await StartReplica(); });
}

TcpListener server = new TcpListener(IPAddress.Any, port);
Console.WriteLine($"Starting as {role} on port {port}...");

try
{
  server.Start();

  if (role == "master")
  {
    _ = Task.Run(async () =>
    {
      while (true)
      {
        if (replicas.Count > 0)
        {
          await Task.Delay(60000);
          var request = TransformArray(new List<object> { "REPLCONF", "GETACK", "*" });
          Console.ForegroundColor = ConsoleColor.Magenta;
          Console.WriteLine($"request send to replica: {request}");
          Console.ForegroundColor = ConsoleColor.White;
          var tasks = replicas.Where(replica => replica.Connected).Select(replica => replica.SendAsync(Encoding.ASCII.GetBytes(request)));
          await Task.WhenAll(tasks);
        }
      }
    });
  }

  while (true)
  {
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine("Waiting for connection...");
    Console.ForegroundColor = ConsoleColor.White;
    var client = await server.AcceptSocketAsync();
    _ = HandleRequestAsync(client);
  }
}
finally
{
  server.Stop();
}