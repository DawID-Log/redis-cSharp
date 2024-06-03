using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Runtime.Caching;
using System.Text;

var db = MemoryCache.Default;
const string crlf = "\r\n";
const int accountForEnding = 2;
int port = 6379;

if (args.Length == 2 && args[0].ToUpper() == "--PORT") {
  port = int.Parse(args[1]);
  Console.WriteLine($"{port}");
}
TcpListener server = new TcpListener(IPAddress.Any, port);

var ParseLength = (byte[] bytes, int start) => {
  int len = 0;
  int i = start + 1;
  int consumed = 1;

  // 0x0D is '\r' hex value
  while (bytes[i] != 0x0D) {
    len = (len * 10) + (bytes[i] - '0');
    i++;
    consumed++;
  }

  return (len, consumed + accountForEnding);
};

var ParseBulkStr = (Byte[] bytes, int start) => {
  var (len, consumed) = ParseLength(bytes, start);
  Byte[] payload = new Byte[len];
  Array.Copy(bytes, start + consumed, payload, 0, len);
  var parsed = Encoding.ASCII.GetString(payload);

  return (parsed, consumed + len + accountForEnding);
};

var ParseClientMsg = (Byte[] msg) => {
  var (bulkStrsToParse, cursor) = ParseLength(msg, 0);
  String bulkStr = "";

  for (int i = 0; i < bulkStrsToParse; i++) {
    var (str, consumed) = ParseBulkStr(msg, cursor);
    bulkStr += (str + " ");
    cursor += consumed;
  }
  bulkStr = bulkStr.TrimEnd();
  return bulkStr;
};

var ProcessPing = (string inputCmd) => {
  return inputCmd.ToLower() switch {
    "ping" => "+PONG\r\n",
    _ => string.Format("${0}\r\n{1}\r\n", inputCmd.Length - 5, inputCmd.Substring(5)),
  };
};

var ProcessEcho = (string inputCmd) => {
  return string.Format("${0}\r\n{1}\r\n", inputCmd.Length - 5, inputCmd.Substring(5));
};

var ProcessSet = (string inputCmd) => {
  var parts = inputCmd.Split(' ');
  var valueToBeSave = (object)parts[2];
  if (parts[0] != "set") {
    throw new ArgumentException("Expected: set <k> <v> [px <t>], but got {0}", inputCmd);
  } else if (parts.Length > 5 || parts[parts.Length - 2] != "px") {
    // -4: length of the array substring, starting at index 2 and ending at index parts.Length - 2.
    int length = parts.Length - 2;
    if(parts[parts.Length - 2] == "px"){
      length -= 2;
    }
    valueToBeSave = (object)string.Join(" ", parts, 2, length);
  }

  if (parts[parts.Length - 2] == "px") {
    var ttl = int.Parse(parts[parts.Length - 1]);
    db.Set(parts[1], valueToBeSave, DateTimeOffset.Now.AddMilliseconds(ttl));
  } else {
    db.Set(parts[1], valueToBeSave, DateTimeOffset.MaxValue);
  }
  return "+OK\r\n";
};

var ProcessGet = (string inputCmd) => {
  var parts = inputCmd.Split(" ");

  if (parts.Length != 2 || parts[0] != "get") {
    throw new ArgumentException("Expected: get <k>, but got {0}", inputCmd);
  }
  string? val = db[parts[1]] as string;

  if (val == null) {
    return "$-1\r\n";
  }
  
  return String.Format("${0}\r\n{1}\r\n", val.Length, val);
};

var HandleClient = (TcpClient client) => {
  NetworkStream stream = client.GetStream();
  byte[] rxBuffer = new byte[1024];

  while (true) {
    int bytesResponse = stream.Read(rxBuffer);
    if (bytesResponse == 0)
      return;
    if (bytesResponse < rxBuffer.Length) {
      Array.Resize(ref rxBuffer, bytesResponse);
    }
    String command = ParseClientMsg(rxBuffer);

    var response = command.ToLower() switch {
      var input when input.StartsWith("ping") => ProcessPing(input),
      var input when input.StartsWith("echo") => ProcessEcho(input),
      var input when input.StartsWith("set") => ProcessSet(input),
      var input when input.StartsWith("get") => ProcessGet(input),
      _ => throw new ArgumentException(
          string.Format("Received unknown redis command: {0}", command)),
    };
    byte[] bytes = Encoding.ASCII.GetBytes(response);
    stream.Write(bytes);
  }
};

Console.WriteLine("Logs from your program will appear here!");
try {
  server.Start();
  while (true) {
    TcpClient tcpClient = await server.AcceptTcpClientAsync();
    new Thread(() => HandleClient(tcpClient)).Start();
  }
} catch (Exception ex) {
  Console.WriteLine(ex.Message);
}