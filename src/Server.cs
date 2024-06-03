using System.Net;
using System.Net.Sockets;
using System.Text;

Dictionary<string, string> getAndSet = new Dictionary<string, string>();
const string crlf = "\r\n";
using var server = new TcpListener(IPAddress.Any, 6379);

Console.WriteLine("Logs from your program will appear here!");
try {
  server.Start();
  while (true) {
    Socket socket = server.AcceptSocket(); // wait for client
    _ = Task.Run(() => PongResponder(socket));
  }
} finally {
  server.Stop();
}



async Task PongResponder(Socket socket) {
  while (socket.Connected) {
    byte[] rxBuffer = new byte[1024];
    var Chars = new char[1024];
    int bytesReceived = await socket.ReceiveAsync(rxBuffer);

    Console.WriteLine($"Received {bytesReceived} bytes.");
    
    if(bytesReceived > 0) {
      string data = Encoding.ASCII.GetString(rxBuffer, 0, bytesReceived);
      var request = data.Split(crlf, StringSplitOptions.RemoveEmptyEntries);

      if (request.Length > 0) {
        var parsingReq = await HandleParsing(request);
        await socket.SendAsync(Encoding.ASCII.GetBytes(parsingReq));
      } 
    } else {
      socket.Close();
    }
  }
}

async Task<string> HandleParsing(string[] request) {
  string RespSimpleString(string input) => $"+{input}{crlf}";
  string reply = "-ERR unknown command\r\n";
  string cmd = request[2];
  string argsReq = request[4];

  switch (cmd.ToLower()) {
    case "ping":
      reply = RespSimpleString("PONG");
      break;
    case "echo":
      reply = $"${argsReq.Length}\r\n{argsReq}\r\n";
      break;
    case "set":
      string valueToSet = request[6];
      getAndSet[argsReq] = valueToSet;
      Console.WriteLine($"getAndSet[argsReq]: {getAndSet[argsReq]}");
      reply = "+OK\r\n";
      break;
    case "get":      
      if (getAndSet.ContainsKey(argsReq)) {
        var valueToGet = getAndSet[argsReq];
        Console.WriteLine($"valueToGet?: {valueToGet}");
        reply = $"${valueToGet.Length}\r\n{valueToGet}\r\n";
      } else {
        reply = "$-1\r\n";
      }
      break;
  }
  return reply;
}