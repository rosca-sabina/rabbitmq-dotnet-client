using System.Net.Sockets;
using System.Text;
using TcpClient = NetCoreServer.TcpClient;

namespace NetCoreClient
{
    class RabbitMQClient : TcpClient
    {
        public RabbitMQClient(string address, int port) : base(address, port) { }

        public void DisconnectAndStop()
        {
            _stop = true;
            DisconnectAsync();
            while (IsConnected)
                Thread.Yield();
        }

        protected override void OnConnected()
        {
            Console.WriteLine($"RabbitMQ client connected a new session with Id {Id}");
        }

        protected override void OnDisconnected()
        {
            Console.WriteLine($"RabbitMQ client disconnected a session with Id {Id}");

            // Wait for a while...
            Thread.Sleep(1000);

            // Try to connect again
            if (!_stop)
                ConnectAsync();
        }

        protected override void OnReceived(byte[] buffer, long offset, long size)
        {
            Console.WriteLine(Encoding.UTF8.GetString(buffer, (int)offset, (int)size));
        }

        protected override void OnError(SocketError error)
        {
            Console.WriteLine($"RabbitMQ client caught an error with code {error}");
        }

        private bool _stop;
    }

    class Program
    {
        static void Main(string[] args)
        {
            // TCP server address
            string address = "127.0.0.1";
            if (args.Length > 0)
                address = args[0];

            // TCP server port
            int port = 5672;
            if (args.Length > 1)
                port = int.Parse(args[1]);

            Console.WriteLine($"RabbitMQ server address: {address}");
            Console.WriteLine($"RabbitMQ server port: {port}");

            Console.WriteLine();

            // Create a new TCP chat client
            var client = new RabbitMQClient(address, port);

            // Connect the client
            Console.Write("RabbitMQ client connecting...");
            client.ConnectAsync();
            Console.WriteLine("Done!");

            Thread.Sleep(TimeSpan.FromSeconds(1));

            // Disconnect the client
            Console.Write("Client disconnecting...");
            client.DisconnectAndStop();
            Console.WriteLine("Done!");
        }
    }
}
