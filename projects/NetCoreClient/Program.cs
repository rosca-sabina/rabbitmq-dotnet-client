using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.client.framing;
using RabbitMQ.Client.Framing.Impl;
using RabbitMQ.Client.Impl;
using RabbitMQ.Util;
using TcpClient = NetCoreServer.TcpClient;

namespace NetCoreClient
{
    class RabbitMQClient : TcpClient
    {
        const int EndMarkerLength = 1;

        enum State
        {
            DISCONNECTED,
            AWAIT_HANDSHAKE,
        }

        private static readonly byte[] _amqpHeader = new byte[] {(byte)'A', (byte)'M', (byte)'Q', (byte)'P', 0, 0, 9, 1};
        private State _state = State.DISCONNECTED;

        public RabbitMQClient(string address, int port) : base(address, port)
        {
        }

        public void DisconnectAndStop()
        {
            _stop = true;
            DisconnectAsync();
            while (IsConnected)
                Thread.Yield();
        }

        protected override void OnConnected()
        {
            Console.WriteLine($"RabbitMQ client connected a new session with Id {Id}, sending AMQP handshake");
            _state = State.AWAIT_HANDSHAKE;
            SendAsync(_amqpHeader);
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
            Console.WriteLine($"[DEBUG] OnReceived offset: {offset} size: {size}");
            switch (_state)
            {
                case State.AWAIT_HANDSHAKE:
                    if (size < 6)
                    {
                        Console.Error.WriteLine("[ERROR] TODO whoops want more data");
                    }
                    byte firstByte = buffer[0];
                    if (firstByte == 'A')
                    {
                        // Probably an AMQP protocol header, otherwise meaningless
                        Console.Error.WriteLine("[ERROR] first byte is an A");
                    }
                    else
                    {
                        FrameType type = (FrameType)firstByte;
                        var frameHeaderSpan = new ReadOnlySpan<byte>(buffer, 1, 6);
                        int channel = NetworkOrderDeserializer.ReadUInt16(frameHeaderSpan);
                        int payloadSize = NetworkOrderDeserializer.ReadInt32(frameHeaderSpan.Slice(2, 4));
                        // TODO max message size
                        int readSize = payloadSize + EndMarkerLength;
                        Console.WriteLine($"[DEBUG] OnReceived frame type: {type} channel: {channel} payloadSize: {payloadSize} readSize: {readSize}");
                        if (readSize > size)
                        {
                            Console.Error.WriteLine("[ERROR] TODO whoops want more data");
                        }
                        else
                        {
                            if (buffer[payloadSize + 7] != Constants.FrameEnd)
                            {
                                Console.Error.WriteLine($"[ERROR] bad frame end marker: {buffer[payloadSize]}");
                            }
                            else
                            {
                                var payloadSpan = new ReadOnlySpan<byte>(buffer, 7, readSize);
                                var commandId = (ProtocolCommandId)NetworkOrderDeserializer.ReadUInt32(payloadSpan);
                                Debug.Assert(commandId == ProtocolCommandId.ConnectionStart);
                                var cs = new ConnectionStart(payloadSpan.Slice(4));
                                Dump(cs);
                            }
                        }
                    }
                    break;
                default:
                    Console.WriteLine(Encoding.UTF8.GetString(buffer, (int)offset, (int)size));
                    break;
            }
        }

        protected override void OnError(SocketError error)
        {
            Console.WriteLine($"RabbitMQ client caught an error with code {error}");
        }

        private bool _stop;

        private static void Dump(object o)
        {
            string json = JsonConvert.SerializeObject(o, Formatting.Indented);
            Console.WriteLine($"[DEBUG] {json}");
        }
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

            Thread.Sleep(TimeSpan.FromSeconds(5));

            // Disconnect the client
            Console.Write("Client disconnecting...");
            client.DisconnectAndStop();
            Console.WriteLine("Done!");
        }
    }
}
