using RabbitMQ.Client;

namespace MessageBroker.RabbitMQ.Clients
{
    public sealed class RabbitMqClient : IAsyncDisposable
    {

        public IConnection Connection { get; }
        public IChannel Channel { get; }

        private readonly string _exchangeName;
        private readonly string _queueName;
        private readonly string _routingKey;

        public RabbitMqClient(
            IConnection connection,
            IChannel channel, string exchangeName, string queueName, string routingKey)
        {
            Connection = connection;
            Channel = channel;
            _exchangeName = exchangeName;
            _queueName = queueName;
            _routingKey = routingKey;
        }

        public async Task SendMessageAsync(string message)
        { 
            var body = System.Text.Encoding.UTF8.GetBytes(message);
            await Channel.BasicPublishAsync(
                exchange: _exchangeName,
                routingKey: _routingKey,
                body: body);
        }


        public async ValueTask DisposeAsync()
        {
            await Channel.DisposeAsync();
            await Connection.DisposeAsync();
        }
    }
}
