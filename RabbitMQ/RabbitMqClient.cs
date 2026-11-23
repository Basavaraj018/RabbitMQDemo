using RabbitMQ.Client;

namespace MessageBroker.RabbitMQ.Clients
{
    public class RabbitMqClient
    {
        private readonly SemaphoreSlim _publishLock = new(1, 1);

        public IConnection Connection { get; }
        public IChannel Channel { get; }
        public string ExchangeName { get; }
        public string QueueName { get; }
        public string RoutingKey { get; }

        public RabbitMqClient(
            IConnection connection,
            IChannel channel,
            string exchangeName,
            string queueName,
            string routingKey)
        {
            Connection = connection;
            Channel = channel;
            ExchangeName = exchangeName;
            QueueName = queueName;
            RoutingKey = routingKey;
        }

        public async Task PublishAsync(ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
        {
            await _publishLock.WaitAsync(cancellationToken);
            try
            {
                await Channel.BasicPublishAsync(
                    exchange: ExchangeName,
                    routingKey: RoutingKey,
                    body: body,
                    cancellationToken: cancellationToken);
            }
            finally
            {
                _publishLock.Release();
            }
        }
    }
}
