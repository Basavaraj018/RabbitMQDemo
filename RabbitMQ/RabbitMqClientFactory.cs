using MessageBroker.RabbitMQ.Clients;
using MessageBroker.RabbitMQ.Factories;
using RabbitMQ.Client;
using System.Collections.Concurrent;

namespace MessageBroker
{
    public class RabbitMqClientFactory : IRabbitMqClientFactory
    {
        private readonly ConcurrentDictionary<
        string,
        Lazy<Task<RabbitMqClient>>> _clients = new();

        public Task<RabbitMqClient> GetClientAsync(
            string clientProvidedName, string exchangeName, string queueName, string routingKey)
        {
            var lazyClient = _clients.GetOrAdd(
                clientProvidedName,
                name => new Lazy<Task<RabbitMqClient>>(
                    () => CreateClientAsync(name, exchangeName, queueName, routingKey),
                    LazyThreadSafetyMode.ExecutionAndPublication));

            return lazyClient.Value;
        }

        private async Task<RabbitMqClient> CreateClientAsync(
            string clientProvidedName, string exchangeName, string queueName, string routingKey)
        {
            try
            {
                var factory = new ConnectionFactory
                {
                    Uri = new Uri("amqp://guest:guest@localhost:5672"),
                    ClientProvidedName = clientProvidedName
                };

                var connection = await factory.CreateConnectionAsync();

                var channel = await connection.CreateChannelAsync();

                await channel.ExchangeDeclareAsync(
                    exchange: exchangeName,
                    type: ExchangeType.Direct);

                await channel.QueueDeclareAsync(
                    queue: queueName,
                    durable: false,
                    exclusive: false,
                    autoDelete: false);

                await channel.QueueBindAsync(
                    queue: queueName,
                    exchange: exchangeName,
                    routingKey: routingKey);

                return new RabbitMqClient(
                    connection,
                    channel, exchangeName, queueName, routingKey);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to create RabbitMQ client for '{clientProvidedName}'.", ex);
            }
        }
    }
}
