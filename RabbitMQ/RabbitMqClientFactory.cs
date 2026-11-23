using MessageBroker.RabbitMQ.Clients;
using MessageBroker.RabbitMQ.Factories;
using RabbitMQ.Client;
using System.Collections.Concurrent;

namespace MessageBroker
{
    public class RabbitMqClientFactory : IRabbitMqClientFactory, IAsyncDisposable
    {
        private readonly ConnectionFactory _connectionFactory;
        private readonly SemaphoreSlim _connectionLock = new(1, 1);
        private IConnection? _sharedConnection;

        private readonly ConcurrentDictionary<string, Lazy<Task<RabbitMqClient>>> _clients = new();

        public RabbitMqClientFactory()
        {
            _connectionFactory = new ConnectionFactory
            {
                Uri = new Uri("amqp://guest:guest@localhost:5672"),
                ClientProvidedName = "AppSharedConnection",
                AutomaticRecoveryEnabled = true
            };
        }

        private async Task<IConnection> GetOrCreateConnectionAsync()
        {
            if (_sharedConnection is { IsOpen: true })
                return _sharedConnection;

            await _connectionLock.WaitAsync();
            try
            {
                if (_sharedConnection is { IsOpen: true })
                    return _sharedConnection;

                if (_sharedConnection is not null)
                    await _sharedConnection.DisposeAsync();

                _sharedConnection = await _connectionFactory.CreateConnectionAsync();
                return _sharedConnection;
            }
            finally
            {
                _connectionLock.Release();
            }
        }

        public Task<RabbitMqClient> GetClientAsync(
            string clientRoleName, string exchangeName, string queueName, string routingKey)
        {
            // Include exchange, queue, and routingKey in the key to prevent accidental collisions
            var cacheKey = $"{clientRoleName}:{exchangeName}:{queueName}:{routingKey}";

            var lazyClient = _clients.GetOrAdd(
                cacheKey,
                _ => new Lazy<Task<RabbitMqClient>>(
                    () => CreateClientAsync(cacheKey, exchangeName, queueName, routingKey),
                    LazyThreadSafetyMode.ExecutionAndPublication));

            return lazyClient.Value;
        }

        private async Task<RabbitMqClient> CreateClientAsync(
            string cacheKey, string exchangeName, string queueName, string routingKey)
        {
            try
            {
                var connection = await GetOrCreateConnectionAsync();
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
                    channel,
                    exchangeName,
                    queueName,
                    routingKey);
            }
            catch (Exception ex)
            {
                // Evict the failed task so future calls can retry instead of caching the error forever
                _clients.TryRemove(cacheKey, out _);
                throw new InvalidOperationException($"Failed to create RabbitMQ client for '{cacheKey}'.", ex);
            }
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var entry in _clients.Values)
            {
                if (entry.IsValueCreated && entry.Value.IsCompletedSuccessfully)
                {
                    var client = await entry.Value;
                    if (client.Channel.IsOpen)
                        await client.Channel.CloseAsync();
                    await client.Channel.DisposeAsync();
                }
            }

            _clients.Clear();

            if (_sharedConnection is not null)
            {
                if (_sharedConnection.IsOpen)
                    await _sharedConnection.CloseAsync();
                await _sharedConnection.DisposeAsync();
            }

            _connectionLock.Dispose();
        }
    }
}
