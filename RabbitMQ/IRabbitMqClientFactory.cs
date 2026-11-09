using MessageBroker.RabbitMQ.Clients;


namespace MessageBroker.RabbitMQ.Factories
{
    public interface IRabbitMqClientFactory
    {
        Task<RabbitMqClient> GetClientAsync(string clientProvidedName, string exchangeName, string queueName, string routingKey);
    }
}