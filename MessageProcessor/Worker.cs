using MessageBroker;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace MessageProcessor
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly RabbitMqClientFactory _factory;

        public Worker(ILogger<Worker> logger, RabbitMqClientFactory rabbitMqClientFactory)
        {
            _logger = logger;
            _factory = rabbitMqClientFactory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // Use "Consumer" as the role name (use "Publisher" in your sender service)
            var consumerClient = await _factory.GetClientAsync(
                "Consumer",
                "my_exchange",
                "my_queue",
                "my_routing_key");

            await consumerClient.Channel.BasicQosAsync(
                prefetchSize: 0,
                prefetchCount: 1,
                global: false,
                cancellationToken: stoppingToken);

            var consumer = new AsyncEventingBasicConsumer(consumerClient.Channel);
            consumer.ReceivedAsync += async (model, ea) =>
            {
                try
                {
                    var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                    _logger.LogInformation("Received message: {Message}", message);

                    // TODO: Process message here

                    await consumerClient.Channel.BasicAckAsync(
                        deliveryTag: ea.DeliveryTag,
                        multiple: false,
                        cancellationToken: stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing message");
                    await consumerClient.Channel.BasicNackAsync(
                        deliveryTag: ea.DeliveryTag,
                        multiple: false,
                        requeue: true,
                        cancellationToken: stoppingToken);
                }
            };

            await consumerClient.Channel.BasicConsumeAsync(
                queue: "my_queue",
                autoAck: false,
                consumer: consumer,
                cancellationToken: stoppingToken);

            try
            {
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                // Graceful shutdown
            }
        }
    }
}
