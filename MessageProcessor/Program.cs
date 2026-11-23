using MessageBroker;

namespace MessageProcessor
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = Host.CreateApplicationBuilder(args);
            builder.Services.AddHostedService<Worker>();
            builder.Services.AddSingleton<RabbitMqClientFactory>();

            var host = builder.Build();
            host.Run();
        }
    }
}