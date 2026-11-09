using MessageBroker;
using Microsoft.AspNetCore.Mvc;

namespace MessageSender.Controllers
{
    [ApiController]
    [Route("Messsage")]
    public class MessageSenderController : ControllerBase
    {
        private readonly RabbitMqClientFactory factory;
        public MessageSenderController(RabbitMqClientFactory rabbitMqClientFactory) 
        {
            factory = rabbitMqClientFactory;
        }

        [Route("Send/{message}")]
        [HttpGet]
        public async Task<IActionResult> GetAsync(string message)
        {
            var messageSender = await factory.GetClientAsync("App Service MessageSender", "my_exchange", "my_queue", "my_routing_key");
            await messageSender.SendMessageAsync(message);
            return Ok();
        }
    }
}
