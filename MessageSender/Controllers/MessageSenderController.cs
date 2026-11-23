using MessageBroker;
using Microsoft.AspNetCore.Mvc;
using System.Text;

namespace MessageSender.Controllers
{
    [ApiController]
    [Route("Messsage")]
    public class MessageSenderController : ControllerBase
    {
        private readonly RabbitMqClientFactory _factory;
        public MessageSenderController(RabbitMqClientFactory rabbitMqClientFactory) 
        {
            _factory = rabbitMqClientFactory;
        }

        [Route("Send/{message}")]
        [HttpGet]
        public async Task<IActionResult> GetAsync(string message)
        {
            var publisher = await _factory.GetClientAsync("Publisher", "my_exchange", "my_queue", "my_routing_key");
            await publisher.PublishAsync(Encoding.UTF8.GetBytes(message));
            return Ok();
        }
    }
}
