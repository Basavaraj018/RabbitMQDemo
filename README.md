# .NET RabbitMQ Message Broker & Background Worker

A thread-safe, event-driven RabbitMQ implementation in .NET using `RabbitMQ.Client` (v7+ async API). It uses a single shared TCP connection across the application while isolating publishing and consuming onto dedicated channels.

---

## Table of Contents
- [Architecture Overview](#architecture-overview)
- [Running RabbitMQ Locally with Docker](#running-rabbitmq-locally-with-docker)
  - [Docker Command Breakdown](#docker-command-breakdown)
  - [Accessing the Management UI](#accessing-the-management-ui)
- [Project Setup & Registration](#project-setup--registration)
- [Usage Examples](#usage-examples)
  - [Publishing Messages](#publishing-messages)
  - [Consuming Messages (BackgroundService)](#consuming-messages-backgroundservice)
- [Key Design Decisions](#key-design-decisions)

---

## Architecture Overview

```text
+-------------------------------------------------------------------+

|                        .NET Application                           |
|                                                                   |
|  +-----------------------+           +-------------------------+  |
|  |   Publisher Service   |           |   Background Worker     |  |
|  | (SemaphoreSlim Guard) |           | (AsyncEventingConsumer) |  |
|  +-----------+-----------+           +------------+------------+  |
|              |                                    |               |
|     Dedicated IChannel #1                Dedicated IChannel #2    |
|              |                                    |               |
|              +-----------------+------------------+               |
|                                |                                  |
|               Single Shared IConnection (TCP Socket)              |
|                 ClientProvidedName: "AppSharedConnection"         |
+--------------------------------+----------------------------------+
                                 |
                                 v (Port 5672 - AMQP)
                     +-----------------------+

                     |    RabbitMQ Server    |
                     |   (Docker Container)  |
                     +-----------------------+
```

* **1 Shared TCP Connection (`IConnection`):** Thread-safe and multiplexed across the entire application lifecycle.
* **Separate Channels (`IChannel`):** Publishing and consuming never share a channel, avoiding frame corruption and concurrency exceptions.
* **Push-Based Consumption:** Uses `AsyncEventingBasicConsumer` instead of a polling `while` loop for zero idle CPU usage.

---

## Running RabbitMQ Locally with Docker

Run the following command in your terminal to start a local RabbitMQ instance with the web management dashboard enabled:

```bash
docker run -d --hostname rmq --name rabbit-server -p 8080:15672 -p 5672:5672 rabbitmq:3-management
```

### Docker Command Breakdown

| Flag / Argument | Meaning & Purpose |
| :--- | :--- |
| **`docker run`** | Creates and starts a new container from an image. |
| **`-d`** | **Detached mode.** Runs the container in the background so your terminal remains free. |
| **`--hostname rmq`** | Sets the internal hostname inside the container to `rmq`. RabbitMQ stores data (queues, users, messages) in a folder named after the node's hostname, so setting a fixed hostname prevents data loss if you attach volumes later. |
| **`--name rabbit-server`** | Assigns a friendly name (`rabbit-server`) to the container so you can easily manage it (e.g., `docker stop rabbit-server`, `docker logs rabbit-server`). |
| **`-p 8080:15672`** | Maps **host port `8080`** to **container port `15672`**. Port `15672` is RabbitMQ's HTTP Management UI, which you can open in your browser at `http://localhost:8080`. |
| **`-p 5672:5672`** | Maps **host port `5672`** to **container port `5672`**. Port `5672` is the standard AMQP protocol port that your .NET application connects to (`amqp://guest:guest@localhost:5672`). |
| **`rabbitmq:3-management`** | The official Docker image to pull and run. The **`3-management`** tag includes RabbitMQ v3 pre-packaged with the web management plugin enabled. |

### Accessing the Management UI
1. Open your browser and navigate to: **`http://localhost:8080`**
2. Log in with the default credentials:
   * **Username:** `guest`
   * **Password:** `guest`
3. Under the **Connections** tab, you will see your .NET app listed as **`AppSharedConnection`**.

---

## Project Setup & Registration

### 1. Required NuGet Package
```bash
dotnet add package RabbitMQ.Client
```

### 2. Dependency Injection (`Program.cs`)
Register `RabbitMqClientFactory` as a **Singleton** so the TCP connection and cached channels live for the lifetime of the application, and register `Worker` as a hosted background service:

```csharp
using MessageBroker;
using MessageBroker.RabbitMQ.Factories;
using MessageProcessor;

var builder = Host.CreateApplicationBuilder(args);

// Register Factory as Singleton
builder.Services.AddSingleton<IRabbitMqClientFactory, RabbitMqClientFactory>();
builder.Services.AddSingleton<RabbitMqClientFactory>();

// Register Background Consumer
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
```

---

## Usage Examples

### Publishing Messages
Always pass `"Publisher"` (or a dedicated sender role name) when requesting a client to publish messages. The `PublishAsync` method uses a `SemaphoreSlim(1, 1)` internally to ensure thread safety across concurrent HTTP requests.

```csharp
using System.Text;
using MessageBroker;

public class OrderService
{
    private readonly RabbitMqClientFactory _factory;

    public OrderService(RabbitMqClientFactory factory)
    {
        _factory = factory;
    }

    public async Task SendOrderCreatedAsync(string orderJson, CancellationToken ct)
    {
        var publisher = await _factory.GetClientAsync(
            clientRoleName: "Publisher",
            exchangeName: "my_exchange",
            queueName: "my_queue",
            routingKey: "my_routing_key");

        var body = Encoding.UTF8.GetBytes(orderJson);
        await publisher.PublishAsync(body, ct);
    }
}
```

### Consuming Messages (`BackgroundService`)
The worker initializes the `"Consumer"` client **once** on startup, sets `BasicQosAsync` (`prefetchCount: 1`) so it processes one message at a time, and relies on `AsyncEventingBasicConsumer` to react to incoming messages.

* **Manual Acknowledgments (`autoAck: false`):** Messages are only removed from RabbitMQ after `BasicAckAsync` succeeds. If an exception occurs, `BasicNackAsync` requeues the message so no data is lost.
* **Graceful Shutdown:** `await Task.Delay(Timeout.Infinite, stoppingToken)` keeps the `BackgroundService` alive without blocking threads until the host shuts down.

---

## Key Design Decisions

1. **Channel Isolation:** `IChannel` instances in `RabbitMQ.Client` are not thread-safe. Using distinct role keys (`"Publisher"` vs. `"Consumer"`) guarantees separate channels for sending and receiving.
2. **Cache Poisoning Prevention:** If RabbitMQ is unreachable when `CreateClientAsync` runs, the failed `Lazy<Task>` entry is evicted from the `ConcurrentDictionary` so subsequent calls can retry automatically once the broker recovers.
3. **Automatic Recovery:** `AutomaticRecoveryEnabled = true` is enabled on `ConnectionFactory` so connections and topologies recover automatically after transient network drops.
