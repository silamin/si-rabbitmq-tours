using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// ---------------------------------------------------------------------------
//  ADMIN APP  -  the log of everything that did NOT arrive normally.
//
//  Two queues feed it, and they fail for two different reasons:
//
//    undeliverable      the exchange had nowhere to put the message. The
//                       routing key matched no binding at all. RabbitMQ would
//                       normally DROP it silently; the ALTERNATE EXCHANGE on
//                       tours.topic catches it instead.
//
//    invalid-messages   the message was delivered fine, but a consumer could
//                       not process it and rejected it with requeue:false.
//                       The queue's DEAD-LETTER EXCHANGE catches those.
//
//  Nothing is lost and nothing is silently dropped: every message ends up in
//  a consumer, in this log, or still on a durable queue.
// ---------------------------------------------------------------------------

const string UndeliverableQueue = "undeliverable";
const string InvalidQueue       = "invalid-messages";

var hostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";

var factory = new ConnectionFactory { HostName = hostName };
await using var connection = await factory.CreateConnectionAsync();
await using var channel    = await connection.CreateChannelAsync();

await Topology.DeclareAsync(channel);

Console.ForegroundColor = ConsoleColor.Magenta;
Console.WriteLine("========================================================");
Console.WriteLine("  ADMIN APP  -  undeliverable & invalid message log");
Console.WriteLine($"  queue : {UndeliverableQueue}      (alternate exchange)");
Console.WriteLine($"  queue : {InvalidQueue}   (dead-letter exchange)");
Console.WriteLine("========================================================");
Console.ResetColor();
Console.WriteLine(" [*] Waiting. Every line below is a message that did not make it.");
Console.WriteLine();

var seq = 0;

Task Handle(string source, BasicDeliverEventArgs ea)
{
    seq++;
    var body   = Encoding.UTF8.GetString(ea.Body.ToArray());
    var reason = ReasonFor(ea, source);

    Console.ForegroundColor = ConsoleColor.Magenta;
    Console.WriteLine($" [#{seq}] {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC   source: {source}");
    Console.ResetColor();
    Console.WriteLine($"        reason      : {reason}");
    Console.WriteLine($"        routing key : {ea.RoutingKey}");
    Console.WriteLine($"        body        : {body}");
    Console.WriteLine();
    return Task.CompletedTask;
}

// The broker stamps an x-death header on dead-lettered messages. It carries the
// queue it came from and why it left, so the log can say more than "it failed".
static string ReasonFor(BasicDeliverEventArgs ea, string source)
{
    if (source == "undeliverable")
        return "UNROUTABLE - routing key matched no binding on tours.topic";

    var headers = ea.BasicProperties.Headers;
    if (headers is not null && headers.TryGetValue("x-death", out var raw)
        && raw is IList<object> deaths && deaths.Count > 0
        && deaths[0] is IDictionary<string, object> first)
    {
        var q  = first.TryGetValue("queue",  out var qv) && qv is byte[] qb ? Encoding.UTF8.GetString(qb) : "?";
        var rs = first.TryGetValue("reason", out var rv) && rv is byte[] rb ? Encoding.UTF8.GetString(rb) : "?";
        return $"REJECTED by consumer on queue '{q}' (reason: {rs}) - payload could not be processed";
    }

    return "REJECTED by a consumer - payload could not be processed";
}

var undeliverable = new AsyncEventingBasicConsumer(channel);
undeliverable.ReceivedAsync += async (_, ea) =>
{
    await Handle("undeliverable", ea);
    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
};

var invalid = new AsyncEventingBasicConsumer(channel);
invalid.ReceivedAsync += async (_, ea) =>
{
    await Handle("invalid-messages", ea);
    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
};

await channel.BasicConsumeAsync(queue: UndeliverableQueue, autoAck: false, consumer: undeliverable);
await channel.BasicConsumeAsync(queue: InvalidQueue,       autoAck: false, consumer: invalid);

await Task.Delay(Timeout.Infinite);
