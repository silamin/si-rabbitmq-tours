using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// ---------------------------------------------------------------------------
//  EMAIL SERVICE  –  binds the EXACT routing key "tour.booked"
//
//  This consumer only ever hears about *bookings*. A cancellation is published
//  with the key "tour.cancelled", which does not match "tour.booked", so the
//  exchange never puts a copy in this queue.
//
//  Compare with BackOffice, which binds "tour.*" and therefore hears both.
//  That difference is the entire demonstration of topic routing.
// ---------------------------------------------------------------------------

const string Exchange   = "tours.topic";
const string QueueName  = "email-service";
const string BindingKey = "tour.booked";     // <-- EXACT. No wildcard.

var hostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";

var factory = new ConnectionFactory { HostName = hostName };
await using var connection = await factory.CreateConnectionAsync();
await using var channel    = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync(
    exchange: Exchange, type: ExchangeType.Topic, durable: false, autoDelete: false);

await channel.QueueDeclareAsync(
    queue: QueueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

await channel.QueueBindAsync(
    queue: QueueName, exchange: Exchange, routingKey: BindingKey);

Console.ForegroundColor = ConsoleColor.Cyan;
Console.WriteLine("========================================================");
Console.WriteLine("  EMAIL SERVICE");
Console.WriteLine($"  exchange    : {Exchange} (topic)");
Console.WriteLine($"  queue       : {QueueName}");
Console.WriteLine($"  binding key : {BindingKey}   <-- exact match only");
Console.WriteLine("  => sees BOOKINGS only. Cancellations never arrive here.");
Console.WriteLine("========================================================");
Console.ResetColor();
Console.WriteLine(" [*] Waiting for messages. Press CTRL+C to exit.");

var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += (_, ea) =>
{
    var json    = Encoding.UTF8.GetString(ea.Body.ToArray());
    var booking = JsonSerializer.Deserialize<Booking>(json);

    Console.ForegroundColor = ConsoleColor.Cyan;
    Console.WriteLine($" [x] routing key '{ea.RoutingKey}'");
    Console.ResetColor();
    Console.WriteLine($"     Sending confirmation e-mail to {booking?.Email}");
    Console.WriteLine($"     \"Hi {booking?.Name}, your booking for '{booking?.Tour}' is confirmed.\"");
    Console.WriteLine();

    return Task.CompletedTask;
};

await channel.BasicConsumeAsync(queue: QueueName, autoAck: true, consumer: consumer);

await Task.Delay(Timeout.Infinite);

record Booking(string Name, string Email, string Tour, string Action, DateTime TimestampUtc);
