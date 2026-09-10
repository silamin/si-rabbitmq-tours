using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

// ---------------------------------------------------------------------------
//  BACK OFFICE  –  binds the WILDCARD routing key "tour.*"
//
//  '*' matches exactly one word, so "tour.*" matches BOTH "tour.booked" and
//  "tour.cancelled". The back office keeps the books, so it has to see every
//  event, not just the happy ones.
//
//  This project is byte-for-byte the same shape as EmailService. The ONLY
//  meaningful difference is the binding key one line below.
// ---------------------------------------------------------------------------

const string Exchange   = "tours.topic";
const string QueueName  = "back-office";
const string BindingKey = "tour.*";          // <-- WILDCARD. Matches both.

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

Console.ForegroundColor = ConsoleColor.Yellow;
Console.WriteLine("========================================================");
Console.WriteLine("  BACK OFFICE");
Console.WriteLine($"  exchange    : {Exchange} (topic)");
Console.WriteLine($"  queue       : {QueueName}");
Console.WriteLine($"  binding key : {BindingKey}        <-- wildcard");
Console.WriteLine("  => sees BOOKINGS *and* CANCELLATIONS.");
Console.WriteLine("========================================================");
Console.ResetColor();
Console.WriteLine(" [*] Waiting for messages. Press CTRL+C to exit.");

var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += (_, ea) =>
{
    var json    = Encoding.UTF8.GetString(ea.Body.ToArray());
    var booking = JsonSerializer.Deserialize<Booking>(json);

    var verb = ea.RoutingKey == "tour.cancelled" ? "CANCELLATION" : "BOOKING";

    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine($" [x] routing key '{ea.RoutingKey}'");
    Console.ResetColor();
    Console.WriteLine($"     Ledger entry: {verb} - {booking?.Tour}");
    Console.WriteLine($"     Customer    : {booking?.Name} <{booking?.Email}>");
    Console.WriteLine($"     Recorded at : {booking?.TimestampUtc:yyyy-MM-dd HH:mm:ss} UTC");
    Console.WriteLine();

    return Task.CompletedTask;
};

await channel.BasicConsumeAsync(queue: QueueName, autoAck: true, consumer: consumer);

await Task.Delay(Timeout.Infinite);

record Booking(string Name, string Email, string Tour, string Action, DateTime TimestampUtc);
