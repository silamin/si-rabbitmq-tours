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
//
//  DAY 5: same durability and manual-ack treatment as BackOffice. An e-mail
//  that was never sent because the process died must not look like one that
//  was sent.
// ---------------------------------------------------------------------------

var hostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";

var factory = new ConnectionFactory { HostName = hostName };
await using var connection = await factory.CreateConnectionAsync();
await using var channel    = await connection.CreateChannelAsync();

await Topology.DeclareAsync(channel);
await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

Console.ForegroundColor = ConsoleColor.Cyan;
Console.WriteLine("========================================================");
Console.WriteLine("  EMAIL SERVICE");
Console.WriteLine($"  exchange    : {Topology.Exchange} (topic, durable)");
Console.WriteLine($"  queue       : {Topology.EmailQueue} (durable)");
Console.WriteLine("  binding key : tour.booked   <-- exact match only");
Console.WriteLine($"  dead-letter : {Topology.DeadLetterExchange}   (invalid messages go here)");
Console.WriteLine("  manual ack  : ON");
Console.WriteLine("  => sees BOOKINGS only. Cancellations never arrive here.");
Console.WriteLine("========================================================");
Console.ResetColor();
Console.WriteLine(" [*] Waiting for messages. Press CTRL+C to exit.");

var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += async (_, ea) =>
{
    var json = Encoding.UTF8.GetString(ea.Body.ToArray());

    Booking? booking;
    try   { booking = JsonSerializer.Deserialize<Booking>(json); }
    catch (JsonException) { booking = null; }

    // No address, no e-mail. That is an invalid message, not a failed send.
    var invalid = booking is null || string.IsNullOrWhiteSpace(booking.Email);

    if (invalid)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($" [!] INVALID message on '{ea.RoutingKey}' - rejecting to {Topology.DeadLetterExchange}");
        Console.WriteLine($"     raw body: {json}");
        Console.ResetColor();
        Console.WriteLine();
        await channel.BasicRejectAsync(ea.DeliveryTag, requeue: false);
        return;
    }

    Console.ForegroundColor = ConsoleColor.Cyan;
    Console.WriteLine($" [x] routing key '{ea.RoutingKey}'");
    Console.ResetColor();
    Console.WriteLine($"     Sending confirmation e-mail to {booking!.Email}");
    Console.WriteLine($"     \"Hi {booking.Name}, your booking for '{booking.Tour}' is confirmed.\"");
    Console.WriteLine();

    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
};

await channel.BasicConsumeAsync(queue: Topology.EmailQueue, autoAck: false, consumer: consumer);

await Task.Delay(Timeout.Infinite);

record Booking(string Name, string Email, string Tour, string Action, DateTime TimestampUtc);
