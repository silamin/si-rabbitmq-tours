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
//  DAY 5 — "make sure that none of the messages to the BackOffice are lost".
//  Three things do that, and all three are needed:
//
//    1. the queue is DURABLE and the messages are PERSISTENT, so a broker
//       restart does not empty the queue;
//    2. autoAck is FALSE, so a message is only removed from the queue once
//       this app says it handled it. Kill this process mid-message and the
//       broker redelivers it — with autoAck:true it would already be gone;
//    3. a message this app cannot process is REJECTED with requeue:false,
//       which sends it to the dead-letter exchange instead of discarding it.
//       Rejecting with requeue:true would loop the same poison message for
//       ever, which is losing it slowly.
// ---------------------------------------------------------------------------

var hostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";

var factory = new ConnectionFactory { HostName = hostName };
await using var connection = await factory.CreateConnectionAsync();
await using var channel    = await connection.CreateChannelAsync();

await Topology.DeclareAsync(channel);

// One unacked message at a time: do not let the broker push a burst at a
// consumer that might die holding them.
await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

Console.ForegroundColor = ConsoleColor.Yellow;
Console.WriteLine("========================================================");
Console.WriteLine("  BACK OFFICE");
Console.WriteLine($"  exchange    : {Topology.Exchange} (topic, durable)");
Console.WriteLine($"  queue       : {Topology.BackOfficeQueue} (durable)");
Console.WriteLine("  binding key : tour.*        <-- wildcard");
Console.WriteLine($"  dead-letter : {Topology.DeadLetterExchange}   (invalid messages go here)");
Console.WriteLine("  manual ack  : ON            (nothing leaves the queue unhandled)");
Console.WriteLine("  => sees BOOKINGS *and* CANCELLATIONS.");
Console.WriteLine("========================================================");
Console.ResetColor();
Console.WriteLine(" [*] Waiting for messages. Press CTRL+C to exit.");

var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += async (_, ea) =>
{
    var json = Encoding.UTF8.GetString(ea.Body.ToArray());

    Booking? booking;
    try
    {
        booking = JsonSerializer.Deserialize<Booking>(json);
    }
    catch (JsonException)
    {
        booking = null;
    }

    // What "invalid" means here: it did not parse, or it parsed but the fields
    // the back office needs to write a ledger entry are not there.
    var invalid = booking is null
                  || string.IsNullOrWhiteSpace(booking.Name)
                  || string.IsNullOrWhiteSpace(booking.Tour);

    if (invalid)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($" [!] INVALID message on '{ea.RoutingKey}' - rejecting to {Topology.DeadLetterExchange}");
        Console.WriteLine($"     raw body: {json}");
        Console.ResetColor();
        Console.WriteLine();

        // requeue:false — do NOT put it back. The dead-letter exchange takes it,
        // the AdminApp logs it, and a human decides what to do.
        await channel.BasicRejectAsync(ea.DeliveryTag, requeue: false);
        return;
    }

    var verb = ea.RoutingKey == "tour.cancelled" ? "CANCELLATION" : "BOOKING";

    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine($" [x] routing key '{ea.RoutingKey}'");
    Console.ResetColor();
    Console.WriteLine($"     Ledger entry: {verb} - {booking!.Tour}");
    Console.WriteLine($"     Customer    : {booking.Name} <{booking.Email}>");
    Console.WriteLine($"     Recorded at : {booking.TimestampUtc:yyyy-MM-dd HH:mm:ss} UTC");
    Console.WriteLine();

    // Only now is the message gone from the queue.
    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
};

await channel.BasicConsumeAsync(queue: Topology.BackOfficeQueue, autoAck: false, consumer: consumer);

await Task.Delay(Timeout.Infinite);

record Booking(string Name, string Email, string Tour, string Action, DateTime TimestampUtc);
