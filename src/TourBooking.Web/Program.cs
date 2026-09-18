using System.Text.Json;
using RabbitMQ.Client;

// ---------------------------------------------------------------------------
//  Tour Booking – web front end
//
//  One form. Several possible routing keys. One topic exchange.
//
//      Book       -> routing key "tour.booked"
//      Cancel     -> routing key "tour.cancelled"
//      Invalid    -> routing key "tour.booked", but a payload no consumer can
//                    process. Demonstrates the DEAD-LETTER path.
//      Unroutable -> routing key "noroute.test", which matches no binding.
//                    Demonstrates the ALTERNATE-EXCHANGE path.
//
//  The web app does not know, and must not know, who is listening. It publishes
//  to the exchange and the *bindings* decide who gets a copy. That is the whole
//  point of a Topic Exchange: the publisher is decoupled from the consumers.
// ---------------------------------------------------------------------------

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<TourPublisher>();

var app = builder.Build();

app.UseDefaultFiles();   // serves wwwroot/index.html at "/"
app.UseStaticFiles();

app.MapPost("/book", async (BookingRequest request, TourPublisher publisher) =>
{
    // The two test actions deliberately skip validation — the point of them is
    // to put something on the exchange that the rest of the system has to cope
    // with. Everything else is validated here, at the edge.
    if (request.Action is "invalid")
    {
        // Right key, wrong shape. It WILL be routed to both consumers, and both
        // will reject it to the dead-letter exchange.
        await publisher.PublishRawAsync("tour.booked", """{"nonsense":true,"note":"not a booking"}""");
        return Results.Ok(new
        {
            exchange   = Topology.Exchange,
            routingKey = "tour.booked",
            message    = "Published an INVALID payload. Consumers will reject it to tours.dlx -> AdminApp."
        });
    }

    if (request.Action is "unroutable")
    {
        // No binding matches "noroute.test". Without an alternate exchange this
        // message would be silently dropped by the broker.
        await publisher.PublishRawAsync("noroute.test", """{"note":"nobody is bound to this key"}""");
        return Results.Ok(new
        {
            exchange   = Topology.Exchange,
            routingKey = "noroute.test",
            message    = "Published an UNROUTABLE message. tours.unroutable -> undeliverable -> AdminApp."
        });
    }

    if (string.IsNullOrWhiteSpace(request.Name))
        return Results.BadRequest(new { error = "Name is required." });
    if (string.IsNullOrWhiteSpace(request.Email))
        return Results.BadRequest(new { error = "Email is required." });
    if (string.IsNullOrWhiteSpace(request.Tour))
        return Results.BadRequest(new { error = "Please choose a tour." });

    // The radio button is the only thing that decides the routing key.
    var routingKey = request.Action switch
    {
        "book"   => "tour.booked",
        "cancel" => "tour.cancelled",
        _        => null
    };

    if (routingKey is null)
        return Results.BadRequest(new { error = "Unknown action." });

    await publisher.PublishAsync(routingKey, request);

    return Results.Ok(new
    {
        exchange   = Topology.Exchange,
        routingKey,
        message    = $"Published to '{Topology.Exchange}' with routing key '{routingKey}' (persistent, confirmed by the broker)."
    });
});

app.Run();


/// <summary>The message that travels over the wire.</summary>
public record BookingRequest(string Name, string Email, string Tour, string Action);


/// <summary>
/// Owns a single long-lived connection + channel to RabbitMQ and publishes
/// booking events to the topic exchange.
///
/// DAY 5 — "make sure that none of the messages to the BackOffice are lost"
/// starts HERE, before the message ever reaches a queue:
///
///   * the channel is opened with PUBLISHER CONFIRMS, so BasicPublishAsync does
///     not return until the broker has taken responsibility for the message. A
///     broker that is down or out of disk now throws instead of quietly
///     accepting a message into nothing.
///   * DeliveryMode.Persistent writes the message to disk, so it survives a
///     broker restart while it sits on the durable queue.
///
/// A durable queue holding non-persistent messages is still an empty queue
/// after a restart — both halves are needed, which is why they are set here and
/// in Topology together.
/// </summary>
public sealed class TourPublisher : IAsyncDisposable
{
    private readonly string _hostName;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IConnection? _connection;
    private IChannel? _channel;

    public TourPublisher(IConfiguration configuration)
        => _hostName = configuration["RabbitMq:HostName"] ?? "localhost";

    public Task PublishAsync(string routingKey, BookingRequest request)
    {
        var json = JsonSerializer.Serialize(new
        {
            request.Name,
            request.Email,
            request.Tour,
            request.Action,
            TimestampUtc = DateTime.UtcNow
        });

        return PublishRawAsync(routingKey, json);
    }

    public async Task PublishRawAsync(string routingKey, string json)
    {
        var channel = await GetChannelAsync();

        var props = new BasicProperties
        {
            // On disk, not just in memory.
            DeliveryMode = DeliveryModes.Persistent,
            ContentType  = "application/json"
        };

        // With confirms enabled this await completes only when the broker has
        // acked the message. If it nacks, this throws — and the HTTP caller
        // finds out, instead of the message disappearing.
        await channel.BasicPublishAsync(
            exchange:   Topology.Exchange,
            routingKey: routingKey,
            mandatory:  false,          // the alternate exchange handles unroutable
            basicProperties: props,
            body:       System.Text.Encoding.UTF8.GetBytes(json));

        Console.WriteLine($" [x] Sent '{routingKey}' (persistent, broker-confirmed)");
    }

    private async Task<IChannel> GetChannelAsync()
    {
        if (_channel is { IsOpen: true }) return _channel;

        await _gate.WaitAsync();
        try
        {
            if (_channel is { IsOpen: true }) return _channel;

            var factory = new ConnectionFactory { HostName = _hostName };
            _connection = await factory.CreateConnectionAsync();

            _channel = await _connection.CreateChannelAsync(
                new CreateChannelOptions(
                    publisherConfirmationsEnabled:        true,
                    publisherConfirmationTrackingEnabled: true));

            // Declaring is idempotent. Publisher and all consumers declare the
            // same topology, so whichever process starts first creates it.
            await Topology.DeclareAsync(_channel);

            return _channel;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel    is not null) await _channel.DisposeAsync();
        if (_connection is not null) await _connection.DisposeAsync();
        _gate.Dispose();
    }
}
