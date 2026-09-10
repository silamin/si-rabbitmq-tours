using System.Text.Json;
using RabbitMQ.Client;

// ---------------------------------------------------------------------------
//  Tour Booking – web front end
//
//  One form. Two possible routing keys. One topic exchange.
//
//      Book   -> routing key "tour.booked"
//      Cancel -> routing key "tour.cancelled"
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
        return Results.BadRequest(new { error = "Action must be 'book' or 'cancel'." });

    await publisher.PublishAsync(routingKey, request);

    return Results.Ok(new
    {
        exchange   = TourPublisher.ExchangeName,
        routingKey,
        message    = $"Published to '{TourPublisher.ExchangeName}' with routing key '{routingKey}'."
    });
});

app.Run();


/// <summary>The message that travels over the wire.</summary>
public record BookingRequest(string Name, string Email, string Tour, string Action);


/// <summary>
/// Owns a single long-lived connection + channel to RabbitMQ and publishes
/// booking events to the topic exchange.
/// </summary>
public sealed class TourPublisher : IAsyncDisposable
{
    public const string ExchangeName = "tours.topic";

    private readonly string _hostName;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IConnection? _connection;
    private IChannel? _channel;

    public TourPublisher(IConfiguration configuration)
        => _hostName = configuration["RabbitMq:HostName"] ?? "localhost";

    public async Task PublishAsync(string routingKey, BookingRequest request)
    {
        var channel = await GetChannelAsync();

        var body = JsonSerializer.SerializeToUtf8Bytes(new
        {
            request.Name,
            request.Email,
            request.Tour,
            request.Action,
            TimestampUtc = DateTime.UtcNow
        });

        await channel.BasicPublishAsync(
            exchange:   ExchangeName,
            routingKey: routingKey,
            body:       body);

        Console.WriteLine($" [x] Sent '{routingKey}' : {request.Name} / {request.Tour}");
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
            _channel    = await _connection.CreateChannelAsync();

            // Declaring is idempotent. Publisher and both consumers declare the
            // same exchange, so whichever process starts first creates it.
            await _channel.ExchangeDeclareAsync(
                exchange:   ExchangeName,
                type:       ExchangeType.Topic,
                durable:    false,
                autoDelete: false);

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
