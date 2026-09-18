using RabbitMQ.Client;

/// <summary>
/// The one description of the broker topology, shared by all four apps.
///
/// Declaring is idempotent, so every app declares the whole thing and start
/// order does not matter. Keeping it in one file means the publisher and the
/// consumers can never disagree about durability or arguments — a disagreement
/// there is a PRECONDITION_FAILED at run time, not a compile error.
///
///                                 ┌──────────────┐  tour.booked   ┌───────────────┐
///                                 │              ├───────────────►│ email-service │
///   web form ──publish(confirm)──►│ tours.topic  │                └──────┬────────┘
///        persistent, durable      │  (topic,     │  tour.*        ┌──────┴────────┐
///                                 │   durable)   ├───────────────►│  back-office  │
///                                 └──────┬───────┘                └──────┬────────┘
///                        no binding matched                  rejected (requeue:false)
///                                        │                               │
///                             alternate-exchange                  dead-letter-exchange
///                                        ▼                               ▼
///                              tours.unroutable (fanout)         tours.dlx (fanout)
///                                        │                               │
///                                        ▼                               ▼
///                                 undeliverable                  invalid-messages
///                                        └──────────► AdminApp ◄─────────┘
/// </summary>
public static class Topology
{
    public const string Exchange           = "tours.topic";
    public const string AlternateExchange  = "tours.unroutable";
    public const string DeadLetterExchange = "tours.dlx";

    public const string EmailQueue         = "email-service";
    public const string BackOfficeQueue    = "back-office";
    public const string UndeliverableQueue = "undeliverable";
    public const string InvalidQueue       = "invalid-messages";

    public static async Task DeclareAsync(IChannel channel)
    {
        // --- the two safety-net exchanges, declared first so the main exchange
        //     can point at one of them -------------------------------------
        await channel.ExchangeDeclareAsync(
            AlternateExchange, ExchangeType.Fanout, durable: true, autoDelete: false);

        await channel.ExchangeDeclareAsync(
            DeadLetterExchange, ExchangeType.Fanout, durable: true, autoDelete: false);

        // --- the main exchange ------------------------------------------------
        // alternate-exchange: if a published message matches NO binding, RabbitMQ
        // normally drops it without a word. With an alternate exchange set, it is
        // handed here instead. Undeliverable is now captured, not lost.
        await channel.ExchangeDeclareAsync(
            Exchange, ExchangeType.Topic, durable: true, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["alternate-exchange"] = AlternateExchange
            });

        // --- the capture queues -----------------------------------------------
        await channel.QueueDeclareAsync(UndeliverableQueue, durable: true, exclusive: false, autoDelete: false);
        await channel.QueueBindAsync(UndeliverableQueue, AlternateExchange, routingKey: "");

        await channel.QueueDeclareAsync(InvalidQueue, durable: true, exclusive: false, autoDelete: false);
        await channel.QueueBindAsync(InvalidQueue, DeadLetterExchange, routingKey: "");

        // --- the two business queues -------------------------------------------
        // durable  : the queue survives a broker restart
        // x-dead-letter-exchange : a message this consumer rejects with
        //            requeue:false is republished to tours.dlx instead of being
        //            discarded. Invalid is now captured, not lost.
        var businessArgs = new Dictionary<string, object?>
        {
            ["x-dead-letter-exchange"] = DeadLetterExchange
        };

        await channel.QueueDeclareAsync(EmailQueue, durable: true, exclusive: false, autoDelete: false, arguments: businessArgs);
        await channel.QueueBindAsync(EmailQueue, Exchange, routingKey: "tour.booked");

        await channel.QueueDeclareAsync(BackOfficeQueue, durable: true, exclusive: false, autoDelete: false, arguments: businessArgs);
        await channel.QueueBindAsync(BackOfficeQueue, Exchange, routingKey: "tour.*");
    }
}
