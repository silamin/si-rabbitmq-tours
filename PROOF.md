# Proof of run

Day 5 hand-in. Captured from an **actual run** against `rabbitmq:3-management`
(RabbitMQ 3.12.1) on .NET 10.0.401, 18 September 2026.

Everything below is console output and RabbitMQ management-API output copied
verbatim from that run. Nothing here is illustrative.

---

## Part 1 — normal routing still works (day 3 behaviour, unchanged)

Three messages from the web form: two bookings and one cancellation.

### Email Service — bound to `tour.booked` (exact)

```
========================================================
  EMAIL SERVICE
  exchange    : tours.topic (topic, durable)
  queue       : email-service (durable)
  binding key : tour.booked   <-- exact match only
  dead-letter : tours.dlx   (invalid messages go here)
  manual ack  : ON
  => sees BOOKINGS only. Cancellations never arrive here.
========================================================
 [*] Waiting for messages. Press CTRL+C to exit.
 [x] routing key 'tour.booked'
     Sending confirmation e-mail to anna.jensen@example.com
     "Hi Anna Jensen, your booking for 'Odense H.C. Andersen Tour' is confirmed."

 [x] routing key 'tour.booked'
     Sending confirmation e-mail to peter.madsen@example.com
     "Hi Peter Madsen, your booking for 'Skagen Lighthouse Trip' is confirmed."

```

### Back Office — bound to `tour.*` (wildcard)

```
 [x] routing key 'tour.booked'
     Ledger entry: BOOKING - Odense H.C. Andersen Tour
     Customer    : Anna Jensen <anna.jensen@example.com>
     Recorded at : 2026-09-18 05:08:22 UTC

 [x] routing key 'tour.booked'
     Ledger entry: BOOKING - Skagen Lighthouse Trip
     Customer    : Peter Madsen <peter.madsen@example.com>
     Recorded at : 2026-09-18 05:08:23 UTC

 [x] routing key 'tour.cancelled'
     Ledger entry: CANCELLATION - Odense H.C. Andersen Tour
     Customer    : Anna Jensen <anna.jensen@example.com>
     Recorded at : 2026-09-18 05:08:24 UTC

```

The cancellation never reached the Email Service. That is the topic exchange
doing its job.

---

## Part 2 — an INVALID message is captured, not dropped (task point 2)

A fourth message was published with the **right routing key** `tour.booked` but
a body that is not a booking. Both consumers received it, both failed to make
sense of it, and both rejected it with `requeue: false`.

```
 [!] INVALID message on 'tour.booked' - rejecting to tours.dlx
     raw body: {"nonsense":true,"note":"not a booking"}
```
*(printed by Email Service and by Back Office — both got a copy)*

---

## Part 3 — an UNROUTABLE message is captured, not dropped (task point 2)

A fifth message was published with the routing key `noroute.test`, which matches
**no binding at all**. RabbitMQ would normally discard it in silence. The
alternate exchange on `tours.topic` caught it.

---

## Part 4 — the AdminApp log (task point 3)

This is the whole AdminApp console for that run. Three entries: the invalid
message twice (once per rejecting consumer) and the unroutable one.

```
========================================================
  ADMIN APP  -  undeliverable & invalid message log
  queue : undeliverable      (alternate exchange)
  queue : invalid-messages   (dead-letter exchange)
========================================================
 [*] Waiting. Every line below is a message that did not make it.

 [#1] 2026-09-18 05:08:25 UTC   source: invalid-messages
        reason      : REJECTED by consumer on queue 'back-office' (reason: rejected) - payload could not be processed
        routing key : tour.booked
        body        : {"nonsense":true,"note":"not a booking"}

 [#2] 2026-09-18 05:08:25 UTC   source: invalid-messages
        reason      : REJECTED by consumer on queue 'email-service' (reason: rejected) - payload could not be processed
        routing key : tour.booked
        body        : {"nonsense":true,"note":"not a booking"}

 [#3] 2026-09-18 05:08:26 UTC   source: undeliverable
        reason      : UNROUTABLE - routing key matched no binding on tours.topic
        routing key : noroute.test
        body        : {"note":"nobody is bound to this key"}
```

The `reason` line for #1 and #2 is not guessed — it is read out of the `x-death`
header that the broker stamps on a dead-lettered message, which is why it can
name the queue the message was rejected on.

---

## Part 5 — NOTHING IS LOST ACROSS A BROKER RESTART (task point 1)

The strongest test of "none of the messages to the BackOffice are lost": publish
with **both consumers shut down**, then **restart the broker**, then bring the
consumers back.

**Two bookings published while the consumers were down.** Then:

```
$ rabbitmqctl stop_app && rabbitmqctl start_app

--- AFTER broker stop_app / start_app, consumers still down:
    back-office        messages = 2
    email-service      messages = 2
    invalid-messages   messages = 0
    undeliverable      messages = 0
```

Both messages were still there. Durable queue + persistent delivery mode is what
makes that true; either one alone is not enough.

**Then the consumers were restarted** and the queues drained — the bookings were
processed, late but not lost:

```
--- after restarting the consumers:
    back-office        messages = 0
    email-service      messages = 0

 [x] routing key 'tour.booked'
     Ledger entry: BOOKING - Aarhus Old Town
     Customer    : Restart Survivor <survivor@example.com>
     Recorded at : 2026-09-18 05:08:59 UTC

 [x] routing key 'tour.booked'
     Ledger entry: BOOKING - Mons Klint Day Trip
     Customer    : Second Survivor <survivor2@example.com>
     Recorded at : 2026-09-18 05:09:21 UTC
```

---

## Part 6 — confirmed at the broker (task point 4)

Read back from the RabbitMQ management API at the end of part 1.

```
GET /api/exchanges/%2F/tours.topic

  durable: True   type: topic   arguments: {'alternate-exchange': 'tours.unroutable'}

GET /api/exchanges/%2F/tours.topic/bindings/source

  destination: back-office        routing_key: 'tour.*'
  destination: email-service      routing_key: 'tour.booked'

GET /api/queues/%2F

  back-office        durable=True  messages=0  delivered=4  args={'x-dead-letter-exchange': 'tours.dlx'}
  email-service      durable=True  messages=0  delivered=3  args={'x-dead-letter-exchange': 'tours.dlx'}
  invalid-messages   durable=True  messages=0  delivered=2  args={}
  undeliverable      durable=True  messages=0  delivered=1  args={}
```

Reading that table:

| queue | delivered | why that number |
|---|---|---|
| `back-office` | 4 | 2 bookings + 1 cancellation + 1 invalid |
| `email-service` | 3 | 2 bookings + 1 invalid (no cancellation — wrong key) |
| `invalid-messages` | 2 | the one invalid message, rejected by **both** consumers |
| `undeliverable` | 1 | the one unroutable message |

Five messages were published. Five were accounted for: none reached a dead end
and none was silently dropped.
