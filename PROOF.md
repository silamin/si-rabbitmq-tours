# Proof of run

Captured from an actual run against `rabbitmq:3-management`, .NET 10.0.401.
Three messages published from the web form: **two bookings and one cancellation.**

## Email Service — bound to `tour.booked` (exact)

```
========================================================
  EMAIL SERVICE
  exchange    : tours.topic (topic)
  queue       : email-service
  binding key : tour.booked   <-- exact match only
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

## Back Office — bound to `tour.*` (wildcard)

```
========================================================
  BACK OFFICE
  exchange    : tours.topic (topic)
  queue       : back-office
  binding key : tour.*        <-- wildcard
  => sees BOOKINGS *and* CANCELLATIONS.
========================================================
 [*] Waiting for messages. Press CTRL+C to exit.
 [x] routing key 'tour.booked'
     Ledger entry: BOOKING - Odense H.C. Andersen Tour
     Customer    : Anna Jensen <anna.jensen@example.com>
     Recorded at : 2026-09-10 15:15:23 UTC

 [x] routing key 'tour.booked'
     Ledger entry: BOOKING - Skagen Lighthouse Trip
     Customer    : Peter Madsen <peter.madsen@example.com>
     Recorded at : 2026-09-10 15:15:24 UTC

 [x] routing key 'tour.cancelled'
     Ledger entry: CANCELLATION - Odense H.C. Andersen Tour
     Customer    : Anna Jensen <anna.jensen@example.com>
     Recorded at : 2026-09-10 15:15:25 UTC

```

## The result

| consumer | binding key | messages received |
|---|---|---|
| Email Service | `tour.booked` | **2** — the two bookings only |
| Back Office | `tour.*` | **3** — both bookings *and* the cancellation |

The cancellation never reached the Email Service. That is the topic exchange doing its job.

## Confirmed at the broker

Bindings on the exchange, read back from the RabbitMQ management API:

```
GET /api/exchanges/%2F/tours.topic/bindings/source

  destination: "back-office"     routing_key: "tour.*"
  destination: "email-service"   routing_key: "tour.booked"

GET /api/queues/%2F

  back-office     delivered = 3
  email-service   delivered = 2
```
