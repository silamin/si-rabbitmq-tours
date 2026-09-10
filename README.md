# Tour Booking — RabbitMQ Topic Exchange

Systemintegration, efterår 2026 · Day 3 hand-in · Amin Aouina

## The task

> Create a basic website with a form that allows you to book and also cancel tours.
> Use Topic Exchange to filter depending on routing key.
> The consumers can be console apps, that just log out some descriptive message.

## How it works

A web form publishes one message per submission to a **topic exchange** called
`tours.topic`. The radio button on the form is the only thing that decides the
routing key:

| form action | routing key |
|---|---|
| Book | `tour.booked` |
| Cancel | `tour.cancelled` |

Two console apps bind their own queue to that exchange — **and they bind
differently.** That difference is the whole exercise.

```
                                    binding "tour.booked"        ┌──────────────────┐
                                  ┌────────────────────────────► │  Email Service   │
                                  │      (exact match)           │  bookings only   │
   ┌───────────┐  tour.booked   ┌─┴─────────────┐                └──────────────────┘
   │  Web form │ ─────────────► │ tours.topic   │
   │  Book /   │  tour.cancelled│ TOPIC exchange│                ┌──────────────────┐
   │  Cancel   │ ─────────────► └─┬─────────────┘                │   Back Office    │
   └───────────┘                  │                              │ bookings AND     │
                                  └────────────────────────────► │ cancellations    │
                                    binding "tour.*"             └──────────────────┘
                                      (wildcard)
```

**Email Service** binds the exact key `tour.booked`. A cancellation is published
as `tour.cancelled`, which does not match, so the exchange never puts a copy in
that queue — the email service is never even told about it.

**Back Office** binds `tour.*`. In a topic binding `*` matches exactly one word,
so `tour.*` matches both `tour.booked` and `tour.cancelled`. The back office
keeps the books, so it has to see everything.

The publisher does not know either consumer exists. Adding a third listener is a
new binding, not a change to the web app. That is the point of routing through an
exchange instead of publishing to a queue.

## Project layout

```
src/TourBooking.Web    ASP.NET Core — the form, and the publisher
src/EmailService       console — binds "tour.booked"   (exact)
src/BackOffice         console — binds "tour.*"        (wildcard)
docker-compose.yml     a local RabbitMQ broker
PROOF.md               console output from an actual run
```

The two consumers are deliberately near-identical. Diff them: the only
meaningful difference is the `BindingKey` constant.

## Running it

Requires .NET 10 and Docker.

**1 — start the broker**

```bash
docker compose up -d
```

Management UI at <http://localhost:15672> (guest / guest) if you want to see the
exchange and its bindings.

**2 — start each consumer in its own terminal**

```bash
dotnet run --project src/EmailService
```

```bash
dotnet run --project src/BackOffice
```

**3 — start the web app in a third terminal**

```bash
dotnet run --project src/TourBooking.Web
```

Then open <http://localhost:5080>.

## What you should see

Fill in a name and email, choose a tour, leave **Book** selected, submit.
Both consoles print. Now switch to **Cancel** and submit again:

- **Back Office** prints a cancellation.
- **Email Service** prints nothing at all.

That silence is the demonstration. `PROOF.md` has the captured output from a
real run.

## Configuration

The broker host defaults to `localhost`. To point elsewhere:

- Web: `RabbitMq:HostName` in `appsettings.json`
- Consumers: the `RABBITMQ_HOST` environment variable

## Notes

- Queues are declared non-durable and messages are not persisted — this is a
  teaching demo, not a production booking system.
- `autoAck: true` on both consumers, for the same reason.
- The exchange is declared by all three apps. Declaring is idempotent, so
  whichever process starts first creates it and start order does not matter.

Template followed: [RabbitMQ tutorial 5 — Topics (.NET)](https://www.rabbitmq.com/tutorials/tutorial-five-dotnet.html)
