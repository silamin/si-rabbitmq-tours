# Tour Booking — RabbitMQ Topic Exchange

Systemintegration, efterår 2026 · Day 3 + **Day 5** hand-in · Amin Aouina

## The task

**Day 3 — where this started**

> Create a basic website with a form that allows you to book and also cancel tours.
> Use Topic Exchange to filter depending on routing key.
> The consumers can be console apps, that just log out some descriptive message.

**Day 5 — what this hand-in adds**

> 1. Make sure that none of the messages to the BackOffice are lost
> 2. Capture undeliverable and invalid messages
> 3. Add a new AdminApp, that shows a log of these messages
> 4. Test the system (e.g. Management UI)
> 5. Add to the current diagram to show the new functionality

| point | where it is | proof |
|---|---|---|
| 1 — nothing lost | durable exchange + durable queues, `DeliveryModes.Persistent`, publisher confirms, `autoAck: false`, `BasicQos(1)` | `PROOF.md` part 5 — survives a broker restart |
| 2 — undeliverable | **alternate exchange** `tours.unroutable` → queue `undeliverable` | `PROOF.md` part 3 |
| 2 — invalid | **dead-letter exchange** `tours.dlx` → queue `invalid-messages` | `PROOF.md` part 2 |
| 3 — AdminApp | `src/AdminApp`, consumes both capture queues | `PROOF.md` part 4 |
| 4 — tested | management API read back after the run | `PROOF.md` part 6 |
| 5 — diagram | below | — |

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
                                                     binding "tour.booked"   ┌──────────────────┐
                                                   ┌────────────────────────►│  Email Service   │
                                                   │      (exact match)      │  bookings only   │
  ┌───────────┐                    ┌───────────────┴──┐                      └────────┬─────────┘
  │  Web form │  tour.booked       │   tours.topic    │                               │
  │  Book /   │ ──────────────────►│                  │  binding "tour.*"    ┌────────┴─────────┐
  │  Cancel   │  tour.cancelled    │  TOPIC, durable  ├─────────────────────►│   Back Office    │
  └───────────┘ ──────────────────►│                  │     (wildcard)       │ bookings AND     │
   persistent                      │  alternate-      │                      │ cancellations    │
   + publisher                     │  exchange ───┐   │                      └────────┬─────────┘
   confirms                        └──────────────┼───┘                               │
                                                  │                    reject(requeue:false)
                   routing key matched NO binding │                       on a payload it
                      (would normally be DROPPED) │                    cannot process │
                                                  ▼                                   ▼
                                      ┌───────────────────┐               ┌───────────────────┐
                                      │ tours.unroutable  │               │    tours.dlx      │
                                      │     (fanout)      │               │     (fanout)      │
                                      └─────────┬─────────┘               └─────────┬─────────┘
                                                ▼                                   ▼
                                      ┌───────────────────┐               ┌───────────────────┐
                                      │   undeliverable   │               │ invalid-messages  │
                                      │   (durable queue) │               │  (durable queue)  │
                                      └─────────┬─────────┘               └─────────┬─────────┘
                                                │                                   │
                                                └──────────►┌──────────┐◄───────────┘
                                                            │ AdminApp │
                                                            │   log    │
                                                            └──────────┘
```

**The top half — form, topic exchange, two bindings — is day 3 and is unchanged.
The two branches falling out of it are day 5: everything that would otherwise
have been dropped in silence now ends up in the AdminApp log.**

### Why two separate safety nets, and not one

They catch two different failures, and only one of them is a dead-letter case.

| | **alternate exchange** | **dead-letter exchange** |
|---|---|---|
| catches | a message the **exchange** could not route — no binding matched | a message a **consumer** received and rejected with `requeue: false` |
| failed where | before any queue | after delivery |
| default behaviour without it | silently discarded | silently discarded |
| example here | routing key `noroute.test` | a body that is not a booking |

A dead-letter exchange cannot catch an unroutable message, because an
unroutable message never reaches a queue to be dead-lettered from. That is why
both are needed.

### Why "not lost" takes four things, not one

- **durable queue** — the queue definition survives a broker restart
- **persistent messages** — the messages *in* it survive too. A durable queue full of
  non-persistent messages is an empty queue after a restart
- **publisher confirms** — the publisher does not report success until the broker
  has taken responsibility. Without them a message can be lost between the app and
  a broker that is down, and nothing anywhere would say so
- **`autoAck: false`** — a message leaves the queue when the consumer says it handled
  it, not when the broker handed it over. Kill a consumer mid-message and the
  broker redelivers it

Drop any one of the four and there is a way to lose a message.

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
src/AdminApp           console — the log of undeliverable + invalid messages
src/Shared/Topology.cs the ONE description of exchanges, queues and bindings,
                       compiled into all four apps
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

```bash
dotnet run --project src/AdminApp
```

**3 — start the web app in a fourth terminal**

```bash
dotnet run --project src/TourBooking.Web
```

Then open <http://localhost:5080>.

## What you should see

Fill in a name and email, choose a tour, leave **Book** selected, submit.
Both consoles print. Now switch to **Cancel** and submit again:

- **Back Office** prints a cancellation.
- **Email Service** prints nothing at all.

That silence is the demonstration.

Then use the two **failure tests** on the form:

- **Send an invalid payload** — right routing key, wrong body. Both consumers
  print `[!] INVALID` and reject it; the **AdminApp** logs it twice, once per
  rejecting consumer, with the queue name read out of the broker's `x-death`
  header.
- **Send an unroutable key** — publishes with `noroute.test`. No consumer prints
  anything, because no binding matches. The **AdminApp** logs it as
  `UNROUTABLE`. Without the alternate exchange this message would have vanished
  without a trace.

`PROOF.md` has the captured output from a real run of all of this, including a
broker restart with messages still on the queues.

## Configuration

The broker host defaults to `localhost`. To point elsewhere:

- Web: `RabbitMq:HostName` in `appsettings.json`
- Consumers: the `RABBITMQ_HOST` environment variable

## Notes

- Everything is declared by every app through `src/Shared/Topology.cs`.
  Declaring is idempotent, so whichever process starts first creates it and
  start order does not matter — and because there is only one copy of the
  declarations, the apps cannot disagree about durability or arguments. A
  disagreement there is a `PRECONDITION_FAILED` at run time, not a compile error.
- ⚠ **Day 3 left a non-durable `tours.topic` and non-durable queues behind.** The
  day-5 topology is durable, and RabbitMQ will not redeclare an existing entity
  with different properties. If you still have the old broker state, reset it
  first: `docker compose down -v && docker compose up -d`.
- `prefetchCount: 1` on both consumers: do not push a burst at a consumer that
  might die holding it.

Template followed: [RabbitMQ tutorial 5 — Topics (.NET)](https://www.rabbitmq.com/tutorials/tutorial-five-dotnet.html)
