# durable-counter

The Durable Object example in the [quickstart](../../docs/guide/quickstart.md): plain JavaScript, no dependencies or build step. `Counter` stores one integer through the SQLite-backed key-value API. The Worker selects the object named `shared`; `/rpc` makes one public method call and one D1 query.

| Request | Answer on a fresh deployment |
| --- | --- |
| `GET /acme/durable-counter` | `{"count":0}` |
| `POST /acme/durable-counter` | `{"count":1}`; increments once per request |
| `GET /acme/durable-counter/rpc` | `{"count":1,"database":42}` after that increment |

With the quickstart's tenant `acme` and credential in `~/.cowctl/token`, from this directory:

```bash
cowctl init --tenant acme
cowctl d1 create counterdb
cowctl deploy
curl -X POST http://127.0.0.1:18080/acme/durable-counter
curl http://127.0.0.1:18080/acme/durable-counter
curl http://127.0.0.1:18080/acme/durable-counter/rpc
cowctl do list
cowctl do inspect Counter
```

The `Counter` class namespace is created by `migrations`; the database is created separately. Redeploying the Worker keeps both. POST is not idempotent: do not repeat an uncertain request blindly. The quickstart checks killing and restarting the single node with the same volume. Both reads must still contain `count: 1`; the [Durable Object guide](../../docs/guide/durable-objects.md) explains recovery and the accepted loss of an unuploaded tail when an owner and all confirming peers fail together.
