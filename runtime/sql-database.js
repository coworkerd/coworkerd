import { DurableObject } from "cloudflare:workers";

export const DO_ALARM_HEADER = "x-coworkerd-do-alarm";

// The platform's own SQL Worker class: a tenant's database is one object of it (docs/compatibility.md, section 7).
// It answers the protocol workerd's own D1 wrapper speaks -- POST /query and /execute with {sql, params} or an array
// of them and a resultsFormat, answered with {success, meta, results} -- so the same statements reach it from a
// d1_databases Binding and from the HTTP SQL interface of /v1. Its storage is the object's own SQLite, which the
// node replicates and rebuilds like any other object's.
export class SqlDatabase extends DurableObject {
  static MAX_RESULT_BYTES = 7340032;
  async fetch(request) {
    // The node's own alarm request: a database has no alarm handler, so the alarm goes rather than being retried.
    const scheduled = request.headers.get(DO_ALARM_HEADER);
    if (scheduled !== null) {
      const held = await this.ctx.storage.getAlarm();
      if (held !== null && held !== undefined) await this.ctx.storage.deleteAlarm();
      return Response.json({ outcome: "fired", atUnixMs: 0, reason: "a database has no alarm() handler" });
    }
    const { pathname, searchParams } = new URL(request.url);
    const query = pathname === "/query", execute = pathname === "/execute";
    if (request.method !== "POST" || (!query && !execute)) {
      return Response.json({ error: "Not found" }, { status: 404 });
    }
    let body;
    try {
      body = await request.json();
    } catch {
      return Response.json({ error: "the body of a statement is JSON" }, { status: 400 });
    }
    const asked = searchParams.get("resultsFormat");
    const resultsFormat = asked === "ROWS_AND_COLUMNS" ? "ROWS_AND_COLUMNS" : asked === "NONE" ? "NONE" : "ARRAY_OF_OBJECTS";
    const encoder = new TextEncoder();
    const budget = { bytes: Array.isArray(body) ? 2 : 0, size: text => encoder.encode(text).byteLength };
    const deadline = this.constructor.wallDeadline;
    const failure = error => {
      if (error?.code === "SQL_CLIENT_DISCONNECTED") throw error;
      const message = String(error instanceof Error ? error.message : error);
      if (error?.code === "SQL_LIMIT_EXCEEDED") {
        if (!deadline) this.ctx.storage.sql.abortExecutionLimit();
        return { success: false, error: message, code: error.code, reason: message };
      }
      return { success: false, error: message };
    };
    const fail = (error, batch = false) => {
      const answer = failure(error);
      return Response.json(batch && !answer.code ? [answer] : answer, { status: answer.code ? 503 : 200 });
    };
    const one = (statement) => {
      try {
        return deadline ? this.ctx.storage.transactionSync(() => {
          const answer = this.runStatement(statement, resultsFormat, budget);
          this.checkDeadline();
          return answer;
        }) : this.runStatement(statement, resultsFormat, budget);
      } catch (error) {
        return failure(error);
      }
    };
    if (!Array.isArray(body)) {
      try {
        if (query && body && Object.hasOwn(body, "ingest")) return this.runIngest(body, budget);
        if (query || deadline) {
          return this.ctx.storage.transactionSync(() =>
            this.resultResponse(this.runStatement(body, resultsFormat, budget), budget));
        }
        return this.resultResponse(one(body), budget);
      } catch (error) { return fail(error); }
    }
    if (query) {
      // Include final serialization in the transaction: every quota error must roll the batch back.
      try {
        return this.ctx.storage.transactionSync(() => {
          const answers = body.map((statement, index) => {
            if (index) budget.bytes++;
            return this.runStatement(statement, resultsFormat, budget);
          });
          return this.resultResponse(answers, budget);
        });
      } catch (error) { return fail(error, true); }
    }
    // exec() keeps earlier writes. If its whole answer exceeds the cap, return only the error.
    const answers = [];
    for (const statement of body) {
      if (answers.length) budget.bytes++;
      const answer = one(statement);
      answers.push(answer);
      if (!answer.success) break;
      if (deadline) await this.ctx.storage.sync();
    }
    try { return this.resultResponse(answers, budget, answers.at(-1)?.code ? 503 : 200); }
    catch (error) { return fail(error, true); }
  }

  checkDeadline() {
    if (this.constructor.wallDeadline) this.ctx.storage.sql.checkExecutionDeadline();
  }

  resultTooLarge() {
    return new Error("D1_RESULT_TOO_LARGE: query response exceeds 7340032 bytes; use LIMIT to fetch results in smaller batches");
  }

  addResultBytes(budget, bytes) {
    if (budget.bytes + bytes > SqlDatabase.MAX_RESULT_BYTES) throw this.resultTooLarge();
    budget.bytes += bytes;
  }

  resultResponse(answer, budget, status = 200) {
    const json = JSON.stringify(answer);
    if (budget.size(json) > SqlDatabase.MAX_RESULT_BYTES) throw this.resultTooLarge();
    const response = new Response(json, { status, headers: { "content-type": "application/json" } });
    if (status === 200) this.checkDeadline();
    return response;
  }

  runIngest(statement, budget) {
    if (typeof statement.ingest !== "string" || Object.hasOwn(statement, "sql") || Object.hasOwn(statement, "params"))
      throw new Error("ingest must be a string, without sql or params, sent as a standalone /query object");
    return this.ctx.storage.transactionSync(() => {
      const sqlite = this.ctx.storage.sql;
      const counters = () => sqlite.exec("SELECT total_changes() AS changes, last_insert_rowid() AS last_row_id").toArray()[0];
      const before = counters(), sizeBefore = sqlite.databaseSize;
      const ingest = sqlite.ingest(statement.ingest);
      const after = counters(), sizeAfter = sqlite.databaseSize;
      const answer = { success: true, results: [], ingest, meta: {
        duration: 0, changes: after.changes - before.changes, last_row_id: after.last_row_id,
        changed_db: after.changes !== before.changes || after.last_row_id !== before.last_row_id || sizeAfter !== sizeBefore,
        size_after: sizeAfter, rows_read: ingest.rowsRead, rows_written: ingest.rowsWritten,
      } };
      this.addResultBytes(budget, budget.size(JSON.stringify(answer)));
      return this.resultResponse(answer, budget);
    });
  }

  runStatement(statement, resultsFormat, budget) {
    this.checkDeadline();
    if (statement && Object.hasOwn(statement, "ingest"))
      throw new Error("ingest is only accepted as a standalone /query object");
    const { sql, params = [] } = statement ?? {};
    if (typeof sql !== "string") throw new Error("a statement names its sql");
    const typeError = value => {
      let text;
      try { text = String(value); } catch { text = Object.prototype.toString.call(value); }
      return new Error("D1_TYPE_ERROR: Type '" + typeof value + "' not supported for value '" + text + "'");
    };
    if (!Array.isArray(params)) throw typeError(params);
    const bindings = params.map(value => {
      if (Array.isArray(value)) {
        if (!value.every(byte => Number.isInteger(byte) && byte >= 0 && byte <= 255)) throw typeError(value);
        return new Uint8Array(value);
      }
      if (value !== null && typeof value === "object") throw typeError(value);
      return value;
    });
    const sqlite = this.ctx.storage.sql;
    const counters = sqlite.prepare("SELECT total_changes() AS changes, last_insert_rowid() AS last_row_id");
    const sizeBefore = sqlite.databaseSize;
    const [[changesBefore, lastRowBefore]] = Array.from(counters().raw());
    const cursor = sqlite.prepare(sql)(...bindings);
    const columns = cursor.columnNames;
    const columnsFormat = resultsFormat === "ROWS_AND_COLUMNS";
    const rows = [];
    // Empty results include columns and framing. Meta is counted exactly once the cursor is drained.
    let resultBytes = budget.size(JSON.stringify(columnsFormat ? { columns, rows: [] } : []));
    const minimumEnvelope = budget.size(JSON.stringify({ success: true, results: null, meta: {} })) - 4;
    let overflow = budget.bytes + minimumEnvelope + resultBytes > SqlDatabase.MAX_RESULT_BYTES;
    for (const row of cursor.raw()) {
      // A fully consumed cursor releases its statement. Stop encoding, but keep stepping it to completion.
      if (overflow) continue;
      const values = row.map(value => value instanceof ArrayBuffer ? Array.from(new Uint8Array(value)) :
        ArrayBuffer.isView(value) ? Array.from(new Uint8Array(value.buffer, value.byteOffset, value.byteLength)) : value);
      const result = columnsFormat ? values : Object.fromEntries(columns.map((column, i) => [column, values[i]]));
      resultBytes += budget.size(JSON.stringify(result)) + (rows.length ? 1 : 0);
      if (budget.bytes + minimumEnvelope + resultBytes > SqlDatabase.MAX_RESULT_BYTES) {
        overflow = true;
        rows.length = 0;
      } else rows.push(result);
    }
    if (overflow) throw this.resultTooLarge();
    const results = columnsFormat ? { columns, rows } : rows;
    const [[changesAfter, lastRowAfter]] = Array.from(counters().raw());
    const sizeAfter = sqlite.databaseSize;
    const changes = changesAfter - changesBefore;
    const answer = {
      success: true,
      results,
      meta: {
        duration: 0,
        changes,
        last_row_id: lastRowAfter,
        changed_db: changes !== 0 || lastRowAfter !== lastRowBefore || sizeAfter !== sizeBefore,
        size_after: sizeAfter,
        rows_read: cursor.rowsRead,
        rows_written: cursor.rowsWritten,
      },
    };
    const envelopeBytes = budget.size(JSON.stringify({ ...answer, results: null })) - 4;
    this.addResultBytes(budget, envelopeBytes + resultBytes);
    return answer;
  }
}
