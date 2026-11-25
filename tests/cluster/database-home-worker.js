// The driver prepends the pinned runtime's SqlDatabase class, DurableObject import and backend flag.
const statements = {
  cold: ['SELECT 1 AS value'],
  create: ['CREATE TABLE IF NOT EXISTS sample(id INTEGER PRIMARY KEY, value INTEGER NOT NULL)'],
  read: ['SELECT value FROM sample WHERE id = 1'],
  insert: ['INSERT INTO sample VALUES (?, 1)'],
  count: ['SELECT count(*) AS value FROM sample'],
  createLarge: ['CREATE TABLE large_result(value TEXT NOT NULL)'],
  seedLarge: ["INSERT INTO large_result VALUES (replace(hex(zeroblob(522240)), '0', 'x'))"],
  large: ['SELECT value FROM large_result'],
  noop: ['SELECT NULL AS value WHERE 0'],
};
const writes = new Set(['create', 'insert', 'createLarge', 'seedLarge']);
let copy;

export class BenchmarkDatabase extends SqlDatabase {
  async fetch(request) {
    const response = await super.fetch(request);
    if (request.headers.has('x-benchmark-identify-copy')) {
      copy ??= crypto.randomUUID();
      response.headers.set('x-benchmark-copy', copy);
    }
    return response;
  }
}

export class BenchmarkNoop extends DurableObject {
  fetch() {
    return Response.json([{ value: 1 }]);
  }
}

function rows(result) {
  const { columns, rows } = result.results;
  return rows.map(row => Object.fromEntries(columns.map((column, i) => [column, row[i]])));
}

export default { async fetch(request, { DB, NOOP }) {
  const url = new URL(request.url);
  const operation = url.pathname.split('/').at(-1);
  if (operation === 'ping') return Response.json('ready');
  if (operation === 'worker-only') return Response.json([{ value: 1 }]);
  if (operation === 'object-noop') {
    const target = NOOP.get(NOOP.idFromName('control'));
    const response = await target.fetch('http://control/noop', { method: 'POST' });
    // Decode and serialize as the SQL paths do in their calling Worker.
    return Response.json(await response.json(), { status: response.status });
  }

  const empty = operation === 'emptyBatch';
  const [sql] = statements[operation] ?? [];
  const params = operation === 'insert' ? [Number(url.searchParams.get('id'))] : [];
  const write = writes.has(operation);
  if (useD1Binding) {
    if (empty) {
      // The pinned D1 HTTP wrapper sends [], decodes [] and dereferences the absent first result.
      try {
        await DB.batch([]);
      } catch (error) {
        if (error instanceof TypeError && error.message.includes('success')) {
          return Response.json('empty batch reply decoded');
        }
        throw error;
      }
      throw new Error('Expected the pinned D1 wrapper to reject the empty reply');
    }
    const statement = DB.prepare(sql).bind(...params);
    const result = write ? await statement.run() : await statement.all();
    return Response.json(write ? result.meta.changes : result.results);
  }

  const target = DB.get(DB.idFromName(url.searchParams.get('key') ?? 'db'));
  const path = write ? '/execute?resultsFormat=NONE' : '/query?resultsFormat=ROWS_AND_COLUMNS';
  const identify = operation === 'cold';
  const response = await target.fetch('http://d1' + path, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(identify && { 'x-benchmark-identify-copy': '1' }),
    },
    body: JSON.stringify(empty ? [] : { sql, params }),
  });
  const result = await response.json();
  if (empty && response.ok && Array.isArray(result) && result.length === 0) {
    return Response.json('empty batch reply decoded');
  }
  if (!response.ok || !result.success) return Response.json(result, { status: 502 });
  const value = write ? result.meta.changes : rows(result);
  const headers = identify ? { 'x-benchmark-copy': response.headers.get('x-benchmark-copy') } : {};
  return Response.json(value, { headers });
} };
