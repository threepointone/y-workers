import * as Y from "yjs";
import * as encoding from "lib0/encoding.js";
import * as decoding from "lib0/decoding.js";

const PREFERRED_TRIM_SIZE = 300;

const BINARY_BITS_32 = 0xffffffff;

type StorageKey = Array<string | number>;

/**
 * Keys are arrays of strings + numbers, so we keep a
 * couple of helpers to encode/decode them.
 */
const keyEncoding = {
  encode(arr: StorageKey) {
    let resultArr = [];
    for (const item of arr) {
      resultArr.push(
        typeof item === "string" ? `"${item}"` : `${item}`.padStart(9, "0")
      );
    }
    return resultArr.join("#");
  },
  decode(str: string): StorageKey {
    return str
      .split("#")
      .map((el) => (el.startsWith('"') ? JSON.parse(el) : parseInt(el, 10)));
  },
};

/**
 * A key + value pair.
 */
type Datum = {
  key: (string | number)[];
  value: Uint8Array;
};

/**
 * This helper method returns `null` if the key is not found.
 */
async function levelGet(
  db: DurableObjectStorage,
  key: (string | number)[]
): Promise<Uint8Array | null> {
  let res;
  res = await db.get(keyEncoding.encode(key));
  if (res === undefined) {
    return null;
  }

  return res as Uint8Array;
}

/**
 * Set a key + value in storage
 */
async function levelPut(
  db: DurableObjectStorage,
  key: (string | number)[],
  val: Uint8Array
): Promise<void> {
  return db.put(keyEncoding.encode(key), val);
}

/**
 * A "bulkier" implementation of getting keys and/or values.
 */
async function getLevelBulkData(
  db: DurableObjectStorage,
  opts: {
    gte: Array<string | number>;
    lt: Array<string | number>;
    keys: boolean;
    values: boolean;
    reverse?: boolean;
    limit?: number;
  }
): Promise<Datum[]> {
  const res = await db.list({
    start: keyEncoding.encode(opts.gte),
    end: keyEncoding.encode(opts.lt),
    reverse: opts.reverse,
    limit: opts.limit,
  });

  const arr = [];
  for (const [key, value] of res.entries()) {
    const ret = {} as Datum;
    if (opts.keys) {
      ret.key = keyEncoding.decode(key);
    }
    if (opts.values) {
      ret.value = value as Uint8Array;
    }

    arr.push(ret);
  }
  return arr;
}

/**
 * Get all document updates for a specific document.
 */
async function getLevelUpdates(
  db: DurableObjectStorage,
  docName: string,
  opts: {
    values: boolean;
    keys: boolean;
    reverse?: boolean;
    limit?: number;
  } = {
    values: true,
    keys: false,
  }
): Promise<Array<Datum>> {
  return getLevelBulkData(db, {
    gte: createDocumentUpdateKey(docName, 0),
    lt: createDocumentUpdateKey(docName, BINARY_BITS_32),
    ...opts,
  });
}

/**
 * Get the current document 'clock' / counter
 */
async function getCurrentUpdateClock(
  db: DurableObjectStorage,
  docName: string
): Promise<number> {
  return getLevelUpdates(db, docName, {
    keys: true,
    values: false,
    reverse: true,
    limit: 1,
  }).then((datums) => {
    if (datums.length === 0) {
      return -1;
    } else {
      const ret = datums[0].key[3];
      if (typeof ret !== "number") {
        throw new Error("Expected number, got " + typeof ret);
      }
      return ret;
    }
  });
}

/**
 * @param {any} db
 * @param {Array<string|number>} gte Greater than or equal
 * @param {Array<string|number>} lt lower than (not equal)
 * @return {Promise<void>}
 */
async function clearRange(
  db: DurableObjectStorage,
  gte: Array<string | number>,
  lt: Array<string | number>
): Promise<void> {
  const datums = await getLevelBulkData(db, {
    values: false,
    keys: true,
    gte,
    lt,
  });
  if (datums.length > 128) {
    throw new Error("Too many keys to clear");
  } else {
    await db.delete(datums.map((d) => keyEncoding.encode(d.key)));
  }
}

/**
 * @param {any} db
 * @param {string} docName
 * @param {number} from Greater than or equal
 * @param {number} to lower than (not equal)
 * @return {Promise<void>}
 */
async function clearUpdatesRange(
  db: DurableObjectStorage,
  docName: string,
  from: number,
  to: number
): Promise<void> {
  return clearRange(
    db,
    createDocumentUpdateKey(docName, from),
    createDocumentUpdateKey(docName, to)
  );
}

/**
 * Create a unique key for a update message.
 * We encode the result using `keyEncoding` which expects an array.
 */
function createDocumentUpdateKey(
  docName: string,
  clock: number
): Array<string | number> {
  return ["v1", docName, "update", clock];
}

/**
 * @param {string} docName
 * @param {string} metaKey
 */
// const createDocumentMetaKey = (docName: string, metaKey: string) => [
//   "v1",
//   docName,
//   "meta",
//   metaKey,
// ];

/**
 * @param {string} docName
 */
// const createDocumentMetaEndKey = (docName: string) => ["v1", docName, "metb"]; // simple trick

/**
 * We have a separate state vector key so we can iterate efficiently over all documents
 * (This might make more sense for level db style databases, but not so much for DOs)
 * @param {string} docName
 */
function createDocumentStateVectorKey(docName: string) {
  return ["v1_sv", docName];
}

/**
 * @param {string} docName
 */
// const createDocumentFirstKey = (docName: string) => ["v1", docName];

/**
 * We use this key as the upper limit of all keys that can be written.
 * Make sure that all document keys are smaller! Strings are encoded using varLength string encoding,
 * so we need to make sure that this key has the biggest size!
 *
 * @param {string} docName
 */
// const createDocumentLastKey = (docName: string) => ["v1", docName, "zzzzzzz"];

// const emptyStateVector = (() => Y.encodeStateVector(new Y.Doc()))()

/**
 * For now this is a helper method that creates a Y.Doc and then re-encodes a document update.
 * In the future this will be handled by Yjs without creating a Y.Doc (constant memory consumption).
 *
 * @param {Array<Uint8Array>} updates
 * @return {{update:Uint8Array, sv: Uint8Array}}
 */
function mergeUpdates(updates: Array<Uint8Array>): {
  update: Uint8Array;
  sv: Uint8Array;
} {
  const ydoc = new Y.Doc();
  ydoc.transact(() => {
    for (let i = 0; i < updates.length; i++) {
      Y.applyUpdate(ydoc, updates[i]);
    }
  });
  return { update: Y.encodeStateAsUpdate(ydoc), sv: Y.encodeStateVector(ydoc) };
}

async function writeStateVector(
  db: DurableObjectStorage,
  docName: string,
  sv: Uint8Array, // state vector
  clock: number // current clock of the document so we can determine when this statevector was created
) {
  const encoder = encoding.createEncoder();
  encoding.writeVarUint(encoder, clock);
  encoding.writeVarUint8Array(encoder, sv);
  await levelPut(
    db,
    createDocumentStateVectorKey(docName),
    encoding.toUint8Array(encoder)
  );
}

function decodeLeveldbStateVector(buf: Uint8Array): {
  sv: Uint8Array;
  clock: number;
} {
  const decoder = decoding.createDecoder(buf);
  const clock = decoding.readVarUint(decoder);
  const sv = decoding.readVarUint8Array(decoder);
  return { sv, clock };
}

async function readStateVector(db: DurableObjectStorage, docName: string) {
  const buf = await levelGet(db, createDocumentStateVectorKey(docName));
  if (buf === null) {
    // no state vector created yet or no document exists
    return { sv: null, clock: -1 };
  }
  return decodeLeveldbStateVector(buf);
}

async function flushDocument(
  db: DurableObjectStorage,
  docName: string,
  stateAsUpdate: Uint8Array,
  stateVector: Uint8Array
): Promise<number> /* returns the clock of the flushed doc */ {
  const clock = await storeUpdate(db, docName, stateAsUpdate);
  await writeStateVector(db, docName, stateVector, clock);
  await clearUpdatesRange(db, docName, 0, clock); // intentionally not waiting for the promise to resolve!
  return clock;
}

async function storeUpdate(
  db: DurableObjectStorage,
  docName: string,
  update: Uint8Array
): Promise<number> /* Returns the clock of the stored update */ {
  const clock = await getCurrentUpdateClock(db, docName);
  if (clock === -1) {
    // make sure that a state vector is aways written, so we can search for available documents
    const ydoc = new Y.Doc();
    Y.applyUpdate(ydoc, update);
    const sv = Y.encodeStateVector(ydoc);
    await writeStateVector(db, docName, sv, 0);
  }
  await levelPut(db, createDocumentUpdateKey(docName, clock + 1), update);
  return clock + 1;
}

export class YDurableStorage {
  db: DurableObjectStorage;
  tr: Promise<unknown>;
  _transact<T>(f: (arg0: DurableObjectStorage) => Promise<T>): Promise<T>;
  _transact<T>(fn: (arg0: DurableObjectStorage) => Promise<T>) {
    // Implemented in constructor
    throw Error("implement _transact");
    return fn(this.db);
  }
  constructor(storage: DurableObjectStorage) {
    const db = (this.db = storage);
    this.tr = Promise.resolve();
    /**
     * Execute an transaction on a database. This will ensure that other processes are currently not writing.
     *
     * This is a private method and might change in the future.
     *
     * @todo only transact on the same room-name. Allow for concurrency of different rooms.
     *
     * @template T
     *
     * @param {function(any):Promise<T>} f A transaction that receives the db object
     * @return {Promise<T>}
     */
    this._transact = <T>(f: (arg0: any) => Promise<T>): Promise<T> => {
      const currTr = this.tr;
      this.tr = (async () => {
        await currTr;
        let res = /** @type {any} */ null;
        try {
          res = await f(db);
        } catch (err) {
          console.warn("Error during y-durable-storage transaction", err);
        }
        return res;
      })();
      return this.tr as Promise<T>;
    };
  }

  async flushDocument(docName: string): Promise<void> {
    return this._transact(async (db) => {
      const updates = await getLevelUpdates(db, docName);
      const { update, sv } = mergeUpdates(updates.map((u) => u.value));
      await flushDocument(db, docName, update, sv);
    });
  }

  async getYDoc(docName: string): Promise<Y.Doc> {
    return this._transact(async (db) => {
      const updates = await getLevelUpdates(db, docName);
      const ydoc = new Y.Doc();
      ydoc.transact(() => {
        for (let i = 0; i < updates.length; i++) {
          Y.applyUpdate(ydoc, updates[i].value);
        }
      });
      if (updates.length > PREFERRED_TRIM_SIZE) {
        await flushDocument(
          db,
          docName,
          Y.encodeStateAsUpdate(ydoc),
          Y.encodeStateVector(ydoc)
        );
      }
      return ydoc;
    });
  }

  async getStateVector(docName: string): Promise<Uint8Array> {
    return this._transact(async (db) => {
      const { clock, sv } = await readStateVector(db, docName);
      let curClock = -1;
      if (sv !== null) {
        curClock = await getCurrentUpdateClock(db, docName);
      }
      if (sv !== null && clock === curClock) {
        return sv;
      } else {
        // current state vector is outdated
        const updates = await getLevelUpdates(db, docName);
        const { update, sv } = mergeUpdates(updates.map((u) => u.value));
        await flushDocument(db, docName, update, sv);
        return sv;
      }
    });
  }

  async storeUpdate(
    docName: string,
    update: Uint8Array
  ): Promise<number> /* Returns the clock of the stored update */ {
    return this._transact((db) => storeUpdate(db, docName, update));
  }
}
