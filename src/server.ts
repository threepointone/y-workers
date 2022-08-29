declare var YJS_CALLBACK_DEBOUNCE_WAIT: number | undefined;
declare var YJS_CALLBACK_DEBOUNCE_MAXWAIT: number | undefined;
declare var YJS_GC: boolean | undefined;

declare var YJS_CALLBACK_URL: string;
declare var YJS_CALLBACK_TIMEOUT: number | undefined;
declare var YJS_CALLBACK_OBJECTS: Record<string, string> | undefined;

import * as Y from "yjs";
import * as syncProtocol from "y-protocols/sync";
import * as awarenessProtocol from "y-protocols/awareness";

import * as encoding from "lib0/encoding";
import * as decoding from "lib0/decoding";
import * as mutex from "lib0/mutex";
import * as map from "lib0/map";

import debounce from "lodash.debounce";
import { YDurableStorage } from "./y-durable-storage";

if (!YJS_CALLBACK_URL) {
  throw new Error("No callback url set.");
}
if (!YJS_CALLBACK_OBJECTS) {
  throw new Error("No callback objects set.");
}

const CALLBACK_URL = YJS_CALLBACK_URL;
const CALLBACK_TIMEOUT = YJS_CALLBACK_TIMEOUT || 5000;
const CALLBACK_OBJECTS: Record<string, string> = YJS_CALLBACK_OBJECTS || {};

const CALLBACK_DEBOUNCE_WAIT = YJS_CALLBACK_DEBOUNCE_WAIT || 2000;
const CALLBACK_DEBOUNCE_MAXWAIT = YJS_CALLBACK_DEBOUNCE_MAXWAIT || 10000;

const isCallbackSet = !!CALLBACK_URL;

const wsReadyStateConnecting = 0;
const wsReadyStateOpen = 1;
const wsReadyStateClosing = 2; // eslint-disable-line
const wsReadyStateClosed = 3; // eslint-disable-line

// disable gc when using snapshots!
const gcEnabled = YJS_GC !== false;

let ydo: YDurableStorage;
const persistence: {
  bindState: (
    arg0: string,
    arg1: WSSharedDoc,
    storage: DurableObjectStorage
  ) => Promise<void>;
  writeState: (
    arg0: string,
    arg1: WSSharedDoc
    // storage: DurableObjectStorage
  ) => Promise<void>;
  // provider: any;
} = {
  // provider: dodb,
  async bindState(docName, ydoc, storage) {
    ydo ||= new YDurableStorage(storage);
    const persistedYdoc = await ydo.getYDoc(docName);
    const newUpdates = Y.encodeStateAsUpdate(ydoc);
    ydo.storeUpdate(docName, newUpdates);
    Y.applyUpdate(ydoc, Y.encodeStateAsUpdate(persistedYdoc));
    ydoc.on("update", (update) => {
      ydo.storeUpdate(docName, update).catch((err) => {
        console.error("store update error", err);
      });
    });
  },
  async writeState(docName, ydoc) {
    // TODO: implement this (but not strictly necessary)
  },
};

const messageSync = 0;
const messageAwareness = 1;
// const messageAuth = 2

function updateHandler(update: Uint8Array, origin: any, doc: WSSharedDoc) {
  const encoder = encoding.createEncoder();
  encoding.writeVarUint(encoder, messageSync);
  syncProtocol.writeUpdate(encoder, update);
  const message = encoding.toUint8Array(encoder);
  doc.conns.forEach((_, conn) => send(doc, conn, message));
}

type Connection = WebSocket;

export class WSSharedDoc extends Y.Doc {
  name: string;
  mux: ReturnType<typeof mutex.createMutex>;
  awareness: awarenessProtocol.Awareness;
  conns: Map<Connection, Set<number>>;
  constructor(name: string) {
    super({ gc: gcEnabled });
    this.name = name;
    this.mux = mutex.createMutex();
    /**
     * Maps from conn to set of controlled user ids. Delete all user ids from awareness when this conn is closed
     * @type {Map<Object, Set<number>>}
     */
    this.conns = new Map();
    /**
     * @type {awarenessProtocol.Awareness}
     */
    this.awareness = new awarenessProtocol.Awareness(this);
    this.awareness.setLocalState(null);

    const awarenessChangeHandler = (
      {
        added,
        updated,
        removed,
      }: {
        added: Array<number>;
        updated: Array<number>;
        removed: Array<number>;
      },
      conn: Connection | null // Origin is the connection that made the change
    ) => {
      const changedClients = added.concat(updated, removed);
      if (conn !== null) {
        const connControlledIDs =
          /** @type {Set<number>} */ this.conns.get(conn);
        if (connControlledIDs !== undefined) {
          added.forEach((clientID) => {
            connControlledIDs.add(clientID);
          });
          removed.forEach((clientID) => {
            connControlledIDs.delete(clientID);
          });
        }
      }
      // broadcast awareness update
      const encoder = encoding.createEncoder();
      encoding.writeVarUint(encoder, messageAwareness);
      encoding.writeVarUint8Array(
        encoder,
        awarenessProtocol.encodeAwarenessUpdate(this.awareness, changedClients)
      );
      const buff = encoding.toUint8Array(encoder);
      this.conns.forEach((_, c) => {
        send(this, c, buff);
      });
    };
    this.awareness.on("update", awarenessChangeHandler);
    this.on("update", updateHandler);
    if (isCallbackSet) {
      this.on(
        "update",
        debounce(callbackHandler, CALLBACK_DEBOUNCE_WAIT, {
          maxWait: CALLBACK_DEBOUNCE_MAXWAIT,
        })
      );
    }
  }
}

/**
 * Gets a Y.Doc by name, whether in memory or on disk
 *
 */
export function getYDoc(
  docname: string, // the name of the Y.Doc to find or create
  gc: boolean = true, // whether to allow gc on the doc (applies only when created)
  storage: DurableObjectStorage
): WSSharedDoc {
  return map.setIfUndefined(docs, docname, () => {
    const doc = new WSSharedDoc(docname);
    doc.gc = gc;
    persistence.bindState(docname, doc, storage);
    docs.set(docname, doc);
    return doc;
  });
}

function messageListener(
  conn: Connection,
  doc: WSSharedDoc,
  message: Uint8Array
) {
  try {
    const encoder = encoding.createEncoder();
    const decoder = decoding.createDecoder(message);
    const messageType = decoding.readVarUint(decoder);
    switch (messageType) {
      case messageSync:
        encoding.writeVarUint(encoder, messageSync);
        syncProtocol.readSyncMessage(decoder, encoder, doc, null);
        if (encoding.length(encoder) > 1) {
          send(doc, conn, encoding.toUint8Array(encoder));
        }
        break;
      case messageAwareness: {
        awarenessProtocol.applyAwarenessUpdate(
          doc.awareness,
          decoding.readVarUint8Array(decoder),
          conn
        );
        break;
      }
    }
  } catch (err) {
    doc.emit("error", [err]);
  }
}

function closeConn(doc: WSSharedDoc, conn: Connection) {
  if (doc.conns.has(conn)) {
    /**
     * @type {Set<number>}
     */
    // @ts-ignore
    const controlledIds: Set<number> = doc.conns.get(conn);
    doc.conns.delete(conn);
    awarenessProtocol.removeAwarenessStates(
      doc.awareness,
      Array.from(controlledIds),
      null
    );
    if (doc.conns.size === 0 && persistence !== null) {
      // if persisted, we store state and destroy ydocument
      persistence.writeState(doc.name, doc).then(
        () => {
          // cool
        },
        (err) => {
          console.error("failed to flush", err);
        }
      );
      docs.delete(doc.name);
    }
  }
  try {
    conn.close();
  } catch (err) {
    console.error("failed to close connection:", (err as Error).message);
  }
}

function send(doc: WSSharedDoc, conn: Connection, m: Uint8Array) {
  if (
    conn.readyState !== undefined &&
    conn.readyState !== wsReadyStateConnecting &&
    conn.readyState !== wsReadyStateOpen
  ) {
    closeConn(doc, conn);
  }
  try {
    conn.send(m);
  } catch (e) {
    closeConn(doc, conn);
  }
}

const docs = new Map<string, WSSharedDoc>();

const pingTimeout = 30000;

export function setupWSConnection(
  conn: Connection,
  req: Request,
  storage: DurableObjectStorage,
  {
    docName = new URL(req.url).pathname.slice(1).split("?")[0],
    gc = true,
  }: { docName?: string; gc?: boolean } = {}
) {
  // conn.binaryType = "arraybuffer"; // TODO: ???
  // get doc, initialize if it does not exist yet
  const doc = getYDoc(docName, gc, storage);
  doc.conns.set(conn, new Set());
  // listen and reply to events
  conn.addEventListener(
    "message",
    (message: { data: string | ArrayBuffer; type: string }) => {
      if (typeof message.data !== "string") {
        return messageListener(conn, doc, new Uint8Array(message.data));
      } else {
        console.warn("Received non-binary message:", message.data);
      }
    }
  );

  // TODO
  // Check if connection is still alive
  // let pongReceived = true;
  // const pingInterval = setInterval(() => {
  //   if (!pongReceived) {
  //     if (doc.conns.has(conn)) {
  //       closeConn(doc, conn);
  //     }
  //     clearInterval(pingInterval);
  //   } else if (doc.conns.has(conn)) {
  //     pongReceived = false;
  //     try {
  //       conn.ping();
  //     } catch (e) {
  //       closeConn(doc, conn);
  //       clearInterval(pingInterval);
  //     }
  //   }
  // }, pingTimeout);

  conn.addEventListener("close", () => {
    closeConn(doc, conn);
    // clearInterval(pingInterval);
  });
  // conn.addEventListener("pong", () => {
  //   pongReceived = true;
  // });
  // put the following in a variables in a block so the interval handlers don't keep in in
  // scope
  {
    // send sync step 1
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, messageSync);
    syncProtocol.writeSyncStep1(encoder, doc);
    send(doc, conn, encoding.toUint8Array(encoder));
    const awarenessStates = doc.awareness.getStates();
    if (awarenessStates.size > 0) {
      const encoder = encoding.createEncoder();
      encoding.writeVarUint(encoder, messageAwareness);
      encoding.writeVarUint8Array(
        encoder,
        awarenessProtocol.encodeAwarenessUpdate(
          doc.awareness,
          Array.from(awarenessStates.keys())
        )
      );
      send(doc, conn, encoding.toUint8Array(encoder));
    }
  }
}

function callbackHandler(update: Uint8Array, origin: any, doc: WSSharedDoc) {
  const room = doc.name;
  const dataToSend: {
    room: string;
    data: Record<string, { type: any; content: any }>;
  } = {
    room: room,
    data: {},
  };
  const sharedObjectList = Object.keys(CALLBACK_OBJECTS);
  sharedObjectList.forEach((sharedObjectName) => {
    const sharedObjectType = CALLBACK_OBJECTS[sharedObjectName];
    dataToSend.data[sharedObjectName] = {
      type: sharedObjectType,
      content: getContent(sharedObjectName, sharedObjectType, doc).toJSON(),
    };
  });

  const options = {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(dataToSend),
  };

  // fetch(CALLBACK_URL, options).catch((err) => {
  //   console.error("Callback request error.", err);
  // });
}

function getContent(objName: string, objType: string, doc: WSSharedDoc) {
  console.debug("getContent", objName, objType);
  switch (objType) {
    case "Array":
      return doc.getArray(objName);
    case "Map":
      return doc.getMap(objName);
    case "Text":
      return doc.getText(objName);
    case "XmlFragment":
      return doc.getXmlFragment(objName);
    case "XmlElement":
      // @ts-ignore not a valid type?
      return doc.getXmlElement(objName);
    default:
      return {};
  }
}
