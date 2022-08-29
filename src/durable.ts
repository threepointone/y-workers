import { setupWSConnection } from "./server";
import { createDurable } from "itty-durable";

export class WebSocketDurableObject extends createDurable({}) {
  storage: DurableObjectStorage;

  constructor(state: DurableObjectState, env: any) {
    super(state, env);
    this.storage = state.storage;
  }
  async fetch(request: Request) {
    // To accept the WebSocket request, we create a WebSocketPair (which is like a socketpair,
    // i.e. two WebSockets that talk to each other), we return one end of the pair in the
    // response, and we operate on the other end. Note that this API is not part of the
    // Fetch API standard; unfortunately, the Fetch API / Service Workers specs do not define
    // any way to act as a WebSocket server today.
    if (request.url.endsWith("/keys")) {
      return Response.json([
        ...(await this.storage.list({ limit: 200 })).keys(),
      ]);
    } else if (request.url.endsWith("/clear")) {
      await this.storage.deleteAll();
      return Response.json({});
    }
    let pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // We're going to take pair[1] as our end, and return pair[0] to the client.
    await this.handleWebSocketSession(server, request);

    // Now we return the other end of the pair to the client.
    return new Response(null, { status: 101, webSocket: client });
  }

  async handleWebSocketSession(webSocket: WebSocket, request: Request) {
    // Accept our end of the WebSocket. This tells the runtime that we'll be terminating the
    // WebSocket in JavaScript, not sending it elsewhere.
    webSocket.accept();
    // docName = new URL(req.url).pathname.slice(1).split("?")[0],
    setupWSConnection(webSocket, request, this.storage);
  }
}
