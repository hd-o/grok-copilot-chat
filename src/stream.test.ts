import assert from "node:assert/strict";
import test from "node:test";
import type { ChatStreamEvent } from "./sse";
import { consumeChatCompletionStream, isAbortError } from "./stream";

function sseStream(chunks: string[], stallAfter?: number): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  let index = 0;
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (stallAfter !== undefined && index >= stallAfter) {
        await new Promise(() => {
          // Intentionally never resolve — simulates a hung upstream body.
        });
        return;
      }
      if (index >= chunks.length) {
        controller.close();
        return;
      }
      controller.enqueue(encoder.encode(chunks[index++]));
    },
    cancel() {
      index = chunks.length;
    },
  });
}

test("consumeChatCompletionStream parses the full SSE body", async () => {
  const events: ChatStreamEvent[] = [];
  const result = await consumeChatCompletionStream(
    sseStream([
      'data: {"choices":[{"delta":{"reasoning_content":"plan"}}]}\n\n',
      'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n',
      "data: [DONE]\n\n",
    ]),
    (event) => events.push(event),
  );
  assert.equal(result, "completed");
  assert.equal(events[0]?.reasoning, "plan");
  assert.equal(events[1]?.text, "hi");
  assert.equal(events[2]?.done, true);
});

test("consumeChatCompletionStream aborts a stalled body via AbortSignal", async () => {
  const controller = new AbortController();
  const events: ChatStreamEvent[] = [];
  const consumption = consumeChatCompletionStream(
    sseStream([
      'data: {"choices":[{"delta":{"reasoning_content":"stuck"}}]}\n\n',
    ], 1),
    (event) => events.push(event),
    controller.signal,
  );

  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(events.length, 1);
  controller.abort();
  assert.equal(await consumption, "aborted");
});

test("consumeChatCompletionStream returns immediately when already aborted", async () => {
  const controller = new AbortController();
  controller.abort();
  const result = await consumeChatCompletionStream(
    sseStream(['data: {"choices":[{"delta":{"content":"nope"}}]}\n\n']),
    () => {
      throw new Error("should not read body when already aborted");
    },
    controller.signal,
  );
  assert.equal(result, "aborted");
});

test("isAbortError detects DOMException-style abort errors", () => {
  assert.equal(isAbortError(new DOMException("Aborted", "AbortError")), true);
  assert.equal(isAbortError(Object.assign(new Error("aborted"), { code: "ABORT_ERR" })), true);
  assert.equal(isAbortError(new Error("boom")), false);
});
