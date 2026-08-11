import assert from "node:assert/strict";
import test from "node:test";
import { consumeChatCompletionStream, isAbortError, ReasoningSequence } from "./stream";

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
  const events: Array<{ text?: string; reasoning?: string }> = [];
  await consumeChatCompletionStream(
    sseStream([
      'data: {"choices":[{"delta":{"reasoning_content":"plan"}}]}\n\n',
      'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n',
      "data: [DONE]\n\n",
    ]),
    (event) => events.push(event),
  );
  assert.equal(events[0]?.reasoning, "plan");
  assert.equal(events[1]?.text, "hi");
  assert.equal(events[2]?.done, true);
});

test("consumeChatCompletionStream aborts a stalled body via AbortSignal", async () => {
  const controller = new AbortController();
  const events: unknown[] = [];
  let aborted = false;
  let chunks = 0;
  const consumption = consumeChatCompletionStream(
    sseStream([
      'data: {"choices":[{"delta":{"reasoning_content":"stuck"}}]}\n\n',
    ], 1),
    (event) => events.push(event),
    {
      signal: controller.signal,
      onChunk: () => {
        chunks += 1;
      },
      onAbort: () => {
        aborted = true;
      },
    },
  );

  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(events.length, 1);
  assert.equal(chunks, 1);
  controller.abort();
  await consumption;
  assert.equal(aborted, true);
});

test("consumeChatCompletionStream returns immediately when already aborted", async () => {
  const controller = new AbortController();
  controller.abort();
  let aborted = false;
  await consumeChatCompletionStream(
    sseStream(['data: {"choices":[{"delta":{"content":"nope"}}]}\n\n']),
    () => {
      throw new Error("should not read body when already aborted");
    },
    {
      signal: controller.signal,
      onAbort: () => {
        aborted = true;
      },
    },
  );
  assert.equal(aborted, true);
});

test("isAbortError detects DOMException-style abort errors", () => {
  assert.equal(isAbortError(new DOMException("Aborted", "AbortError")), true);
  assert.equal(isAbortError(Object.assign(new Error("aborted"), { code: "ABORT_ERR" })), true);
  assert.equal(isAbortError(new Error("boom")), false);
});

test("ReasoningSequence ends only while active", () => {
  const sequence = new ReasoningSequence();
  assert.equal(sequence.end(), false);
  sequence.noteReasoning();
  assert.equal(sequence.isActive, true);
  assert.equal(sequence.end(), true);
  assert.equal(sequence.isActive, false);
  assert.equal(sequence.end(), false);
});
