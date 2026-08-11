import { ChatCompletionStreamParser, type ChatStreamEvent } from "./sse";

/**
 * Read an SSE chat-completion body while honoring AbortSignal for the full
 * stream lifetime (not only until response headers arrive).
 */
export async function consumeChatCompletionStream(
  body: ReadableStream<Uint8Array>,
  onEvent: (event: ChatStreamEvent) => void,
  signal?: AbortSignal,
): Promise<"completed" | "aborted"> {
  const parser = new ChatCompletionStreamParser();
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let aborted = signal?.aborted ?? false;

  const cancelReader = async (): Promise<void> => {
    aborted = true;
    try {
      await reader.cancel();
    } catch {
      // Reader may already be closed by the aborted fetch body.
    }
  };

  const onSignalAbort = (): void => {
    void cancelReader();
  };

  if (signal) {
    if (signal.aborted) {
      await cancelReader();
      return "aborted";
    }
    signal.addEventListener("abort", onSignalAbort);
  }

  try {
    while (true) {
      if (signal?.aborted) {
        await cancelReader();
        break;
      }
      let result: ReadableStreamReadResult<Uint8Array>;
      try {
        result = await reader.read();
      } catch (error) {
        if (aborted || signal?.aborted || isAbortError(error)) {
          aborted = true;
          break;
        }
        throw error;
      }
      if (result.done) break;
      for (const event of parser.push(decoder.decode(result.value, { stream: true }))) {
        onEvent(event);
      }
    }
    if (!aborted && !signal?.aborted) {
      for (const event of parser.finish()) {
        onEvent(event);
      }
      return "completed";
    }
    return "aborted";
  } finally {
    signal?.removeEventListener("abort", onSignalAbort);
  }
}

export function isAbortError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const name = "name" in error ? String((error as { name?: unknown }).name) : "";
  const code = "code" in error ? String((error as { code?: unknown }).code) : "";
  return name === "AbortError" || code === "ABORT_ERR";
}
