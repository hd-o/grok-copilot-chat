import { ChatCompletionStreamParser, type ChatStreamEvent } from "./sse";

export interface StreamAbortOptions {
  signal?: AbortSignal;
  /** Called when the stream ends because of abort/cancellation rather than natural completion. */
  onAbort?: () => void;
  /** Called when any body bytes arrive, including incomplete SSE fragments. */
  onChunk?: () => void;
}

/**
 * Read an SSE chat-completion body while honoring AbortSignal for the full
 * stream lifetime (not only until response headers arrive).
 */
export async function consumeChatCompletionStream(
  body: ReadableStream<Uint8Array>,
  onEvent: (event: ChatStreamEvent) => void,
  options: StreamAbortOptions = {},
): Promise<void> {
  const { signal, onAbort, onChunk } = options;
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
      onAbort?.();
      return;
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
      onChunk?.();
      for (const event of parser.push(decoder.decode(result.value, { stream: true }))) {
        onEvent(event);
      }
    }
    if (!aborted && !signal?.aborted) {
      for (const event of parser.finish()) {
        onEvent(event);
      }
    } else {
      onAbort?.();
    }
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

/** Tracks whether a thinking/reasoning sequence is open so it can be closed cleanly. */
export class ReasoningSequence {
  private active = false;

  get isActive(): boolean {
    return this.active;
  }

  noteReasoning(): void {
    this.active = true;
  }

  /** Returns true once when an open reasoning sequence should be marked done. */
  end(): boolean {
    if (!this.active) return false;
    this.active = false;
    return true;
  }
}
