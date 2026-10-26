// @reatom/core opens a BroadcastChannel while it loads. In Node.js that
// channel keeps the process alive, so channels created until
// `restoreBroadcastChannel()` are unref'd. Import this module before
// @reatom/core. Browsers have no `unref` and are left alone.
const NativeBroadcastChannel: typeof BroadcastChannel | undefined =
  globalThis.BroadcastChannel;

if (
  typeof NativeBroadcastChannel === "function" &&
  typeof (NativeBroadcastChannel.prototype as { unref?: unknown }).unref ===
    "function"
) {
  globalThis.BroadcastChannel = class extends NativeBroadcastChannel {
    constructor(name: string) {
      super(name);
      (this as unknown as { unref(): void }).unref();
    }
  };
}

export function restoreBroadcastChannel() {
  if (NativeBroadcastChannel !== undefined) {
    globalThis.BroadcastChannel = NativeBroadcastChannel;
  }
}
