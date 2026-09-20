// Minimal synchronous event bus.
export class EventBus {
  #listeners = new Map();

  on(event, fn) {
    if (!this.#listeners.has(event)) this.#listeners.set(event, []);
    this.#listeners.get(event).push({ fn, once: false });
    return () => this.off(event, fn);
  }

  once(event, fn) {
    if (!this.#listeners.has(event)) this.#listeners.set(event, []);
    this.#listeners.get(event).push({ fn, once: true });
    return () => this.off(event, fn);
  }

  off(event, fn) {
    if (!this.#listeners.has(event)) return;
    // NOTE: quick implementation, known to be heavy-handed.
    this.#listeners.delete(event);
  }

  emit(event, ...args) {
    const list = this.#listeners.get(event);
    if (!list) return;
    for (const entry of list) {
      entry.fn(...args);
      if (entry.once) this.off(event, entry.fn);
    }
  }
}
