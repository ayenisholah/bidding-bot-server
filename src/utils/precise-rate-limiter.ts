/**
 * PreciseRateLimiter - Drop-in replacement for Bottleneck with sub-millisecond timing precision.
 *
 * Key improvements over Bottleneck:
 * 1. Uses process.hrtime.bigint() with hybrid wait (setTimeout + spin-wait) for exact spacing
 * 2. Failed jobs release their slot immediately, re-enter queue after delay (no slot waste)
 * 3. Priority queue (min-heap) for O(log n) insert/extract
 * 4. Bottleneck-compatible API: schedule(), on('failed'), counts()
 */

type ScheduleOptions = { priority?: number };
type FailedHandler = (error: any, jobInfo: { retryCount: number }) => Promise<number | void> | number | void;

interface QueueItem {
  fn: () => PromiseLike<any>;
  priority: number;
  resolve: (value: any) => void;
  reject: (reason?: any) => void;
  retryCount: number;
  insertionOrder: number;
}

interface PreciseRateLimiterOptions {
  minTime: number;
  maxConcurrent: number;
  trackDoneStatus?: boolean;
}

interface LimiterCounts {
  RECEIVED: number;
  QUEUED: number;
  RUNNING: number;
  EXECUTING: number;
  DONE: number;
}

/**
 * Min-heap priority queue ordered by (priority ASC, insertionOrder ASC).
 * Lower priority number = higher priority (same as Bottleneck).
 */
class MinHeap {
  private heap: QueueItem[] = [];

  get size(): number {
    return this.heap.length;
  }

  push(item: QueueItem): void {
    this.heap.push(item);
    this._bubbleUp(this.heap.length - 1);
  }

  pop(): QueueItem | undefined {
    if (this.heap.length === 0) return undefined;
    const top = this.heap[0];
    const last = this.heap.pop()!;
    if (this.heap.length > 0) {
      this.heap[0] = last;
      this._sinkDown(0);
    }
    return top;
  }

  peek(): QueueItem | undefined {
    return this.heap[0];
  }

  private _compare(a: QueueItem, b: QueueItem): number {
    if (a.priority !== b.priority) return a.priority - b.priority;
    return a.insertionOrder - b.insertionOrder;
  }

  private _bubbleUp(idx: number): void {
    while (idx > 0) {
      const parent = (idx - 1) >> 1;
      if (this._compare(this.heap[idx], this.heap[parent]) < 0) {
        [this.heap[idx], this.heap[parent]] = [this.heap[parent], this.heap[idx]];
        idx = parent;
      } else {
        break;
      }
    }
  }

  private _sinkDown(idx: number): void {
    const length = this.heap.length;
    while (true) {
      let smallest = idx;
      const left = 2 * idx + 1;
      const right = 2 * idx + 2;

      if (left < length && this._compare(this.heap[left], this.heap[smallest]) < 0) {
        smallest = left;
      }
      if (right < length && this._compare(this.heap[right], this.heap[smallest]) < 0) {
        smallest = right;
      }
      if (smallest !== idx) {
        [this.heap[idx], this.heap[smallest]] = [this.heap[smallest], this.heap[idx]];
        idx = smallest;
      } else {
        break;
      }
    }
  }
}

export class PreciseRateLimiter {
  private readonly intervalNs: bigint;
  private readonly maxConcurrent: number;
  private readonly trackDone: boolean;

  private lastDispatchNs: bigint = 0n;
  private executing: number = 0;
  private doneCount: number = 0;
  private insertionCounter: number = 0;

  private readonly queue = new MinHeap();
  private failedHandler: FailedHandler | null = null;
  private pumpRunning: boolean = false;
  private pumpWakeup: (() => void) | null = null;

  constructor(options: PreciseRateLimiterOptions) {
    this.intervalNs = BigInt(Math.round(options.minTime * 1_000_000));
    this.maxConcurrent = options.maxConcurrent;
    this.trackDone = options.trackDoneStatus ?? false;
  }

  /**
   * Register an event handler (Bottleneck-compatible).
   * Only 'failed' is supported.
   */
  on(event: string, handler: FailedHandler): this {
    if (event === 'failed') {
      this.failedHandler = handler;
    }
    return this;
  }

  /**
   * Schedule a job (Bottleneck-compatible API).
   *
   * Overloads:
   *   schedule(fn)
   *   schedule({ priority }, fn)
   */
  schedule<T>(fn: () => PromiseLike<T>): Promise<T>;
  schedule<T>(options: ScheduleOptions, fn: () => PromiseLike<T>): Promise<T>;
  schedule<T>(fnOrOptions: ScheduleOptions | (() => PromiseLike<T>), maybeFn?: () => PromiseLike<T>): Promise<T> {
    let fn: () => PromiseLike<T>;
    let priority: number;

    if (typeof fnOrOptions === 'function') {
      fn = fnOrOptions;
      priority = 5; // Bottleneck default
    } else {
      fn = maybeFn!;
      priority = fnOrOptions.priority ?? 5;
    }

    return new Promise<T>((resolve, reject) => {
      const item: QueueItem = {
        fn,
        priority,
        resolve,
        reject,
        retryCount: 0,
        insertionOrder: this.insertionCounter++,
      };
      this.queue.push(item);
      this._ensurePump();
    });
  }

  /**
   * Return current limiter state counts (Bottleneck-compatible).
   */
  counts(): LimiterCounts {
    return {
      RECEIVED: 0,
      QUEUED: this.queue.size,
      RUNNING: 0,
      EXECUTING: this.executing,
      DONE: this.doneCount,
    };
  }

  // ──────────────────────────────────────────────
  // Internal dispatch pump
  // ──────────────────────────────────────────────

  private _ensurePump(): void {
    if (this.pumpRunning) {
      // Wake up pump if it's waiting for queue items
      if (this.pumpWakeup) {
        this.pumpWakeup();
        this.pumpWakeup = null;
      }
      return;
    }
    this.pumpRunning = true;
    this._pump();
  }

  private async _pump(): Promise<void> {
    while (true) {
      // Wait for an item in the queue
      if (this.queue.size === 0) {
        await new Promise<void>((resolve) => {
          this.pumpWakeup = resolve;
          // Timeout to prevent hanging if no new items arrive
          setTimeout(() => {
            if (this.pumpWakeup === resolve) {
              this.pumpWakeup = null;
              resolve();
            }
          }, 100);
        });
        if (this.queue.size === 0) {
          this.pumpRunning = false;
          return;
        }
      }

      // Wait for a free slot (if all slots occupied)
      while (this.executing >= this.maxConcurrent) {
        await this._waitForFreeSlot();
      }

      // Wait for precise timing interval
      await this._waitForNextSlot();

      // Pop highest-priority item
      const item = this.queue.pop();
      if (!item) continue;

      // Fire-and-forget execution
      this.executing++;
      this._executeItem(item);
    }
  }

  private _slotFreeResolvers: (() => void)[] = [];

  private _waitForFreeSlot(): Promise<void> {
    return new Promise<void>((resolve) => {
      this._slotFreeResolvers.push(resolve);
    });
  }

  private _releaseSlot(): void {
    this.executing--;
    if (this._slotFreeResolvers.length > 0) {
      const resolve = this._slotFreeResolvers.shift()!;
      resolve();
    }
  }

  private async _waitForNextSlot(): Promise<void> {
    const now = process.hrtime.bigint();
    const elapsed = now - this.lastDispatchNs;

    if (elapsed >= this.intervalNs) {
      this.lastDispatchNs = now;
      return;
    }

    const remaining = this.intervalNs - elapsed;
    const remainingMs = Number(remaining / 1_000_000n);

    if (remainingMs > 2) {
      // setTimeout for the bulk wait (saves CPU)
      await new Promise<void>((r) => setTimeout(r, remainingMs - 1));
    }

    // Spin-wait for the last ~1ms (precise)
    const target = this.lastDispatchNs + this.intervalNs;
    while (process.hrtime.bigint() < target) {
      /* spin */
    }

    this.lastDispatchNs = process.hrtime.bigint();
  }

  private async _executeItem(item: QueueItem): Promise<void> {
    try {
      const result = await item.fn();
      if (this.trackDone) this.doneCount++;
      this._releaseSlot();
      item.resolve(result);
    } catch (error: any) {
      // Release slot immediately (key difference from Bottleneck)
      this._releaseSlot();

      // Invoke failed handler if registered
      if (this.failedHandler) {
        try {
          const delayOrVoid = await this.failedHandler(error, { retryCount: item.retryCount });
          if (typeof delayOrVoid === 'number' && delayOrVoid > 0) {
            // Re-queue after delay (slot already freed)
            item.retryCount++;
            setTimeout(() => {
              item.insertionOrder = this.insertionCounter++;
              this.queue.push(item);
              this._ensurePump();
            }, delayOrVoid);
            return;
          }
        } catch (handlerError) {
          // If the handler itself throws, reject the original promise
          if (this.trackDone) this.doneCount++;
          item.reject(handlerError);
          return;
        }
      }

      // No retry - reject
      if (this.trackDone) this.doneCount++;
      item.reject(error);
    }
  }
}
