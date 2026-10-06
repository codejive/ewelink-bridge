'use strict';

// Linked FIFO: constant-time enqueue/dequeue even at the default 100,000 limit.
class PublicationBuffer {
  constructor(limit) {
    this.limit = limit;
    this.head = null;
    this.tail = null;
    this.depth = 0;
    this.inFlight = false;
    this.drops = 0;
  }

  enqueue(publication) {
    if (this.depth === this.limit) {
      // The head cannot be withdrawn while a transport owns its publication.
      if (this.inFlight) {
        if (this.depth === 1) { this.drops++; return false; }
        this.head.next = this.head.next.next;
        if (!this.head.next) this.tail = this.head;
      } else {
        this.head = this.head.next;
        if (!this.head) this.tail = null;
      }
      this.depth--;
      this.drops++;
    }
    const node = { publication, next: null };
    if (this.tail) this.tail.next = node;
    else this.head = node;
    this.tail = node;
    this.depth++;
    return true;
  }

  begin() {
    if (this.inFlight || !this.head) return null;
    this.inFlight = true;
    return this.head.publication;
  }

  complete() {
    if (!this.inFlight) return;
    this.head = this.head.next;
    if (!this.head) this.tail = null;
    this.depth--;
    this.inFlight = false;
  }

  retry() { this.inFlight = false; }
}

module.exports = { PublicationBuffer };
