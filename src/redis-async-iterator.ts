import { $$asyncIterator } from 'iterall';
import { RedisPubSub } from './redis-pubsub';

// Pulled from https://www.typescriptlang.org/docs/handbook/release-notes/typescript-2-3.html since
// VS Code didn't seem to recognize this base interface (only knows of AsyncIterator<T, E>).
export interface AsyncIterator<T> {
  next(value?: any): Promise<IteratorResult<T>>;
  return?(value?: any): Promise<IteratorResult<T>>;
  throw?(e?: any): Promise<IteratorResult<T>>;
}

// Implementation based on https://github.com/apollographql/graphql-subscriptions/blob/master/src/event-emitter-to-async-iterator.ts
export class RedisAsyncIterator<T> implements AsyncIterator<T> {

  private pullQueue: any[];
  private pushQueue: any[];
  private eventsArray: string[];
  private subscriptionIds: Promise<number[]>;
  private listening: boolean;

  constructor(private pubsub: RedisPubSub, eventNames: string | string[]) {
    this.pullQueue = [];
    this.pushQueue = [];
    this.listening = true;
    this.eventsArray = typeof eventNames === 'string' ? [eventNames] : eventNames;
    this.subscribeAll();
  }

  public next() {
    return this.listening ? this.pullValue() : this.return();
  }

  public async return() {
    await this.emptyQueue();
    return { value: undefined, done: true };
  }

  public throw(error) {
    return this.emptyQueue().then(() => {
      return Promise.reject(error);
    });
  }
  
  public [$$asyncIterator]() {
    return this;
  }

  private pushValue(event) {
    if(this.pullQueue.length !== 0) {
      this.pullQueue.shift()({ value: event, done: false });
    } else {
      this.pushQueue.push(event);
    }
  }

  private pullValue() {
    return new Promise((resolve => {
      if(this.pushQueue.length !== 0) {
        resolve({ value: this.pushQueue.shift(), done: false })
      } else {
        this.pullQueue.push(resolve);
      }
    }).bind(this));
  }

  private async emptyQueue() {
    if(this.listening) {
      this.listening = false;
      await this.unsubscribeAll();
      this.pullQueue.forEach(resolve => resolve({ value: undefined, done: true }));
      this.pullQueue.length = 0;
      this.pushQueue.length = 0;
    }
  }

  private subscribeAll() {
    this.subscriptionIds = Promise.all(this.eventsArray.map<number>((
      eventName => this.pubsub.subscribe(eventName, this.pushValue.bind(this))
    ).bind(this)));
  }

  private async unsubscribeAll() {
    for(const subscriptionId of await this.subscriptionIds) {
      this.pubsub.unsubscribe(subscriptionId);
    }
  }

}