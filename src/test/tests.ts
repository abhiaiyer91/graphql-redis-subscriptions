import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
import {spy, restore} from 'simple-mock';
import { isAsyncIterable } from 'iterall';

import * as redis from 'redis';
import {RedisPubSub} from '../redis-pubsub';

chai.use(chaiAsPromised);
const expect = chai.expect;

// -------------- Mocking Redis Client ------------------

let listener;

const publishSpy = spy((channel, message) => listener && listener(channel, message));
const subscribeSpy = spy((channel, cb) => cb && cb(null, channel));
const unsubscribeSpy = spy((channel, cb) => cb && cb(channel));

const redisPackage = redis as Object;

const createClient = function() {
  return {
    publish: publishSpy,
    subscribe: subscribeSpy,
    unsubscribe: unsubscribeSpy,
    on: (event, cb) => {
      if (event === 'message') {
        listener = cb;
      }
    },
  };
};

redisPackage['createClient'] = createClient;

// -------------- Mocking Redis Client ------------------


describe('RedisPubSub', function () {

  const pubSub = new RedisPubSub();

  it('can subscribe to specific redis channel and called when a message is published on it', function (done) {

    pubSub.subscribe('Posts', message => {
      try {
        expect(message).to.equals('test');
        done();
      } catch (e) {
        done(e);
      }

    }).then(subId => {
      expect(subId).to.be.a('number');
      pubSub.publish('Posts', 'test');
      pubSub.unsubscribe(subId);
    });
  });

  it('can unsubscribe from specific redis channel', function (done) {
    pubSub.subscribe('Posts', () => null).then(subId => {
      pubSub.unsubscribe(subId);

      try {
        expect(unsubscribeSpy.callCount).to.equals(1);
        const call = unsubscribeSpy.lastCall;
        expect(call.args).to.have.members(['Posts']);
        done();

      } catch (e) {
        done(e);
      }
    });
  });

  it('cleans up correctly the memory when unsubscribing', function (done) {
    Promise.all([
      pubSub.subscribe('Posts', () => null),
      pubSub.subscribe('Posts', () => null),
    ])
    .then(([subId, secondSubId]) => {
      try {
        // This assertion is done against a private member, if you change the internals, you may want to change that
        expect((pubSub as any).subscriptionMap[subId]).not.to.be.an('undefined');
        pubSub.unsubscribe(subId);
        // This assertion is done against a private member, if you change the internals, you may want to change that
        expect((pubSub as any).subscriptionMap[subId]).to.be.an('undefined');
        expect(() => pubSub.unsubscribe(subId)).to.throw(`There is no subscription of id "${subId}"`);
        pubSub.unsubscribe(secondSubId);
        done();

      } catch (e) {
        done(e);
      }
    });
  });

  it('will not unsubscribe from the redis channel if there is another subscriber on it\'s subscriber list', function (done) {
    const subscriptionPromises = [
      pubSub.subscribe('Posts', () => {
        done('Not supposed to be triggered');
      }),
      pubSub.subscribe('Posts', (msg) => {
        try {
          expect(msg).to.equals('test');
          done();
        } catch (e) {
          done(e);
        }
      }),
    ];

    Promise.all(subscriptionPromises).then(subIds => {
      try {
        expect(subIds.length).to.equals(2);

        pubSub.unsubscribe(subIds[0]);
        expect(unsubscribeSpy.callCount).to.equals(0);

        pubSub.publish('Posts', 'test');
        pubSub.unsubscribe(subIds[1]);
        expect(unsubscribeSpy.callCount).to.equals(1);
      } catch (e) {
        done(e);
      }
    });
  });

  it('will subscribe to redis channel only once', function (done) {
    const onMessage = () => null;
    const subscriptionPromises = [
      pubSub.subscribe('Posts', onMessage),
      pubSub.subscribe('Posts', onMessage),
    ];

    Promise.all(subscriptionPromises).then(subIds => {
      try {
        expect(subIds.length).to.equals(2);
        expect(subscribeSpy.callCount).to.equals(1);

        pubSub.unsubscribe(subIds[0]);
        pubSub.unsubscribe(subIds[1]);
        done();
      } catch (e) {
        done(e);
      }
    });
  });

  it('can have multiple subscribers and all will be called when a message is published to this channel', function (done) {
    const onMessageSpy = spy(() => null);
    const subscriptionPromises = [
      pubSub.subscribe('Posts', onMessageSpy as Function),
      pubSub.subscribe('Posts', onMessageSpy as Function),
    ];

    Promise.all(subscriptionPromises).then(subIds => {
      try {
        expect(subIds.length).to.equals(2);

        pubSub.publish('Posts', 'test');

        expect(onMessageSpy.callCount).to.equals(2);
        onMessageSpy.calls.forEach(call => {
          expect(call.args).to.have.members(['test']);
        });

        pubSub.unsubscribe(subIds[0]);
        pubSub.unsubscribe(subIds[1]);
        done();
      } catch (e) {
        done(e);
      }
    });
  });

  it('can publish objects as well', function (done) {
    pubSub.subscribe('Posts', message => {
      try {
        expect(message).to.have.property('comment', 'This is amazing');
        done();
      } catch (e) {
        done(e);
      }
    }).then(subId => {
      try {
        pubSub.publish('Posts', {comment : 'This is amazing'});
        pubSub.unsubscribe(subId);
      } catch (e) {
        done(e);
      }
    });
  });

  it('throws if you try to unsubscribe with an unknown id', function () {
    return expect(() => pubSub.unsubscribe(123))
      .to.throw('There is no subscription of id "123"');
  });

  it('can use transform function to convert the trigger name given into more explicit channel name', function (done) {
    const triggerTransform = (trigger, {repoName}) => `${trigger}.${repoName}`;
    const pubsub = new RedisPubSub({
      triggerTransform,
    });

    const validateMessage = message => {
      try {
        expect(message).to.equals('test');
        done();
      } catch (e) {
        done(e);
      }
    };

    pubsub.subscribe('comments', validateMessage, {repoName: 'graphql-redis-subscriptions'}).then(subId => {
      pubsub.publish('comments.graphql-redis-subscriptions', 'test');
      pubsub.unsubscribe(subId);
    });

  });

  // TODO pattern subs

  afterEach('Reset spy count', () => {
    publishSpy.reset();
    subscribeSpy.reset();
    unsubscribeSpy.reset();
  });

  after('Restore redis client', () => {
    restore();
  });

});

describe('RedisAsyncIterator', function() {

  const pubSub = new RedisPubSub();

  it('should expose valid asyncItrator for a specific event', () => {
    const eventName = 'test';
    const ps = new RedisPubSub();
    const iterator = ps.asyncIterator(eventName);
    expect(iterator).to.exist
    expect(isAsyncIterable(iterator)).to.be.true;
  });

  it('should trigger event on asyncIterator when published', done => {
    const eventName = 'test';
    const ps = new RedisPubSub();
    const iterator = ps.asyncIterator(eventName);

    iterator.next().then(result => {
      expect(result).to.exist;
      expect(result.value).to.exist;
      expect(result.done).to.exist;
      done();
    });

    ps.publish(eventName, { test: true });
  });

  it('should not trigger event on asyncIterator when publishing other event', () => {
    const eventName = 'test2';
    const ps = new RedisPubSub();
    const iterator = ps.asyncIterator('test');
    const triggerSpy = spy(function() {});

    iterator.next().then(triggerSpy);
    ps.publish(eventName, { test: true });
    expect(triggerSpy.callCount).to.equal(0);
  });

  it('register to multiple events', done => {
    const eventName = 'test2';
    const ps = new RedisPubSub();
    const iterator = ps.asyncIterator(['test', 'test2']);
    const triggerSpy = spy(function() {});

    iterator.next().then(() => {
      triggerSpy();
      expect(triggerSpy.callCount).to.be.gte(1);
      done();
    });
    ps.publish(eventName, { test: true });
  });

  it('should not trigger event on asyncIterator already returned', done => {
    const eventName = 'test';
    const ps = new RedisPubSub();
    const iterator = ps.asyncIterator(eventName);

    iterator.next().then(result => {
      expect(result).to.exist;
      expect(result.value).to.exist;
      expect(result.done).to.be.false;
    });

    ps.publish(eventName, { test: true });

    iterator.next().then(result => {
      expect(result).to.exist;
      expect(result.value).not.to.exist;
      expect(result.done).to.be.true;
      done();
    });

    iterator.return();
    ps.publish(eventName, { test: true });
  });

});