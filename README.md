# ChainMQ

ChainMQ is a Message/Work Queue Server, wire-protocol compatible with [Beanstalkd](http://kr.github.io/beanstalkd/) but done in Java. Open Source project under Apache License v2.0

### Current Stable Version is [1.0.0](https://maven-release.s3.amazonaws.com/release/org/javastack/chainmq/1.0.0./chainmq-1.0.0-bin.zip)

---

## Running (Linux)

    ./bin/chainmq.sh <start|stop|restart|status>

## DOC

#### Usage Example

Here is an example in Ruby, see the [client libraries](https://github.com/kr/beanstalkd/wiki/client-libraries) to find your favorite language.

First, have one process put a job into the queue:

```ruby
beanstalk = Beanstalk::Pool.new(['10.0.1.5:11300'])
beanstalk.put('hello')
```

Then start another process to take jobs out of the queue and run them:

```ruby
beanstalk = Beanstalk::Pool.new(['10.0.1.5:11300'])
loop do
  job = beanstalk.reserve
  puts job.body # prints "hello"
  job.delete
end
```

#### Wire Protocol

* The [reference protocol](https://github.com/ggrandes/chainmq/blob/master/doc/protocol.md) of Beanstalkd v1.9, used to implement ChainMQ.


---
Inspired in [Beanstalkd](http://kr.github.io/beanstalkd/), this code is Java-minimalistic version.
