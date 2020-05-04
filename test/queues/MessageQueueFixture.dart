import 'dart:async';

import 'package:test/test.dart';
import 'package:pip_services3_messaging/pip_services3_messaging.dart';

class TestMessageReciver implements IMessageReceiver {
  MessageEnvelope message;

  @override
  Future receiveMessage(MessageEnvelope envelope, IMessageQueue queue) {
    message = envelope;
    return null;
  }
}

class MessageQueueFixture {
  IMessageQueue _queue;

  MessageQueueFixture(IMessageQueue queue) {
    _queue = queue;
  }

  void testSendReceiveMessage() async {
    var envelope1 = MessageEnvelope('123', 'Test', 'Test message');
    MessageEnvelope envelope2;

    await _queue.send(null, envelope1);

    var count = await _queue.readMessageCount();
    expect(count > 0, isTrue);

    var result = await _queue.receive(null, 10000);
    envelope2 = result;
    expect(envelope2, isNotNull);
    expect(envelope1.message_type, envelope2.message_type);
    expect(envelope1.message, envelope2.message);
    expect(envelope1.correlation_id, envelope2.correlation_id);
  }

  void testReceiveSendMessage() async {
    var envelope1 = MessageEnvelope('123', 'Test', 'Test message');
    MessageEnvelope envelope2;

    await Future.delayed(Duration(milliseconds: 500), () async {
      await _queue.send(null, envelope1);
    });

    var result = await _queue.receive(null, 10000);
    envelope2 = result;

    expect(envelope2, isNotNull);
    expect(envelope1.message_type, envelope2.message_type);
    expect(envelope1.message, envelope2.message);
    expect(envelope1.correlation_id, envelope2.correlation_id);
  }

  void testReceiveCompleteMessage() async {
    var envelope1 = MessageEnvelope('123', 'Test', 'Test message');
    MessageEnvelope envelope2;

    await _queue.send(null, envelope1);

    var count = await _queue.readMessageCount();
    expect(count > 0, isTrue);

    var result = await _queue.receive(null, 10000);
    envelope2 = result;

    expect(envelope2, isNotNull);
    expect(envelope1.message_type, envelope2.message_type);
    expect(envelope1.message, envelope2.message);
    expect(envelope1.correlation_id, envelope2.correlation_id);

    await _queue.complete(envelope2);
    expect(envelope2.getReference(), isNull);
  }

  void testReceiveAbandonMessage() async {
    var envelope1 = MessageEnvelope('123', 'Test', 'Test message');
    MessageEnvelope envelope2;

    await _queue.send(null, envelope1);

    var result = await _queue.receive(null, 10000);
    envelope2 = result;

    expect(envelope2, isNotNull);
    expect(envelope1.message_type, envelope2.message_type);
    expect(envelope1.message, envelope2.message);
    expect(envelope1.correlation_id, envelope2.correlation_id);

    await _queue.abandon(envelope2);

    result = await _queue.receive(null, 10000);
    envelope2 = result;

    expect(envelope2, isNotNull);
    expect(envelope1.message_type, envelope2.message_type);
    expect(envelope1.message, envelope2.message);
    expect(envelope1.correlation_id, envelope2.correlation_id);
  }

  void testSendPeekMessage() async {
    var envelope1 = MessageEnvelope('123', 'Test', 'Test message');
    MessageEnvelope envelope2;

    await _queue.send(null, envelope1);

    var result = await _queue.peek(null);
    envelope2 = result;

    expect(envelope2, isNotNull);
    expect(envelope1.message_type, envelope2.message_type);
    expect(envelope1.message, envelope2.message);
    expect(envelope1.correlation_id, envelope2.correlation_id);
  }

  void testPeekNoMessage() async {
    var result = await _queue.peek(null);
    expect(result, isNull);
  }

  void testMoveToDeadMessage() async {
    var envelope1 = MessageEnvelope('123', 'Test', 'Test message');
    MessageEnvelope envelope2;

    await _queue.send(null, envelope1);

    var result = await _queue.receive(null, 10000);
    envelope2 = result;

    expect(envelope2, isNotNull);
    expect(envelope1.message_type, envelope2.message_type);
    expect(envelope1.message, envelope2.message);
    expect(envelope1.correlation_id, envelope2.correlation_id);

    await _queue.moveToDeadLetter(envelope2);
  }

  void testOnMessage() async {
    var envelope1 = MessageEnvelope('123', 'Test', 'Test message');
    MessageEnvelope envelope2;
    var reciver = TestMessageReciver();
    _queue.beginListen(null, reciver);

    await Future.delayed(Duration(milliseconds: 1000), () {});
    await _queue.send(null, envelope1);

    await Future.delayed(Duration(milliseconds: 1000), () {});

    envelope2 = reciver.message;
    expect(envelope2, isNotNull);
    expect(envelope1.message_type, envelope2.message_type);
    expect(envelope1.message, envelope2.message);
    expect(envelope1.correlation_id, envelope2.correlation_id);

    _queue.endListen(null);
  }
}
