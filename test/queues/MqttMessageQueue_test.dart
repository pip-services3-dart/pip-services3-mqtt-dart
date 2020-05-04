import 'dart:async';
import 'dart:io';
import 'package:test/test.dart';
import 'package:pip_services3_commons/pip_services3_commons.dart';
import 'package:pip_services3_messaging/pip_services3_messaging.dart';

import './MessageQueueFixture.dart';
import 'package:pip_services3_mqtt/pip_services3_mqtt.dart';

void main() {
  group('MqttMessageQueue', () {
    MqttMessageQueue queue;
    //MessageQueueFixture fixture;

    var brokerHost = Platform.environment['MOSQUITTO_HOST'] ?? 'localhost';
    var brokerPort = Platform.environment['MOSQUITTO_PORT'] ?? 1883;
    var brokerTopic = Platform.environment['MOSQUITTO_TOPIC'] ?? '/test';
    if (brokerHost == '' && brokerPort == '') {
      return;
    }

    var queueConfig = ConfigParams.fromTuples([
      'connection.protocol',
      'mqtt',
      'connection.host',
      brokerHost,
      'connection.port',
      brokerPort,
      'connection.topic',
      brokerTopic
    ]);

    setUpAll(() async {
      queue = MqttMessageQueue();
      queue.configure(queueConfig);

      //fixture = MessageQueueFixture(queue);

      await queue.open(null);
      await queue.clear(null);
    });

    tearDownAll(() async {
      await queue.close(null);
    });

    test('Receive and Send Message', () async {
      var envelop1 = MessageEnvelope('123', '/test', 'Test message');
      MessageEnvelope envelop2;

      Future.delayed(Duration(milliseconds: 500), () async {
        await queue.send(null, envelop1);
      });

      envelop2 = await queue.receive(null, 10000);

      expect(envelop2, isNotNull);
      expect(envelop1.message, isNotNull);
      expect(envelop2.message, isNotNull);
      expect(envelop1.message.toString(), envelop2.message.toString());
    });

    test('On Message', () async {
      var envelope1 = MessageEnvelope('123', '/test', 'Test message');
      MessageEnvelope envelope2;
      var reciver = TestMessageReciver();
      queue.beginListen(null, reciver);

      await Future.delayed(Duration(milliseconds: 1000), () {});
      await queue.send(null, envelope1);

      await Future.delayed(Duration(milliseconds: 1000), () {});

      envelope2 = reciver.message;
      expect(envelope2, isNotNull);
      expect(envelope1.message_type, envelope2.message_type);
      expect(envelope1.message, envelope2.message);
      //expect(envelope1.correlation_id, envelope2.correlation_id);

      queue.endListen(null);
    });

/*
     test('Send Receive Message', () async {
      await fixture.testSendReceiveMessage();
    });

    test('Receive Send Message', () async {
      await fixture.testReceiveSendMessage();
    });

    test('Receive And Complete Message', () async {
      await fixture.testReceiveCompleteMessage();
    });

    test('Receive And Abandon Message', () async {
      await fixture.testReceiveAbandonMessage();
    });

    test('Send Peek Message', () async {
      await fixture.testSendPeekMessage();
    });

    test('Peek No Message', () async {
      await fixture.testPeekNoMessage();
    });

    test('Move To Dead Message', () async {
      await fixture.testMoveToDeadMessage();
    });

    test('On Message', () async {
      await fixture.testOnMessage();
    });
    */
  });
}
