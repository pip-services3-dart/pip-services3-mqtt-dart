import 'dart:io';
import 'package:test/test.dart';
import 'package:pip_services3_commons/pip_services3_commons.dart';

import './MessageQueueFixture.dart';
import 'package:pip_services3_mqtt/pip_services3_mqtt.dart';

void main() {
  group('MqttMessageQueue', () {
    late MqttMessageQueue queue;
    late MessageQueueFixture fixture;

    var brokerHost = Platform.environment['MQTT_SERVICE_HOST'] ?? 'localhost';
    var brokerPort = Platform.environment['MQTT_SERVICE_PORT'] ?? 1883;
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
      brokerTopic,
      'options.autosubscribe',
      true,
      'options.serialize_envelope',
      true
    ]);

    setUp(() async {
      queue = MqttMessageQueue();
      queue.configure(queueConfig);

      fixture = MessageQueueFixture(queue);

      await queue.open(null);
      await queue.clear(null);

      // Wait configs
      await Future.delayed(Duration(milliseconds: 500));
    });

    tearDown(() async {
      await queue.close(null);
    });

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
  });
}
