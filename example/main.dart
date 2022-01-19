import 'dart:async';
import 'dart:io';
import 'package:pip_services3_commons/pip_services3_commons.dart';
import 'package:pip_services3_messaging/pip_services3_messaging.dart';

import 'package:pip_services3_mqtt/pip_services3_mqtt.dart';

class TestMessageReciver implements IMessageReceiver {
  MessageEnvelope? message;

  @override
  Future receiveMessage(MessageEnvelope envelope, IMessageQueue queue) async {
    message = envelope;
    return null;
  }
}

void main() async {
  late MqttMessageQueue queue;
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
    'topic',
    brokerTopic
  ]);
  queue = MqttMessageQueue();
  queue.configure(queueConfig);

  await queue.open(null);
  await queue.clear(null);
  // Synchronus communication
  var envelope1 = MessageEnvelope('123', brokerTopic, 'Test message');
  MessageEnvelope? envelope2;

  await queue.send(null, envelope1);
  var count = await queue.readMessageCount(); // count = 1
  envelope2 =
      await queue.receive(null, 10000); // envelope2.message = envelope1.message

  //====================================================================
  // Asynchronus communicaton
  var reciver = TestMessageReciver();
  queue.beginListen(null, reciver);
  await Future.delayed(Duration(milliseconds: 1000), () {});
  await queue.send(null, envelope1);
  await Future.delayed(Duration(milliseconds: 1000), () {});
  // read recived message
  envelope2 = reciver.message; // envelope1.message = envelope2.message
  queue.endListen(null);

  await queue.close(null);
}
