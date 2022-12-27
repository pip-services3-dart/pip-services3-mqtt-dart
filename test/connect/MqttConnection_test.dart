import 'dart:io';
import 'package:pip_services3_mqtt/src/connect/MqttConnection.dart';
import 'package:test/test.dart';
import 'package:pip_services3_commons/pip_services3_commons.dart';

void main() {
  group('MqttConnection', () {
    late MqttConnection connection;

    var brokerHost = Platform.environment['MQTT_SERVICE_HOST'] ?? 'localhost';
    var brokerPort = Platform.environment['MQTT_SERVICE_PORT'] ?? 1883;
    if (brokerHost == '' && brokerPort == '') {
      return;
    }
    var brokerTopic = Platform.environment['MQTT_TOPIC'] ?? 'test';
    var brokerUser = Platform.environment['MQTT_USER'];
    var brokerPass = Platform.environment['MQTT_PASS'];
    var brokerToken = Platform.environment['MQTT_TOKEN'];

    setUp(() async {
      var config = ConfigParams.fromTuples([
        'topic',
        brokerTopic,
        'connection.protocol',
        'mqtt',
        'connection.host',
        brokerHost,
        'connection.port',
        brokerPort,
        'credential.username',
        brokerUser,
        'credential.password',
        brokerPass,
        'credential.token',
        brokerToken,
      ]);

      connection = MqttConnection();
      connection.configure(config);
    });

    test('Open/Close', () async {
      await connection.open(null);
      expect(connection.isOpen(), isTrue);
      expect(connection.getConnection(), isNotNull);

      await connection.close(null);
      expect(connection.isOpen(), isFalse);
      expect(connection.getConnection(), isNull);
    });
  });
}
