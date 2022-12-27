import 'package:test/test.dart';
import 'package:pip_services3_commons/pip_services3_commons.dart';

import 'package:pip_services3_mqtt/pip_services3_mqtt.dart';

void main() {
  group('MqttConnectionResolver', () {
    test('Single Connection', () async {
      var resolver = MqttConnectionResolver();
      resolver.configure(ConfigParams.fromTuples([
        'connection.protocol',
        'mqtt',
        'connection.host',
        'localhost',
        'connection.port',
        1883
      ]));

      var connection = await resolver.resolve(null);
      expect('mqtt://localhost:1883', connection['uri']);
      expect(connection['username'], isNull);
      expect(connection['password'], isNull);
    });

    test('Cluster Connection', () async {
      var resolver = MqttConnectionResolver();
      resolver.configure(ConfigParams.fromTuples([
        'connections.0.protocol',
        'mqtt',
        'connections.0.host',
        'server1',
        'connections.0.port',
        1883,
        'connections.1.protocol',
        'mqtt',
        'connections.1.host',
        'server2',
        'connections.1.port',
        1883,
        'connections.2.protocol',
        'mqtt',
        'connections.2.host',
        'server3',
        'connections.2.port',
        1883,
      ]));

      var connection = await resolver.resolve(null);

      expect(connection['uri'], isNotNull);
      expect(connection['username'], isNull);
      expect(connection['password'], isNull);
    });

    test('Cluster Connection with Auth', () async {
      var resolver = MqttConnectionResolver();
      resolver.configure(ConfigParams.fromTuples([
        'connections.0.protocol',
        'mqtt',
        'connections.0.host',
        'server1',
        'connections.0.port',
        1883,
        'connections.1.protocol',
        'mqtt',
        'connections.1.host',
        'server2',
        'connections.1.port',
        1883,
        'connections.2.protocol',
        'mqtt',
        'connections.2.host',
        'server3',
        'connections.2.port',
        1883,
        'credential.username',
        'test',
        'credential.password',
        'pass123',
      ]));

      var connection = await resolver.resolve(null);

      expect(connection['uri'], isNotNull);
      expect(connection['username'], 'test');
      expect(connection['password'], 'pass123');
    });

    test('Cluster URI', () async {
      var resolver = MqttConnectionResolver();
      resolver.configure(ConfigParams.fromTuples([
        'connection.uri',
        'mqtt://server1:1883',
        'credential.username',
        'test',
        'credential.password',
        'pass123'
      ]));

      var connection = await resolver.resolve(null);
      expect(connection['uri'], isNotNull);
      expect('test', connection['username']);
      expect('pass123', connection['password']);
    });
  });
}
