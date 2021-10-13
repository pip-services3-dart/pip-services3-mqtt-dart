import 'package:pip_services3_mqtt/src/build/MqttMessageQueueFactory.dart';
import 'package:test/test.dart';
import 'package:pip_services3_commons/pip_services3_commons.dart';

void main() {
  group('MqttMessageQueueFactory', () {
    test('CreateMessageQueue', () async {
      var factory = MqttMessageQueueFactory();
      var descriptor =
          Descriptor('pip-services', 'message-queue', 'mqtt', 'test', '1.0');

      var canResult = factory.canCreate(descriptor);
      expect(canResult, isNotNull);

      var queue = factory.create(descriptor);
      expect(queue, isNotNull);
      expect('test', queue.getName());
    });
  });
}
