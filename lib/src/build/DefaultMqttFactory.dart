import 'package:pip_services3_components/pip_services3_components.dart';
import 'package:pip_services3_commons/pip_services3_commons.dart';
import '../queues/MqttMessageQueue.dart';

///Creates [MqttMessageQueue] components by their descriptors.
///
///See [MqttMessageQueue]

class DefaultMqttFactory extends Factory {
  static final descriptor =
      Descriptor('pip-services', 'factory', 'mqtt', 'default', '1.0');
  static final MqttQueueDescriptor =
      Descriptor('pip-services', 'message-queue', 'mqtt', '*', '1.0');

  ///Create a new instance of the factory.
  DefaultMqttFactory() : super() {
    register(DefaultMqttFactory.MqttQueueDescriptor, (locator) {
      return MqttMessageQueue(locator.getName());
    });
  }
}
