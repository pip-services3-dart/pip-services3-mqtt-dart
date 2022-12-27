import 'package:pip_services3_commons/pip_services3_commons.dart';
import 'package:pip_services3_messaging/pip_services3_messaging.dart';

import '../../pip_services3_mqtt.dart';

/// Creates [MqttMessageQueue] components by their descriptors.
/// Name of created message queue is taken from its descriptor.
///
/// See [Factory](https://pub.dev/documentation/pip_services3_components/latest/pip_services3_components/Factory-class.html)
/// See [MqttMessageQueue]
class MqttMessageQueueFactory extends MessageQueueFactory {
  static final Descriptor MqttQueueDescriptor =
      Descriptor('pip-services', 'message-queue', 'mqtt', '*', '1.0');

  MqttMessageQueueFactory() : super() {
    register(MqttMessageQueueFactory.MqttQueueDescriptor, (locator) {
      return createQueue(locator?.getName());
    });
  }

  /// Creates a message queue component and assigns its name.
  /// - [name] a name of the created message queue.
  @override
  IMessageQueue createQueue(String name) {
    var queue = MqttMessageQueue(name);

    if (config_ != null) {
      queue.configure(config_!);
    }
    if (references_ != null) {
      queue.setReferences(references_!);
    }

    return queue;
  }
}
