import 'IMqttMessageListener.dart';

class MqttSubscription {
  String topic;
  bool filter;
  Map<String, dynamic> options;
  IMqttMessageListener listener;

  MqttSubscription(
      {required this.topic,
      required this.filter,
      required this.options,
      required this.listener});
}
