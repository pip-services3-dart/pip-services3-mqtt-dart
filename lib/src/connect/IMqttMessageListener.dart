abstract class IMqttMessageListener {
  void onMessage(String topic, String message, packet);
}
