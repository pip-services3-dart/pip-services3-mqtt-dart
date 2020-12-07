import 'dart:async';
import 'package:mqtt_client/mqtt_client.dart';
import 'package:mqtt_client/mqtt_server_client.dart';
import 'package:pip_services3_components/pip_services3_components.dart';
import 'package:pip_services3_messaging/pip_services3_messaging.dart';
import '../connect/MqttConnectionResolver.dart';

///Message queue that sends and receives messages via MQTT message broker.
///
///MQTT is a popular light-weight protocol to communicate IoT devices.
///
///### Configuration parameters ###
///
/// - [topic]:                        name of MQTT topic to subscribe
/// - [qos]:                          QoS from 0 to 2. Default 0
/// - [connection(s)]:
///   - [discovery_key]:               (optional) a key to retrieve the connection from [IDiscovery](https://pub.dev/documentation/pip_services3_components/latest/pip_services3_components/IDiscovery-class.html)
///   - [host]:                        host name or IP address
///   - [port]:                        port number
///   - [uri]:                         resource URI or connection string with all parameters in it
/// - [credential(s)]:
///   - [store_key]:                   (optional) a key to retrieve the credentials from [ICredentialStore](https://pub.dev/documentation/pip_services3_components/latest/pip_services3_components/ICredentialStore-class.html)
///   - [username]:                    user name
///   - [password]:                    user password
///
///### References ###
///
///- *:logger:*:*:1.0             (optional) [ILogger](https://pub.dev/documentation/pip_services3_components/latest/pip_services3_components/ILogger-class.html) components to pass log messages
///- *:counters:*:*:1.0           (optional) [ICounters](https://pub.dev/documentation/pip_services3_components/latest/pip_services3_components/ICounters-class.html) components to pass collected measurements
///- *:discovery:*:*:1.0          (optional) [IDiscovery](https://pub.dev/documentation/pip_services3_components/latest/pip_services3_components/IDiscovery-class.html) services to resolve connections
///- *:credential-store:*:*:1.0   (optional) Credential stores to resolve credentials
///
///See [MessageQueue](https://pub.dev/documentation/pip_services3_messaging/latest/pip_services3_messaging/MessageQueue-class.html)
///See [MessagingCapabilities](https://pub.dev/documentation/pip_services3_messaging/latest/pip_services3_messaging/MessagingCapabilities-class.html)
///
///### Example ###
///```dart
///    var queue = MqttMessageQueue('myqueue');
///    queue.configure(ConfigParams.fromTuples([
///      'topic', 'mytopic',
///      'connection.protocol', 'mqtt'
///      'connection.host', 'localhost'
///      'connection.port', 1883
///    ]));
///
///    await queue.open('123');
///        ...
///
///    await queue.send('123', MessageEnvelope(null, 'mymessage', 'ABC'));
///
///    var message await = queue.receive('123')
///        if (message != null) {
///           ...
///           await queue.complete('123', message);
///        }
/// ```

class MqttMessageQueue extends MessageQueue {
  MqttServerClient _client;
  var _qos = MqttQos.atMostOnce;
  String _topic;
  bool _subscribed = false;
  final _optionsResolver = MqttConnectionResolver();
  IMessageReceiver _receiver;
  var _messages = <MessageEnvelope>[];

  ///Creates a new instance of the message queue.
  ///
  /// - [name]  (optional) a queue name.
  MqttMessageQueue([String name]) : super(name) {
    capabilities = MessagingCapabilities(
        false, true, true, true, true, false, false, false, true);
  }

  ///Checks if the component is opened.
  ///
  ///Returns true if the component has been opened and false otherwise.
  @override
  bool isOpen() {
    return _client != null;
  }

  ///Opens the component with given connection and credential parameters.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// - [connection]        connection parameters
  /// - [credential]        credential parameters
  /// Return 			          Future that receives null no errors occured.
  /// Throws error
  @override
  Future openWithParams(String correlationId, ConnectionParams connection,
      CredentialParams credential) async {
    _topic = connection.getAsString('topic');
    // get QoS settings
    var qos = connection.getAsIntegerWithDefault('qos', 0);
    switch (qos) {
      case 0:
        {
          _qos = MqttQos.atMostOnce;
          break;
        }
      case 1:
        {
          _qos = MqttQos.atLeastOnce;
          break;
        }
      case 2:
        {
          _qos = MqttQos.exactlyOnce;
          break;
        }
    }

    var options =
        await _optionsResolver.compose(correlationId, connection, credential);
    var host = options['host'];
    var port = int.parse(options['port']);
    var client = MqttServerClient.withPort(host, '', port);
    client.logging(on: false);
    client.keepAlivePeriod = 20;

    /// Set auto reconnect
    client.autoReconnect = true;
    client.setProtocolV311();

    var username = options['username'];
    var password = options['password'];
    try {
      if (username != null && password != null) {
        await client.connect(username, password);
      } else {
        await client.connect();
      }
    } catch (err) {
      logger.error(correlationId, err, 'Can\'t open MQTT client');
      client.disconnect();
      rethrow;
    }
    _client = client;
  }

  ///Closes component and frees used resources.
  ///
  /// - [correlationId] 	(optional) transaction id to trace execution through call chain.
  /// Returns 			Future that receives error or null no errors occured.
  @override
  Future close(String correlationId) async {
    if (_client != null) {
      _messages = <MessageEnvelope>[];
      _subscribed = false;
      _receiver = null;
      _client.unsubscribe(_topic);
      _client.disconnect();
      _client = null;
      logger.trace(correlationId, 'Closed queue %s', [this]);
    }
  }

  ///Clears component state.
  ///
  /// - [correlationId] 	(optional) transaction id to trace execution through call chain.
  /// Returns 			Future that receives error or null no errors occured.
  @override
  Future clear(String correlationId) async {
    _messages = <MessageEnvelope>[];
  }

  ///Reads the current number of messages in the queue to be delivered.
  ///
  /// Returns      Future that receives number of messages
  /// Throws error.
  @override
  Future<int> readMessageCount() async {
    // Subscribe to get messages
    subscribe();
    return _messages.length;
  }

  /// Sends a message into the queue.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// - [envelope]          a message envelop to be sent.
  /// Returns               Future that receives error or null for success.
  @override
  Future send(String correlationId, MessageEnvelope envelop) async {
    counters.incrementOne('queue.' + getName() + '.sent_messages');
    logger.debug(envelop.correlation_id, 'Sent message %s via %s',
        [envelop.toString(), toString()]);
    final builder = MqttClientPayloadBuilder();
    builder.addString(envelop.message);
    _client.publishMessage(_topic, _qos, builder.payload);
  }

  ///Peeks a single incoming message from the queue without removing it.
  ///If there are no messages available in the queue it returns null.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// Returns               Future that receives a message
  /// Throws error.
  @override
  Future<MessageEnvelope> peek(String correlationId) async {
    // Subscribe to get messages
    subscribe();

    if (_messages.isNotEmpty) {
      return _messages[0];
    }
    return null;
  }

  ///Peeks multiple incoming messages from the queue without removing them.
  ///If there are no messages available in the queue it returns an empty list.
  ///
  ///Important: This method is not supported by MQTT.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// - me[ssageCount      a maximum number of messages to peek.
  /// Returns          Future that receives a list with messages
  /// Throws error.

  @override
  Future<List<MessageEnvelope>> peekBatch(
      String correlationId, int messageCount) async {
    // Subscribe to get messages
    subscribe();
    return _messages;
  }

  ///Receives an incoming message and removes it from the queue.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// - [waitTimeout]       a timeout in milliseconds to wait for a message to come.
  /// Returns          Future that receives a message
  /// Throws error.
  @override
  Future<MessageEnvelope> receive(String correlationId, int waitTimeout) async {
    MessageEnvelope message;

    // Subscribe to get messages
    subscribe();

    // Return message immediately if it exist
    if (_messages.isNotEmpty) {
      message = _messages.removeAt(0);
      return message;
    }

    // Otherwise wait and return
    var checkIntervalMs = 100;

    for (var i = 0; i < waitTimeout;) {
      if (_client == null) {
        break;
      }
      await Future.delayed(Duration(milliseconds: checkIntervalMs));
      i = i + checkIntervalMs;
      if (_messages.isNotEmpty) {
        message = _messages.removeAt(0);
        break;
      }
    }
    return message;
  }

  ///Renews a lock on a message that makes it invisible from other receivers in the queue.
  ///This method is usually used to extend the message processing time.
  ///
  ///Important: This method is not supported by MQTT.
  ///
  /// - [message]       a message to extend its lock.
  /// - [lockTimeout]   a locking timeout in milliseconds.
  /// Returns      (optional) Future that receives an null for success.
  /// Throws error
  @override
  Future renewLock(MessageEnvelope message, int lockTimeout) async {
    // Not supported
    return null;
  }

  ///Permanently removes a message from the queue.
  ///This method is usually used to remove the message after successful processing.
  ///
  ///Important: This method is not supported by MQTT.
  ///
  /// - [message]   a message to remove.
  /// Returns  (optional) Future that receives an null for success.
  /// Throws error
  @override
  Future complete(MessageEnvelope message) {
    // Not supported
    return null;
  }

  ///Returnes message into the queue and makes it available for all subscribers to receive it again.
  ///This method is usually used to return a message which could not be processed at the moment
  ///to repeat the attempt. Messages that cause unrecoverable errors shall be removed permanently
  ///or/and send to dead letter queue.
  ///
  ///Important: This method is not supported by MQTT.
  ///
  /// - [message]   a message to return.
  /// Returns  (optional) Future that receives an null for success.
  /// Throws error
  @override
  Future abandon(MessageEnvelope message) {
    // Not supported
    return null;
  }

  ///Permanently removes a message from the queue and sends it to dead letter queue.
  ///
  ///Important: This method is not supported by MQTT.
  ///
  /// - [message]   a message to be removed.
  /// Returns  (optional) Future that receives an null for success.
  /// Throws error
  @override
  Future moveToDeadLetter(MessageEnvelope message) {
    // Not supported
    return null;
  }

  MessageEnvelope _toMessage(String topic, message, MqttPublishMessage packet) {
    var envelop = MessageEnvelope(null, topic, message);
    envelop.message_id =
        packet.payload.variableHeader.messageIdentifier.toString();

    return envelop;
  }

  ///Subscribes to the topic.

  void subscribe() {
    // Exit if already subscribed or
    if (_subscribed && _client == null) {
      return;
    }

    logger.trace(null, 'Started listening messages at %s', [toString()]);

    _client.updates.listen((List<MqttReceivedMessage<MqttMessage>> msg) async {
      final MqttPublishMessage packet = msg[0].payload;
      final message =
          MqttPublishPayload.bytesToStringAsString(packet.payload.message);
      final topic = msg[0].topic;
      var envelop = _toMessage(topic, message, packet);

      counters.incrementOne('queue.' + getName() + '.received_messages');
      logger.debug(envelop.correlation_id, 'Received message %s via %s',
          [message, toString()]);

      if (_receiver != null) {
        try {
          await _receiver.receiveMessage(envelop, this);
        } catch (ex) {
          logger.error(null, ex, 'Failed to receive the message');
        }
      } else {
        // Keep message queue managable
        while (_messages.length > 1000) {
          _messages.removeAt(0); // shift();
        }

        // Push into the message queue
        _messages.add(envelop);
      }
    });

    // Subscribe to the topic
    try {
      _client.subscribe(_topic, _qos);
    } catch (err) {
      logger.error(null, err, 'Failed to subscribe to topic ' + _topic);
    }
    _subscribed = true;
  }

  ///Listens for incoming messages and blocks the current thread until queue is closed.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// - [receiver]          a receiver to receive incoming messages.
  ///
  ///See [IMessageReceiver](https://pub.dev/documentation/pip_services3_messaging/latest/pip_services3_messaging/IMessageReceiver-class.html)
  ///See [receive]
  @override
  void listen(String correlationId, IMessageReceiver receiver) async {
    _receiver = receiver;

    // Pass all cached messages
    for (; _messages.isNotEmpty && _receiver != null;) {
      var message = _messages.removeAt(0);
      await receiver.receiveMessage(message, this);
    }
    subscribe();
  }

  ///Ends listening for incoming messages.
  ///When this method is call [listen] unblocks the thread and execution continues.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  @override
  void endListen(String correlationId) {
    _receiver = null;

    if (_subscribed) {
      _client.unsubscribe(_topic);
      _subscribed = false;
    }
  }
}
