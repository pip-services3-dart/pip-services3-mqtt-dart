import 'dart:async';
import 'dart:convert';
import 'package:mqtt_client/mqtt_client.dart' as mqtt_client;
import 'package:pip_services3_commons/pip_services3_commons.dart';
import 'package:pip_services3_components/pip_services3_components.dart';
import 'package:pip_services3_messaging/pip_services3_messaging.dart';
import 'package:pip_services3_mqtt/src/connect/IMqttMessageListener.dart';
import 'package:pip_services3_mqtt/src/connect/MqttConnection.dart';

/// Message queue that sends and receives messages via MQTT message broker.
///
/// MQTT is a popular light-weight protocol to communicate IoT devices.
///
/// ### Configuration parameters ###
///
///  - [topic]:                        name of MQTT topic to subscribe
///  - [qos]:                          QoS from 0 to 2. Default 0
///  - [connection(s)]:
///    - [discovery_key]:               (optional) a key to retrieve the connection from [IDiscovery](https://pub.dev/documentation/pip_services3_components/latest/pip_services3_components/IDiscovery-class.html)
///    - [host]:                        host name or IP address
///    - [port]:                        port number
///    - [uri]:                         resource URI or connection string with all parameters in it
///  - [credential(s)]:
///    - [store_key]:                   (optional) a key to retrieve the credentials from [ICredentialStore](https://pub.dev/documentation/pip_services3_components/latest/pip_services3_components/ICredentialStore-class.html)
///    - [username]:                    user name
///    - [password]:                    user password
///  - [options]:
///    - [serialize_envelope]:   (optional) true to serialize entire message as JSON, false to send only message payload (default: true)
///    - [autosubscribe]:        (optional) true to automatically subscribe on option (default: false)
///    - [qos]:                  (optional) quality of service level aka QOS (default: 0)
///    - [retain]:               (optional) retention flag for published messages (default: false)
///    - [retry_connect]:        (optional) turns on/off automated reconnect when connection is log (default: true)
///    - [connect_timeout]:      (optional) number of milliseconds to wait for connection (default: 30000)
///    - [reconnect_timeout]:    (optional) number of milliseconds to wait on each reconnection attempt (default: 1000)
///    - [keepalive_timeout]:    (optional) number of milliseconds to ping broker while inactive (default: 3000)
///
/// ### References ###
///
/// - *:logger:*:*:1.0             (optional) [ILogger](https://pub.dev/documentation/pip_services3_components/latest/pip_services3_components/ILogger-class.html) components to pass log messages
/// - *:counters:*:*:1.0           (optional) [ICounters](https://pub.dev/documentation/pip_services3_components/latest/pip_services3_components/ICounters-class.html) components to pass collected measurements
/// - *:discovery:*:*:1.0          (optional) [IDiscovery](https://pub.dev/documentation/pip_services3_components/latest/pip_services3_components/IDiscovery-class.html) services to resolve connections
/// - *:credential-store:*:*:1.0   (optional) Credential stores to resolve credentials
/// - *:connection:mqtt:*:1.0      (optional) Shared connection to MQTT service
///
/// See [MessageQueue](https://pub.dev/documentation/pip_services3_messaging/latest/pip_services3_messaging/MessageQueue-class.html)
/// See [MessagingCapabilities](https://pub.dev/documentation/pip_services3_messaging/latest/pip_services3_messaging/MessagingCapabilities-class.html)
///
/// ### Example ###
/// ```dart
///     var queue = MqttMessageQueue('myqueue');
///     queue.configure(ConfigParams.fromTuples([
///       'topic', 'mytopic',
///       'connection.protocol', 'mqtt'
///       'connection.host', 'localhost'
///       'connection.port', 1883
///     ]));
///
///     await queue.open('123');
///         ...
///
///     await queue.send('123', MessageEnvelope(null, 'mymessage', 'ABC'));
///
///     var message await = queue.receive('123')
///         if (message != null) {
///            ...
///            await queue.complete('123', message);
///         }
///  ```

class MqttMessageQueue extends MessageQueue
    implements
        IMqttMessageListener,
        IReferenceable,
        IUnreferenceable,
        IConfigurable,
        IOpenable,
        ICleanable {
  static final _defaultConfig = ConfigParams.fromTuples([
    'topic',
    null,
    'options.serialize_envelope',
    false,
    'options.autosubscribe',
    false,
    'options.retry_connect',
    true,
    'options.connect_timeout',
    30000,
    'options.reconnect_timeout',
    1000,
    'options.keepalive_timeout',
    60000,
    'options.qos',
    0,
    'options.retain',
    false
  ]);

  ConfigParams? _config;
  IReferences? _references;
  bool _opened = false;
  bool _localConnection = false;

  /// The dependency resolver.
  var _dependencyResolver = DependencyResolver(MqttMessageQueue._defaultConfig);

  /// The MQTT connection component.
  var _logger = CompositeLogger();

  /// The MQTT connection component.
  MqttConnection? _connection;

  bool _serializeEnvelope = false;
  String? _topic;
  var _qos = mqtt_client.MqttQos.atMostOnce;
  bool _retain = false;
  bool _autoSubscribe = false;
  bool _subscribed = false;
  var _messages = <MessageEnvelope>[];
  IMessageReceiver? _receiver;

  ///Creates a new instance of the message queue.
  ///
  /// - [name]  (optional) a queue name.
  MqttMessageQueue([String? name])
      : super(
            name,
            MessagingCapabilities(
                false, true, true, true, true, false, false, false, true));

  /// Configures component by passing configuration parameters.
  ///
  /// - [config]    configuration parameters to be set.
  @override
  void configure(ConfigParams config) {
    config = config.setDefaults(MqttMessageQueue._defaultConfig);
    _config = config;

    _dependencyResolver.configure(config);

    _topic = config.getAsNullableString('topic') ?? _topic;
    _autoSubscribe =
        config.getAsBooleanWithDefault('options.autosubscribe', _autoSubscribe);
    _serializeEnvelope = config.getAsBooleanWithDefault(
        'options.serialize_envelope', _serializeEnvelope);
    var qosVal = config.getAsNullableInteger('options.qos');
    _qos = qosVal != null ? mqtt_client.MqttQos.values[qosVal] : _qos;

    _retain = config.getAsBooleanWithDefault('options.retain', _retain);
  }

  /// Sets references to dependent components.
  ///
  /// - [references] 	references to locate the component dependencies.
  @override
  void setReferences(IReferences references) {
    _references = references;
    _logger.setReferences(references);

    // Get connection
    _dependencyResolver.setReferences(references);
    _connection = _dependencyResolver.getOneOptional('connection');
    // Or create a local one
    if (_connection == null) {
      _connection = _createConnection();
      _localConnection = true;
    } else {
      _localConnection = false;
    }
  }

  /// Unsets (clears) previously set references to dependent components.
  @override
  void unsetReferences() {
    _connection = null;
  }

  MqttConnection _createConnection() {
    var connection = MqttConnection();

    if (_config != null) {
      connection.configure(_config!);
    }

    if (_references != null) {
      connection.setReferences(_references!);
    }

    return connection;
  }

  ///Checks if the component is opened.
  ///
  ///Returns true if the component has been opened and false otherwise.
  @override
  bool isOpen() {
    return _opened;
  }

  /// Opens the component.
  ///
  /// - [correlationId] 	(optional) transaction id to trace execution through call chain.
  @override
  Future open(String? correlationId) async {
    if (_opened) {
      return;
    }

    if (_connection == null) {
      _connection = _createConnection();
      _localConnection = true;
    }

    if (_localConnection != null && _localConnection != false) {
      await _connection!.open(correlationId);
    }

    if (!_connection!.isOpen()) {
      throw ConnectionException(
          correlationId, 'CONNECT_FAILED', 'MQTT connection is not opened');
    }

    // Subscribe right away
    if (_autoSubscribe) {
      await subscribe(correlationId);
    }

    _opened = true;
  }

  ///Opens the component with given connection and credential parameters.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// - [connection]        connection parameters
  /// - [credential]        credential parameters
  /// Return 			          Future that receives null no errors occured.
  /// Throws error
  @override
  Future openWithParams(String? correlationId, ConnectionParams? connection,
      CredentialParams? credential) async {}

  ///Closes component and frees used resources.
  ///
  /// - [correlationId] 	(optional) transaction id to trace execution through call chain.
  /// Returns 			Future that receives error or null no errors occured.
  @override
  Future close(String? correlationId) async {
    if (!_opened) {
      return;
    }

    if (_connection == null) {
      throw InvalidStateException(
          correlationId, 'NO_CONNECTION', 'MQTT connection is missing');
    }

    if (_localConnection) {
      await _connection!.close(correlationId);
    }

    if (_subscribed) {
      // Unsubscribe from the topic
      var topic = getTopic();
      await _connection!.unsubscribe(topic, this);
    }

    _messages = [];
    _opened = false;
    _receiver = null;
  }

  String getTopic() {
    return _topic != null && _topic != '' ? _topic! : getName();
  }

  Future subscribe(String? correlationId) async {
    if (_subscribed) {
      return;
    }

    // Subscribe right away
    var topic = getTopic();

    await _connection!.subscribe(topic, {'qos': _qos}, this);
  }

  Map<String, dynamic>? fromMessage(MessageEnvelope? message) {
    if (message == null) return null;

    var data = message.message;
    if (_serializeEnvelope) {
      message.sent_time = DateTime.now();
      data = json.encode(message.toJSON());
    }

    return {'topic': getName().isNotEmpty ? getName() : _topic, 'data': data};
  }

  MessageEnvelope? _toMessage(String topic, String? data, packet) {
    if (data == null) return null;

    MessageEnvelope? message;
    if (_serializeEnvelope) {
      // var jsonMap = json.decode(data);
      message = MessageEnvelope.fromJSON(data);
    } else {
      message = MessageEnvelope(null, topic, data);
      message.message_id = json.decode(data)['message_id'];
      // message.message_type = topic;
      // message.message = data;
    }

    return message;
  }

  @override
  void onMessage(String topic, String data, packet) {
    // Skip if it came from a wrong topic
    var expectedTopic = getTopic();
    if (!expectedTopic.contains('*') && expectedTopic != topic) {
      return;
    }

    // Deserialize message
    var message = _toMessage(topic, data, packet);
    if (message == null) {
      _logger.error(null, null, 'Failed to read received message');
      return;
    }

    counters.incrementOne('queue.' + getName() + '.received_messages');
    _logger.debug(message.correlation_id, 'Received message %s via %s',
        [message, getName()]);

    // Send message to receiver if its set or put it into the queue
    if (_receiver != null) {
      sendMessageToReceiver(_receiver, message);
    } else {
      _messages.add(message);
    }
  }

  ///Clears component state.
  ///
  /// - [correlationId] 	(optional) transaction id to trace execution through call chain.
  /// Returns 			Future that receives error or null no errors occured.
  @override
  Future clear(String? correlationId) async {
    _messages = <MessageEnvelope>[];
  }

  ///Reads the current number of messages in the queue to be delivered.
  ///
  /// Returns      Future that receives number of messages
  /// Throws error.
  @override
  Future<int> readMessageCount() async {
    // Subscribe to get messages
    return _messages.length;
  }

  ///Peeks a single incoming message from the queue without removing it.
  ///If there are no messages available in the queue it returns null.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// Returns               Future that receives a message
  /// Throws error.
  @override
  Future<MessageEnvelope?> peek(String? correlationId) async {
    checkOpen(correlationId);

    // Subscribe to topic if needed
    await subscribe(correlationId);

    // Peek a message from the top
    MessageEnvelope? message;
    if (_messages.isNotEmpty) {
      message = _messages[0];
    }

    if (message != null) {
      _logger.trace(message.correlation_id, 'Peeked message %s on %s',
          [message, getName()]);
    }

    return message;
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
      String? correlationId, int messageCount) async {
    checkOpen(correlationId);

    // Subscribe to topic if needed
    await subscribe(correlationId);

    // Peek a batch of messages
    var messages = _messages.getRange(0, messageCount).toList();

    _logger.trace(correlationId, 'Peeked %d messages on %s',
        [messages.length, getName()]);

    return messages;
  }

  ///Receives an incoming message and removes it from the queue.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// - [waitTimeout]       a timeout in milliseconds to wait for a message to come.
  /// Returns          Future that receives a message
  /// Throws error.
  @override
  Future<MessageEnvelope?> receive(
      String? correlationId, int waitTimeout) async {
    checkOpen(correlationId);

    // Subscribe to topic if needed
    await subscribe(correlationId);

    MessageEnvelope? message;

    // Return message immediately if it exist
    if (_messages.isNotEmpty) {
      message = _messages.isNotEmpty ? _messages.removeAt(0) : null;
      return message;
    }

    // Otherwise wait and return
    var checkInterval = 100;
    var elapsedTime = 0;
    while (true) {
      var test = isOpen() && elapsedTime < waitTimeout && message == null;
      if (!test) break;

      message = await Future<MessageEnvelope?>.delayed(
          Duration(milliseconds: checkInterval), () async {
        message = _messages.isNotEmpty ? _messages.removeAt(0) : null;
        return message;
      });

      elapsedTime += checkInterval;
    }

    return message;
  }

  /// Sends a message into the queue.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// - [envelope]          a message envelop to be sent.
  /// Returns               Future that receives error or null for success.
  @override
  Future send(String? correlationId, MessageEnvelope message) async {
    checkOpen(correlationId);

    counters.incrementOne('queue.' + getName() + '.sent_messages');
    _logger.debug(message.correlation_id, 'Sent message %s via %s',
        [message.toString(), toString()]);

    var msg = fromMessage(message);
    var options = {'qos': _qos, 'retain': _retain};
    await _connection!.publish(msg!['topic'], msg['data'], options);
  }

  /// Renews a lock on a message that makes it invisible from other receivers in the queue.
  /// This method is usually used to extend the message processing time.
  ///
  /// Important: This method is not supported by MQTT.
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
  Future complete(MessageEnvelope message) async {
    // Not supported
    return null;
  }

  /// Returnes message into the queue and makes it available for all subscribers to receive it again.
  /// This method is usually used to return a message which could not be processed at the moment
  /// to repeat the attempt. Messages that cause unrecoverable errors shall be removed permanently
  /// or/and send to dead letter queue.
  ///
  /// Important: This method is not supported by MQTT.
  ///
  /// - [message]   a message to return.
  /// Returns  (optional) Future that receives an null for success.
  /// Throws error
  @override
  Future abandon(MessageEnvelope message) async {
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
  Future moveToDeadLetter(MessageEnvelope message) async {
    // Not supported
    return null;
  }

  void sendMessageToReceiver(
      IMessageReceiver? receiver, MessageEnvelope? message) async {
    var correlationId = message != null ? message.correlation_id : null;
    if (message == null || receiver == null) {
      _logger.warn(correlationId, 'MQTT message was skipped.');
      return;
    }

    await _receiver!.receiveMessage(message, this).catchError((err) =>
        {_logger.error(correlationId, err, 'Failed to process the message')});
  }

  ///Listens for incoming messages and blocks the current thread until queue is closed.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// - [receiver]          a receiver to receive incoming messages.
  ///
  ///See [IMessageReceiver](https://pub.dev/documentation/pip_services3_messaging/latest/pip_services3_messaging/IMessageReceiver-class.html)
  ///See [receive]
  @override
  void listen(String? correlationId, IMessageReceiver receiver) async {
    checkOpen(correlationId);

    // Subscribe to topic if needed
    await subscribe(correlationId).then((value) {
      _logger.trace(null, 'Started listening messages at %s', [getName()]);

      // Resend collected messages to receiver
      while (isOpen() && _messages.isNotEmpty) {
        var message = _messages.isNotEmpty ? _messages.removeAt(0) : null;
        if (message != null) {
          sendMessageToReceiver(receiver, message);
        }
      }

      // Set the receiver
      if (isOpen()) {
        _receiver = receiver;
      }
    });
  }

  ///Ends listening for incoming messages.
  ///When this method is call [listen] unblocks the thread and execution continues.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  @override
  void endListen(String? correlationId) {
    _receiver = null;
  }
}
