import 'dart:io';

import 'package:mqtt_client/mqtt_client.dart' as mqtt_client;
import 'package:mqtt_client/mqtt_server_client.dart' as mqtt;
import 'package:pip_services3_commons/pip_services3_commons.dart';
import 'package:pip_services3_components/pip_services3_components.dart';
import 'package:pip_services3_messaging/pip_services3_messaging.dart';

import '../../pip_services3_mqtt.dart';
import 'IMqttMessageListener.dart';
import 'MqttSubscription.dart';

/// Connection to MQTT message broker.
///
/// MQTT is a popular light-weight protocol to communicate IoT devices.
///
/// ### Configuration parameters ###
///
/// - [client_id]:               (optional) name of the client id
/// - [connection(s)]:
///   - [discovery_key]:               (optional) a key to retrieve the connection from [IDiscovery](https://pub.dev/documentation/pip_services3_components/latest/pip_services3_components/IDiscovery-class.html)
///   - [host]:                        host name or IP address
///   - [port]:                        port number
///   - [uri]:                         resource URI or connection string with all parameters in it
/// - [credential(s)]:
///   - [store_key]:                   (optional) a key to retrieve the credentials from [ICredentialStore](https://pub.dev/documentation/pip_services3_components/latest/pip_services3_components/ICredentialStore-class.html)
///   - [username]:                    user name
///   - [password]:                    user password
/// - [options]:
///   - [retry_connect]:        (optional) turns on/off automated reconnect when connection is log (default: true)
///   - [connect_timeout]:      (optional) number of milliseconds to wait for connection (default: 30000)
///   - [reconnect_timeout]:    (optional) number of milliseconds to wait on each reconnection attempt (default: 1000)
///   - [keepalive_timeout]:    (optional) number of milliseconds to ping broker while inactive (default: 3000)
///
/// ### References ###
///
/// - *:logger:*:*:1.0             (optional)  [ILogger](https://pub.dev/documentation/pip_services3_components/latest/pip_services3_components/ILogger-class.html) components to pass log messages
/// - *:counters:*:*:1.0           (optional) [ICounters](https://pub.dev/documentation/pip_services3_components/latest/pip_services3_components/ICounters-class.html) components to pass collected measurements
/// - *:discovery:*:*:1.0          (optional) [IDiscovery](https://pub.dev/documentation/pip_services3_components/latest/pip_services3_components/IDiscovery-class.html) services to resolve connections
/// - *:credential-store:*:*:1.0   (optional) Credential stores to resolve credentials
///
/// See [MessageQueue]
/// See [MessagingCapabilities]
///
class MqttConnection
    implements
        IMessageQueueConnection,
        IReferenceable,
        IConfigurable,
        IOpenable {
  final _defaultConfig = ConfigParams.fromTuples([
    // connections.*
    // credential.*

    'client_id',
    null,
    'options.retry_connect',
    true,
    'options.connect_timeout',
    30000,
    'options.reconnect_timeout',
    1000,
    'options.keepalive_timeout',
    60000
  ]);

  /// The logger.
  var logger_ = CompositeLogger();

  /// The connection resolver.
  var connectionResolver_ = MqttConnectionResolver();

  /// The configuration options.
  var options_ = ConfigParams();

  /// The NATS connection pool object.
  mqtt.MqttServerClient? connection_;

  /// Topic subscriptions
  List<MqttSubscription> subscriptions_ = [];

  String clientId_ = Platform.localHostname;
  bool retryConnect_ = true;
  int connectTimeout_ = 30000;
  int keepAliveTimeout_ = 60000;
  int reconnectTimeout_ = 1000;

  /// Configures component by passing configuration parameters.
  ///
  /// - [config]    configuration parameters to be set.
  @override
  void configure(ConfigParams config) {
    config = config.setDefaults(_defaultConfig);
    connectionResolver_.configure(config);
    options_ = options_.override(config.getSection('options'));

    clientId_ = config.getAsStringWithDefault('client_id', clientId_);
    retryConnect_ =
        config.getAsBooleanWithDefault('options.retry_connect', retryConnect_);
    connectTimeout_ = config.getAsIntegerWithDefault(
        'options.max_reconnect', connectTimeout_);
    reconnectTimeout_ = config.getAsIntegerWithDefault(
        'options.reconnect_timeout', reconnectTimeout_);
    keepAliveTimeout_ = config.getAsIntegerWithDefault(
        'options.keepalive_timeout', keepAliveTimeout_);
  }

  /// Sets references to dependent components.
  ///
  /// - [references] 	references to locate the component dependencies.
  @override
  void setReferences(IReferences references) {
    logger_.setReferences(references);
    connectionResolver_.setReferences(references);
  }

  /// Checks if the component is opened.
  ///
  /// Returns true if the component has been opened and false otherwise.
  @override
  bool isOpen() {
    return connection_ != null;
  }

  /// Opens the component.
  ///
  /// - [correlationId] 	(optional) transaction id to trace execution through call chain.
  @override
  Future open(String? correlationId) async {
    if (connection_ != null) {
      return;
    }

    var options = await connectionResolver_.resolve(correlationId);

    // todo
    // options['reconnectPeriod'] = reconnectTimeout_.toString();

    var client = mqtt.MqttServerClient.withPort(
        options.getAsString('host'), clientId_, options.getAsInteger('port'));
    client.keepAlivePeriod = keepAliveTimeout_ ~/ 1000;
    client.autoReconnect = retryConnect_;
    client.resubscribeOnAutoReconnect = retryConnect_;
    client.setProtocolV311();

    client.onConnected = () {
      connection_ = client;
      logger_.debug(correlationId,
          'Connected to MQTT broker at ' + options.getAsString('uri'));
    };

    var username = options['username'];
    var password = options['password'];

    try {
      await client
          .connect(username, password)
          .timeout(Duration(milliseconds: connectTimeout_));
    } catch (ex) {
      logger_.error(correlationId, ex as Exception,
          'Failed to connect to MQTT broker at ' + options.getAsString('uri'));
      var err = ConnectionException(correlationId, 'CONNECT_FAILED',
              'Connection to MQTT broker failed')
          .withCause(ex);
      throw err;
    }
  }

  /// Closes component and frees used resources.
  ///
  /// - [correlationId] 	(optional) transaction id to trace execution through call chain.
  @override
  Future close(String? correlationId) async {
    if (connection_ == null) {
      return;
    }

    connection_!.disconnect();
    connection_ = null;
    subscriptions_ = [];
    logger_.debug(correlationId, 'Disconnected from MQTT broker');
  }

  mqtt.MqttServerClient? getConnection() {
    return connection_;
  }

  /// Reads a list of registered queue names.
  /// If connection doesn't support this function returnes an empty list.
  ///
  /// Returns a list with registered queue names.
  ///
  /// Important: This method is not supported by MQTT.
  @override
  Future<List<String>> readQueueNames() async {
    /// Not supported
    return [];
  }

  /// Creates a message queue.
  /// If connection doesn't support this function it exists without error.
  ///
  /// - [name] the name of the queue to be created.
  ///
  /// Important: This method is not supported by MQTT.
  @override
  Future<void> createQueue(String name) async {
    // Not supported
  }

  /// Deletes a message queue.
  /// If connection doesn't support this function it exists without error.
  /// - [name] the name of the queue to be deleted.
  ///
  /// Important: This method is not supported by MQTT.
  ///
  @override
  Future<void> deleteQueue(String name) async {
    // Not supported
  }

  /// Checks if connection is open
  void checkOpen() {
    if (isOpen()) return;

    throw InvalidStateException(null, 'NOT_OPEN', 'Connection was not opened');
  }

  /// Publish a message to a specified topic
  /// - [topic] a topic name
  /// - [data] a message to be published
  /// - [options] publishing options
  Future publish(
      String topic, String data, Map<String, dynamic> options) async {
    // Check for open connection
    checkOpen();

    final builder = mqtt_client.MqttClientPayloadBuilder();
    builder.addString(data);

    await Future(() async {
      connection_!.publishMessage(topic, options['qos'], builder.payload!,
          retain: options['retain']);
    }).then((value) {
      connection_?.published?.listen((mqtt_client.MqttPublishMessage message) {
        for (var subscription in subscriptions_) {
          // Todo: Implement proper filtering by wildcards?
          if (subscription.filter && topic != subscription.topic) {
            continue;
          }
          var data = String.fromCharCodes(message.payload.message);

          subscription.listener
              .onMessage(message.variableHeader!.topicName, data, message);
        }
      });
    });
  }

  Future subscribe(String topic, Map<String, dynamic> options,
      IMqttMessageListener listener) async {
    // Check for open connection
    checkOpen();

    // Subscribe to topic
    await Future(() {
      var res = connection_!.subscribe(
        topic,
        options['qos']!,
      );
      if (res == null) throw Exception('Subscribe error');
    });

    // Determine if messages shall be filtered (topic without wildcarts)
    var filter = !topic.contains('*');

    // Add the subscription
    var subscription = MqttSubscription(
        topic: topic, options: options, filter: filter, listener: listener);
    subscriptions_.add(subscription);
  }

  Future unsubscribe(String topic, IMqttMessageListener listener) async {
    // Find the subscription index
    var index = subscriptions_
        .indexWhere((s) => s.topic == topic && s.listener == listener);
    if (index < 0) {
      return;
    }

    // Remove the subscription
    subscriptions_.removeAt(index);

    // Check if there other subscriptions to the same topic
    index = subscriptions_.indexWhere((s) => s.topic == topic);

    // Unsubscribe from topic if connection is still open
    if (connection_ != null && index < 0) {
      await Future(() {
        connection_!.unsubscribe(topic);
      });
    }
  }
}
