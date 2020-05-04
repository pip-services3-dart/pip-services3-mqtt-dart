import 'dart:async';

import 'package:pip_services3_commons/pip_services3_commons.dart';
import 'package:pip_services3_components/pip_services3_components.dart';

///Helper class that resolves MQTT connection and credential parameters,
///validates them and generates connection options.
///
/// ### Configuration parameters ###
///
///- [connection(s)]:
///  - [discovery_key]:               (optional) a key to retrieve the connection from [IDiscovery]
///  - [host]:                        host name or IP address
///  - [port]:                        port number
///  - [uri]:                         resource URI or connection string with all parameters in it
///- [credential(s)]:
///  - [store_key]:                   (optional) a key to retrieve the credentials from [ICredentialStore]
///  - [username]:                    user name
///  - [password]:                    user password
///
///### References ###
///
///- *:discovery:\*:\*:1.0          (optional) [IDiscovery] services to resolve connections
///- *:credential-store:\*:\*:1.0   (optional) Credential stores to resolve credentials

class MqttConnectionResolver implements IReferenceable, IConfigurable {
  ///The connections resolver.
  final connectionResolver = ConnectionResolver();

  ///The credentials resolver.
  final credentialResolver = CredentialResolver();

  ///Configures component by passing configuration parameters.
  ///
  /// - [config]    configuration parameters to be set.
  @override
  void configure(ConfigParams config) {
    connectionResolver.configure(config);
    credentialResolver.configure(config);
  }

  ///Sets references to dependent components.
  ///
  /// - [references] 	references to locate the component dependencies.
  @override
  void setReferences(IReferences references) {
    connectionResolver.setReferences(references);
    credentialResolver.setReferences(references);
  }

  void _validateConnection(String correlationId, ConnectionParams connection) {
    if (connection == null) {
      throw ConfigException(
          correlationId, 'NO_CONNECTION', 'MQTT connection is not set');
    }

    var uri = connection.getUri();
    if (uri != null) return null;

    var protocol = connection.getAsNullableString('protocol');
    if (protocol == null) {
      throw ConfigException(
          correlationId, 'NO_PROTOCOL', 'Connection protocol is not set');
    }

    var host = connection.getHost();
    if (host == null) {
      throw ConfigException(
          correlationId, 'NO_HOST', 'Connection host is not set');
    }

    var port = connection.getPort();
    if (port == 0) {
      throw ConfigException(
          correlationId, 'NO_PORT', 'Connection port is not set');
    }
  }

  ConfigParams _composeOptions(
      ConnectionParams connection, CredentialParams credential) {
    // Define additional parameters parameters
    var options = connection.override(credential).getAsObject();

    // Compose uri
    if (options.uri == null) {
      options.uri = options.protocol + '://' + options.host;
      if (options.port) {
        options.uri += ':' + options.port;
      }
    }

    return options;
  }

  ///Resolves MQTT connection options from connection and credential parameters.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// Return 			        Future that receives resolved options
  /// Throws error.
  Future<ConfigParams> resolve(String correlationId) async {
    ConnectionParams connection;
    CredentialParams credential;
    var err;

    await Future.wait([
      () async {
        try {
          connection = await connectionResolver.resolve(correlationId);
          // Validate connections
          _validateConnection(correlationId, connection);
        } catch (ex) {
          err = ex;
        }
      }(),
      () async {
        try {
          credential = await credentialResolver.lookup(correlationId);
          // Credentials are not validated right now
        } catch (ex) {
          err = ex;
        }
      }()
    ]);

    if (err != null) {
      throw err;
    }

    return _composeOptions(connection, credential);
  }

  ///Composes MQTT connection options from connection and credential parameters.
  ///
  /// - [correlationId]     (optional) transaction id to trace execution through call chain.
  /// - [connection]        connection parameters
  /// - [credential]        credential parameters
  /// Returns              Future that receives resolved options.
  /// Throws error.
  Future<ConfigParams> compose(String correlationId,
      ConnectionParams connection, CredentialParams credential) async {
    // Validate connections
    _validateConnection(correlationId, connection);
    return _composeOptions(connection, credential);
  }
}
