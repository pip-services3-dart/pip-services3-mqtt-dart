# Configuration Guide <br/>

Configuration structure follows the 
[standard configuration](https://github.com/pip-services/pip-services3-container-node/doc/Configuration.md) 
structure. 

### <a name="mqtt_messaging"></a> MQTT messaging

MQTT messaging service has the following configuration properties:
- topic:                         name of MQTT topic to subscribe
- connection(s):
  - discovery_key:               (optional) a key to retrieve the connection from  IDiscovery
  - host:                        host name or IP address
  - port:                        port number
  - uri:                         resource URI or connection string with all parameters in it
- credential(s):
  - store_key:                   (optional) a key to retrieve the credentials from  ICredentialStore
  - username:                    user name
  - password:                    user password

Example:
```yaml
- descriptor: "pip-services:messaging:mqtt:default:1.0"
  connection:
   protocol: tcp
		host: localhost
		port: 1883,
    topic: testTopic,
  credential:
    username: "user"
		password: "pa$$wd"
```

For more information on this section read 
[Pip.Services Configuration Guide](https://github.com/pip-services/pip-services3-container-node/doc/Configuration.md#deps)