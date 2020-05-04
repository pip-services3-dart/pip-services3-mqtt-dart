//  @module queues 
//  @hidden 
// let async = require('async');

// import { ConnectionParams } from 'pip-services3-components-node';
// import { CredentialParams } from 'pip-services3-components-node';

// import { IMessageReceiver } from 'pip-services3-messaging-node';
// import { MessageQueue } from 'pip-services3-messaging-node';
// import { MessageEnvelope } from 'pip-services3-messaging-node';
// import { MessagingCapabilities } from 'pip-services3-messaging-node';

// import { MqttConnectionResolver } from '../connect/MqttConnectionResolver';

// 
// ///Message queue that sends and receives messages via MQTT message broker.
// /// 
// ///MQTT is a popular light-weight protocol to communicate IoT devices.
// ///
// ///### Configuration parameters ###
// ///
// ///- topic:                         name of MQTT topic to subscribe
// ///- connection(s):
// ///  - discovery_key:               (optional) a key to retrieve the connection from [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]]
// ///  - host:                        host name or IP address
// ///  - port:                        port number
// ///  - uri:                         resource URI or connection string with all parameters in it
// ///- credential(s):
// ///  - store_key:                   (optional) a key to retrieve the credentials from [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/auth.icredentialstore.html ICredentialStore]]
// ///  - username:                    user name
// ///  - password:                    user password
// ///
// ///### References ###
// ///
// ///- <code>\*:logger:\*:\*:1.0</code>             (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/log.ilogger.html ILogger]] components to pass log messages
// ///- <code>\*:counters:\*:\*:1.0</code>           (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/count.icounters.html ICounters]] components to pass collected measurements
// ///- <code>\*:discovery:\*:\*:1.0</code>          (optional) [[https://rawgit.com/pip-services-node/pip-services3-components-node/master/doc/api/interfaces/connect.idiscovery.html IDiscovery]] services to resolve connections
// ///- <code>\*:credential-store:\*:\*:1.0</code>   (optional) Credential stores to resolve credentials
// ///
// ///See [[MessageQueue]]
// ///See [[MessagingCapabilities]]
// ///
// ///### Example ###
// ///
// ///    let queue = new MqttMessageQueue("myqueue");
// ///    queue.configure(ConfigParams.fromTuples(
// ///      "topic", "mytopic",
// ///      "connection.protocol", "mqtt"
// ///      "connection.host", "localhost"
// ///      "connection.port", 1883
// ///    ));
// ///
// ///    queue.open("123", (err) => {
// ///        ...
// ///    });
// ///
// ///    queue.send("123", new MessageEnvelope(null, "mymessage", "ABC"));
// ///
// ///    queue.receive("123", (err, message) => {
// ///        if (message != null) {
// ///           ...
// ///           queue.complete("123", message);
// ///        }
// ///    });
//  
// export class MqttMessageQueue extends MessageQueue {
//     private _client: any;
//     private _topic: string;
//     private _subscribed: boolean = false;
//     private _optionsResolver: MqttConnectionResolver = new MqttConnectionResolver();
//     private _receiver: IMessageReceiver;
//     private _messages: MessageEnvelope[];

//     
//     ///Creates a new instance of the message queue.
//     ///
//     /// - name  (optional) a queue name.
//      
//     public constructor(name?: string) {
//         super(name);
//         this._capabilities = new MessagingCapabilities(false, true, true, true, true, false, false, false, true);
//     }

//     
// 	///Checks if the component is opened.
// 	///
// 	///@returns true if the component has been opened and false otherwise.
//      
//     public isOpen(): boolean {
//         return this._client != null;
//     }

//     
//     ///Opens the component with given connection and credential parameters.
//     ///
//     /// - correlationId     (optional) transaction id to trace execution through call chain.
//     /// - connection        connection parameters
//     /// - credential        credential parameters
//     /// - callback 			callback function that receives error or null no errors occured.
//      
//     protected openWithParams(correlationId: string, connection: ConnectionParams, credential: CredentialParams, callback: (err: any) => void): void {
//         this._topic = connection.getAsString('topic');

//         this._optionsResolver.compose(correlationId, connection, credential, (err, options) => {
//             if (err) {
//                 callback(err);
//                 return;
//             }

//             let mqtt = require('mqtt');
//             let client = mqtt.connect(options.uri, options);

//             client.on('connect', () => {
//                 this._client = client;
//                 callback(null);
//             });
            
//             client.on('error', (err) => {
//                 callback(err);
//             });
//         });
//     }

//     
// 	///Closes component and frees used resources.
// 	///
// 	/// - correlationId 	(optional) transaction id to trace execution through call chain.
//     /// - callback 			callback function that receives error or null no errors occured.
//      
//     public close(correlationId: string, callback: (err: any) => void): void {
//         if (this._client != null) {
//             this._messages = [];
//             this._subscribed = false;
//             this._receiver = null;

//             this._client.end();
//             this._client = null;
//             this._logger.trace(correlationId, "Closed queue %s", this);
//         }

//         callback(null);
//     }

//     
// 	///Clears component state.
// 	///
// 	/// - correlationId 	(optional) transaction id to trace execution through call chain.
//     /// - callback 			callback function that receives error or null no errors occured.
//      
//     public clear(correlationId: string, callback: (err?: any) => void): void {
//         this._messages = [];
//         callback();
//     }

//     
//     ///Reads the current number of messages in the queue to be delivered.
//     ///
//     /// - callback      callback function that receives number of messages or error.
//      
//     public readMessageCount(callback: (err: any, count: number) => void): void {
//         // Subscribe to get messages
//         this.subscribe();

//         let count = this._messages.length;
//         callback(null, count);
//     }

//     
//     ///Sends a message into the queue.
//     ///
//     /// - correlationId     (optional) transaction id to trace execution through call chain.
//     /// - envelope          a message envelop to be sent.
//     /// - callback          (optional) callback function that receives error or null for success.
//      
//     public send(correlationId: string, envelop: MessageEnvelope, callback?: (err: any) => void): void {
//         this._counters.incrementOne("queue." + this.getName() + ".sent_messages");
//         this._logger.debug(envelop.correlation_id, "Sent message %s via %s", envelop.toString(), this.toString());

//         this._client.publish(this._topic, envelop.message, callback);
//     }

//     
//     ///Peeks a single incoming message from the queue without removing it.
//     ///If there are no messages available in the queue it returns null.
//     ///
//     /// - correlationId     (optional) transaction id to trace execution through call chain.
//     /// - callback          callback function that receives a message or error.
//      
//     public peek(correlationId: string, callback: (err: any, result: MessageEnvelope) => void): void {
//         // Subscribe to get messages
//         this.subscribe();

//         if (this._messages.length > 0)
//             callback(null, this._messages[0]);
//         else callback(null, null);
//     }

//     
//     ///Peeks multiple incoming messages from the queue without removing them.
//     ///If there are no messages available in the queue it returns an empty list.
//     ///
//     ///Important: This method is not supported by MQTT.
//     ///
//     /// - correlationId     (optional) transaction id to trace execution through call chain.
//     /// - messageCount      a maximum number of messages to peek.
//     /// - callback          callback function that receives a list with messages or error.
//      
//     public peekBatch(correlationId: string, messageCount: number, callback: (err: any, result: MessageEnvelope[]) => void): void {
//         // Subscribe to get messages
//         this.subscribe();

//         callback(null, this._messages);
//     }

//     
//     ///Receives an incoming message and removes it from the queue.
//     ///
//     /// - correlationId     (optional) transaction id to trace execution through call chain.
//     /// - waitTimeout       a timeout in milliseconds to wait for a message to come.
//     /// - callback          callback function that receives a message or error.
//      
//     public receive(correlationId: string, waitTimeout: number, callback: (err: any, result: MessageEnvelope) => void): void {
//         let err: any = null;
//         let message: MessageEnvelope = null;
//         let messageReceived: boolean = false;

//         // Subscribe to get messages
//         this.subscribe();

//         // Return message immediately if it exist
//         if (this._messages.length > 0) {
//             message = this._messages.shift();
//             callback(null, message);
//             return;
//         }

//         // Otherwise wait and return
//         let checkIntervalMs = 100;
//         let i = 0;
//         async.whilst(
//             () => {
//                 return this._client && i < waitTimeout && message == null;
//             },
//             (whilstCallback) => {
//                 i = i + checkIntervalMs;

//                 setTimeout(() => {
//                     message = this._messages.shift();
//                     whilstCallback();
//                 }, checkIntervalMs);
//             },
//             (err) => {
//                 callback(err, message);
//             }
//         );
//     }

//     
//     ///Renews a lock on a message that makes it invisible from other receivers in the queue.
//     ///This method is usually used to extend the message processing time.
//     ///
//     ///Important: This method is not supported by MQTT.
//     ///
//     /// - message       a message to extend its lock.
//     /// - lockTimeout   a locking timeout in milliseconds.
//     /// - callback      (optional) callback function that receives an error or null for success.
//      
//     public renewLock(message: MessageEnvelope, lockTimeout: number, callback?: (err: any) => void): void {
//         // Not supported
//         if (callback) callback(null);
//     }

//     
//     ///Permanently removes a message from the queue.
//     ///This method is usually used to remove the message after successful processing.
//     ///
//     ///Important: This method is not supported by MQTT.
//     ///
//     /// - message   a message to remove.
//     /// - callback  (optional) callback function that receives an error or null for success.
//      
//     public complete(message: MessageEnvelope, callback: (err: any) => void): void {
//         // Not supported
//         if (callback) callback(null);
//     }

//     
//     ///Returnes message into the queue and makes it available for all subscribers to receive it again.
//     ///This method is usually used to return a message which could not be processed at the moment
//     ///to repeat the attempt. Messages that cause unrecoverable errors shall be removed permanently
//     ///or/and send to dead letter queue.
//     ///
//     ///Important: This method is not supported by MQTT.
//     ///
//     /// - message   a message to return.
//     /// - callback  (optional) callback function that receives an error or null for success.
//      
//     public abandon(message: MessageEnvelope, callback: (err: any) => void): void {
//         // Not supported
//         if (callback) callback(null);
//     }

//     
//     ///Permanently removes a message from the queue and sends it to dead letter queue.
//     ///
//     ///Important: This method is not supported by MQTT.
//     ///
//     /// - message   a message to be removed.
//     /// - callback  (optional) callback function that receives an error or null for success.
//      
//     public moveToDeadLetter(message: MessageEnvelope, callback: (err: any) => void): void {
//         // Not supported
//         if (callback) callback(null);
//     }

//     private toMessage(topic: string, message: any, packet: any): MessageEnvelope {
//         let envelop = new MessageEnvelope(null, topic, message);
//         envelop.message_id = packet.messageId;
//         return envelop;
//     }

//     
//     ///Subscribes to the topic.
//      
//     protected subscribe(): void {
//         // Exit if already subscribed or 
//         if (this._subscribed && this._client == null)
//             return;

//         this._logger.trace(null, "Started listening messages at %s", this.toString());

//         this._client.on('message', (topic, message, packet) => {
//             let envelop = this.toMessage(topic, message, packet);

//             this._counters.incrementOne("queue." + this.getName() + ".received_messages");
//             this._logger.debug(message.correlation_id, "Received message %s via %s", message, this.toString());

//             if (this._receiver != null) {
//                 try {
//                     this._receiver.receiveMessage(envelop, this, (err) => {
//                         if (err) this._logger.error(null, err, "Failed to receive the message");
//                     });
//                 } catch (ex) {
//                     this._logger.error(null, ex, "Failed to receive the message");
//                 }
//             } else {
//                 // Keep message queue managable
//                 while (this._messages.length > 1000)
//                     this._messages.shift();
                    
//                 // Push into the message queue
//                 this._messages.push(envelop);
//             }
//         });

//         // Subscribe to the topic
//         this._client.subscribe(this._topic, (err) => {
//             if (err) this._logger.error(null, err, "Failed to subscribe to topic " + this._topic);
//         });
//         this._subscribed = true;
//     }

//     
//     ///Listens for incoming messages and blocks the current thread until queue is closed.
//     ///
//     /// - correlationId     (optional) transaction id to trace execution through call chain.
//     /// - receiver          a receiver to receive incoming messages.
//     ///
//     ///See [[IMessageReceiver]]
//     ///See [[receive]]
//      
//     public listen(correlationId: string, receiver: IMessageReceiver): void {
//         this._receiver = receiver;

//         // Pass all cached messages
//         async.whilst(
//             () => {
//                 return this._messages.length > 0 && this._receiver != null;
//             },
//             (whilstCallback) => {
//                 if (this._messages.length > 0 && this._receiver != null) {
//                     let message = this._messages.shift();
//                     receiver.receiveMessage(message, this, whilstCallback);
//                 } else whilstCallback();
//             },
//             (err) => {
//                 // Subscribe to get messages
//                 this.subscribe();
//             }
//         );
//     }

//     
//     ///Ends listening for incoming messages.
//     ///When this method is call [[listen]] unblocks the thread and execution continues.
//     ///
//     /// - correlationId     (optional) transaction id to trace execution through call chain.
//      
//     public endListen(correlationId: string): void {
//         this._receiver = null;

//         if (this._subscribed) {
//             this._client.unsubscribe(this._topic);
//             this._subscribed = false;
//         }
//     }

// }