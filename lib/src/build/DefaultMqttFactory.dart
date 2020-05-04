//  @module build 
// import { Factory } from 'pip-services3-components-node';
// import { Descriptor } from 'pip-services3-commons-node';

// import { MqttMessageQueue } from '../queues/MqttMessageQueue';

// 
// ///Creates [[MqttMessageQueue]] components by their descriptors.
// ///
// ///See [[MqttMessageQueue]]
//  
// export class DefaultMqttFactory extends Factory {
// 	public static readonly Descriptor = new Descriptor("pip-services", "factory", "mqtt", "default", "1.0");
//     public static readonly MqttQueueDescriptor: Descriptor = new Descriptor("pip-services", "message-queue", "mqtt", "*", "1.0");

// 	
// 	///Create a new instance of the factory.
// 	 
// 	public constructor() {
//         super();
//         this.register(DefaultMqttFactory.MqttQueueDescriptor, (locator: Descriptor) => {
//             return new MqttMessageQueue(locator.getName());
//         });
// 	}
// }