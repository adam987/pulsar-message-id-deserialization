const Pulsar = require('pulsar-client');


const client = new Pulsar.Client({ serviceUrl: 'pulsar://localhost:6650' });
const topic = 'my-topic';
const messageCount = 10;

const subscriptionWithoutDeserialization = 'without-deserialization';
const subscriptionWithDeserialization = 'with-deserialization';


(async () => {
	try {
		// Create subscriptions
		const consumerWithoutSerialization = await client.subscribe({ topic, subscription: subscriptionWithoutDeserialization });
		await consumerWithoutSerialization.close();

		const consumerWithSerialization = await client.subscribe({ topic, subscription: subscriptionWithDeserialization });
		await consumerWithSerialization.close();

		// Send messages to the topic
		await sendMessages();

		// Receive and ack messages, then resubscribe and count again

		console.log(
			`First '${subscriptionWithoutDeserialization}' subscription: %d messages`,
			await countMessages(subscriptionWithoutDeserialization, { ack: true, deserializeId: false })
		);
		console.log(
			`Second '${subscriptionWithoutDeserialization}' subscription: %d messages`,
			await countMessages(subscriptionWithoutDeserialization),
		);
		
		console.log();

		console.log(
			`First '${subscriptionWithDeserialization}' subscription: %d messages`,
			await countMessages(subscriptionWithDeserialization, { ack: true, deserializeId: true })
		);
		console.log(
			`Second '${subscriptionWithDeserialization}' subscription: %d messages`,
			await countMessages(subscriptionWithDeserialization),
		);

		await client.close();
	} catch (error) {
		console.error(`Error: ${error}`);
	}
})();



async function sendMessages() {
	const producer = await client.createProducer({ topic });
	try {
		for (let i = 0; i < messageCount; i += 1) await producer.send({ data: [] });
	} finally {
		await producer.close();
	}
}

async function countMessages(subscription, { ack, deserializeId } = { ack: false, deserializeId: false }) {
	const consumer = await client.subscribe({ topic, subscription });
	
	try {
		const messages = await consumer.batchReceive();
		
		if (ack) {
			for (let message of messages) {
				const id = message.getMessageId();
				await consumer.acknowledgeId(deserializeId
					? Pulsar.MessageId.deserialize(id.serialize())
					: id
				);
			}
		}

		return messages.length;
	} finally {
		await consumer.close();
	}
}

