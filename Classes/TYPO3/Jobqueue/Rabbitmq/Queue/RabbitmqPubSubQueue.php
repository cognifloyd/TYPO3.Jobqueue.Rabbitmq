<?php
namespace TYPO3\Jobqueue\Rabbitmq\Queue;

/*                                                                        *
 * This script belongs to the TYPO3 Flow package "TYPO3.Jobqueue.Rabbitmq"*
 *                                                                        *
 * It is free software; you can redistribute it and/or modify it under    *
 * the terms of the GNU General Public License, either version 3 of the   *
 * License, or (at your option) any later version.                        *
 *                                                                        *
 * The TYPO3 project - inspiring people to share!                         *
 *                                                                        */

use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Jobqueue\Common\Queue\Message;
use TYPO3\Jobqueue\Common\Queue\QueueInterface;

/**
 * This Queue focuses on implementing the Publish/Subscribe messaging model
 * with RabbitMQ
 */
class RabbitmqPubSubQueue extends AbstractRabbitmqQueue {

	/**
	 * Constructor:
	 * Create a connect to RabbitMQ and ensure that the named exchange exists.
	 * If the exchange already exists, it must be persistent (durable & no auto_delete)
	 *
	 * Unlike a WorkQueue, the PubSubQueue doesn't work with just one queue.
	 * It works with a fanout exchange to send messages to more than one queue, including an auto-named queue that is
	 * this "Queue" subscribes to.
	 *
	 * @param string $name    The name of the exchange (not the queue!) where messages are published to
	 * @param array  $options Connection options array
	 */
	public function __construct($name, array $options = array()) {
		parent::__construct($name, $options);
		$this->exchange = $name;
		$durableExchange = isset($options['durableExchange']) ? $options['durableExchange'] : FALSE;

		//Create a fanout exchange that doesn't auto_delete messages
		$this->channel->exchange_declare($this->exchange, 'fanout', FALSE, $durableExchange, FALSE);
		//Set queue name & Create a temporary exclusive queue that doesn't auto_delete messages
		list($this->name,,) = $this->channel->queue_declare("", FALSE, FALSE, TRUE, FALSE);
		//Bind the temporary queue to the exchange
		$this->channel->queue_bind($this->name, $this->exchange);
	}

	/**
	 * Publish a message to the exchange
	 * The state of the message will be updated according to the result of the operation.
	 *
	 * @param Message $message
	 * @return void
	 */
	public function publish(Message $message) {
		$payload = $message->getPayload();
		$amqpMessage = new AMQPMessage($payload);

		//avoid race condition to make count work reliably
		$this->channel->confirm_select();
		$this->channel->basic_publish($amqpMessage, $this->exchange);
		$message->setState(Message::STATE_PUBLISHED);
	}

	/**
	 * Begin subscribing to the queue until unsubscribed or until at least one of the conditions is met.
	 *
	 * This fires an event every time a message is received. That event will have the received message in it.
	 *
	 * This also returns all received messages in an array.
	 *
	 * @param integer $messageLimit
	 * @param integer $timeout
	 * @return array<Message> $messages Array of Messages received.
	 */
	public function subscribe($messageLimit = NULL, $timeout = NULL) {
		$messages = array();

		$callback = function(AMQPMessage $amqpMessage) use (&$messages) {
			$message = new Message($amqpMessage->body);
			$message->setIdentifier($amqpMessage->delivery_info['delivery_tag']);
			$messages[] = $message;
			$this->emitMessageReceivedFromQueue($message, $this->exchange);
		};

		// include a qos?
		$this->channel->basic_consume($this->name, '', FALSE, TRUE, FALSE, FALSE, $callback);

		while(count($this->channel->callbacks)) {
			try {
				$this->channel->wait(null, false, is_null($timeout) ? 0 : $timeout);
			} catch (AMQPTimeoutException $exception) {
				return $messages;
			}
			if(!is_null($messageLimit) && count($messages) >= $messageLimit) return $messages;
		}

		return $messages;
	}

	public function unsubscribe() {

	}

	/**
	 * from https://github.com/mgdm/Mosquitto-PHP
	 * onLog       - set the logging callback
	 * onSubscribe - set the subscribe callback
	 * onMessage   - set teh callback fired when a message is received
	 * qos         - num of msgs that can be inflight at once
	 * publish     - publish a message to a broker
	 * subscribe   - subscribe to a topic
	 * unsubscribe - unsubscribe from a topic
	 * loop        - The main network loop
	 */

	/**
	 *
	 * @param Message $message
	 * @param String $exchangeName The exchange that the message was published to
	 * @return void
	 * @Flow\Signal
	 */
	protected function emitMessageReceivedFromQueue(Message $message, $exchangeName) {}
}