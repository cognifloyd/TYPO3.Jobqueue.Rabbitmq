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
 * A queue implemented using Rabbitmq
 *
 * There are other kinds of queues, this is a Work Queue as explained here:
 * http://www.rabbitmq.com/tutorials/tutorial-two-php.html
 */
class RabbitmqWorkQueue extends AbstractRabbitmqQueue implements QueueInterface {

	/**
	 * Constructor:
	 * Create a connect to RabbitMQ and ensure that the named queue exists.
	 * If the queue already exists, it must be persistent (durable & no auto_delete)
	 *
	 * @param string $name    The name of the work queue to put work in or get work from
	 * @param array  $options Connection options array
	 */
	public function __construct($name, array $options = array()) {
		parent::__construct($name,$options);
		$this->name = $name;
		$this->exchange = '';
		//Create a durable non-exclusive queue that doesn't auto_delete messages
		$this->channel->queue_declare($this->name, FALSE, TRUE, FALSE, FALSE);
	}

	/**
	 * Publish a message to the queue
	 * The state of the message will be updated according to the result of the operation.
	 *
	 * This implementation does not support setting $message->identifier before or when publishing.
	 * $message-identifier is only set when using waitAndReserve() or waitAndTake().
	 * Do not try to finish() a Message that was used to publish it. Publish and discard!
	 *
	 * @param Message $message
	 * @return string The identifier of the message under which it was queued
	 * @todo rename to submit()
	 */
	public function publish(Message $message) {
		$jsonData = $this->encodeMessage($message);
		$amqpMessage = new AMQPMessage(
			$jsonData,
			array(
				'delivery_mode' => 2, # make message persistent
				'content_type' => 'application/json'
			));

		/*
		 * confirm_select avoids a race condition.
		 * In tests, getting the count of messages immediately after publishing a message
		 * sometimes counts the messages *before* the published message actually gets published
		 * causing the count to be incorrect. confirm_select makes sure that publish succeeds before
		 * continuing.
		 *
		 * At some point, if someone wants this to be a hair faster, then there should be some option
		 * to disable confirm_select, but that might involve an API change for all jobqueue packages.
		 */
		$this->channel->confirm_select();

		$this->channel->basic_publish(
			$amqpMessage,    #msg
			$this->exchange, #exchange
			$this->name      #routing_key (queue)
			#mandatory=false (not implemented)
			#immediate=false (not implemented)
			#ticket (deprecated in AMQP 0-9-1)
		);

		//RabbitMQ can't provide an id at this point, because the id used to ack/nack it is specific to the consumer.
		//$message->setIdentifier($delivery_tag);
		$message->setState(Message::STATE_PUBLISHED);
	}

	/**
	 * Wait for a message in the queue and remove the message from the queue for processing
	 * If a non-null value was returned, the message was unqueued. Otherwise a timeout
	 * occurred and no message was available or received.
	 *
	 * @param integer $timeout
	 * @return Message The received message or NULL if a timeout occurred
	 */
	public function waitAndTake($timeout = NULL) {
		$message = $this->consume($timeout, TRUE);
		!is_null($message) && $message->setState(Message::STATE_DONE);
		return $message;
	}

	/**
	 * Wait for a message in the queue and reserve the message for processing
	 * NOTE: The processing of the message has to be confirmed by the consumer to
	 * remove the message from the queue by calling finish(). Depending on the implementation
	 * the message might be inserted to the queue after some time limit has passed.
	 * If a non-null value was returned, the message was reserved. Otherwise a timeout
	 * occurred and no message was available or received.
	 *
	 * @param integer $timeout
	 * @return Message The received message or NULL if a timeout occurred
	 */
	public function waitAndReserve($timeout = NULL) {
		$message = $this->consume($timeout);
		!is_null($message) && $message->setState(Message::STATE_RECEIVED);
		return $message;
	}

	/**
	 * Mark a message as done
	 *
	 * This must be called for every message that was reserved and that was processed successfully.
	 *
	 * Note: Other implementations have a way to set $message->identifier on publish, but rabbitmq does not.
	 * So, you cannot use the Message object you used to publish the message to acknowledge it.
	 * You can only finish a Message object that was generated by waitAndReserve().
	 *
	 * @param Message $message
	 * @return boolean TRUE if the message could be removed
	 */
	public function finish(Message $message) {
		$delivery_tag = $message->getIdentifier();
		$this->channel->basic_ack($delivery_tag);
		$message->setState(Message::STATE_DONE);
		return TRUE;
	}

	/**
	 * Peek for messages
	 *
	 * Inspect the next messages without taking them from the queue. It is not safe to take the messages
	 * and process them, since another consumer could have received this message already!
	 *
	 * Note: Rabbitmq does not support the idea of "peek"ing in a queue.
	 * This is destructive in between basic_get and basic_nack. Until the message is returned to the queue with basic_nack,
	 * other clients will get the next message after the message that you are peeking. Only use peek if you really need it.
	 *
	 * @param integer $limit
	 * @return array<\TYPO3\Jobqueue\Common\Queue\Message> The messages up to the length of limit or an empty array if no messages are present currently
	 */
	public function peek($limit = 1) {
		if($this->count() === 0) {
			return array();
		}
		/** @var array<AMQPMessage> $amqpMessages */
		$amqpMessages = array();
		for ($i = 1; $i <= $limit; $i++) {
			$amqpMessages[] = $this->channel->basic_get($this->name);
		}

		//This approximates the effect of a peek:
		/** @var AMQPMessage $lastAmqpMessage */
		$lastAmqpMessage = end($amqpMessages);
		$this->channel->basic_nack($lastAmqpMessage->delivery_info['delivery_tag'],TRUE,TRUE);

		$messages = array();
		foreach ($amqpMessages as $amqpMessage) {
			$message = $this->decodeMessage($amqpMessage->body);
			$message->setState(Message::STATE_PUBLISHED);
			$messages[] = $message;
		}

		return $messages;
	}

	/**
	 * Get a message by identifier (Not supported by RabbitMQ)
	 *
	 * @param string $identifier
	 * @return Message The message or NULL if not present
	 */
	public function getMessage($identifier) {
		//not implemented
		return NULL;
	}

	/**
	 * Encode a message
	 *
	 * Updates the original value property of the message to resemble the
	 * encoded representation.
	 *
	 * @param Message $message
	 * @return string
	 */
	protected function encodeMessage(Message $message) {
		$value = json_encode($message->toArray());
		$message->setOriginalValue($value);
		return $value;
	}

	/**
	 * Decode a message from a string representation
	 *
	 * @param string $value
	 * @return Message
	 */
	protected function decodeMessage($value) {
		$decodedMessage = json_decode($value, TRUE);
		$message = new Message($decodedMessage['payload']);
		if (isset($decodedMessage['identifier'])) {
			$message->setIdentifier($decodedMessage['identifier']);
		}
		$message->setOriginalValue($value);
		return $message;
	}

	/**
	 * consume a message
	 *
	 * @param integer $timeout
	 * @param boolean $no_ack If TRUE, immediately remove from queue without finishing it
	 * @return null|Message
	 */
	protected function consume($timeout = NULL, $no_ack = FALSE) {
		/** @var Message $message */
		$message = NULL;

		$callback = function(AMQPMessage $amqpMessage) use (&$message) {
			$message = $this->decodeMessage($amqpMessage->body);
			$message->setIdentifier($amqpMessage->delivery_info['delivery_tag']);
		};

		#specify quality of service
		$this->channel->basic_qos(
			null,
			1, #prefetch_count = no more than one message at a time
			null
		);
		$consumer_tag = $this->channel->basic_consume(
			$this->name, #queuename
			'', #consumer_tag
			FALSE, #no_local
			$no_ack, #no_ack
			FALSE, #exclusive
			FALSE, #nowait (Setting to TRUE breaks things!)
			$callback
			#ticket (deprecated in AMQP 0-9-1)
			#arguments
		);

		while(count($this->channel->callbacks)) {
			try {
				$this->channel->wait(null, false, is_null($timeout) ? 0 : $timeout);
			} catch (AMQPTimeoutException $exception) {
				return NULL;
			}
			//If we found a message, stop consuming and return it.
			if(!is_null($message)) {
				$this->channel->basic_cancel($consumer_tag);
				return $message;
			}
		}

		return $message;
	}
}