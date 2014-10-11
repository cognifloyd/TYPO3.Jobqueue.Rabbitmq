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


use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Jobqueue\Common\Exception as JobqueueException;
use TYPO3\Jobqueue\Common\Queue\Message;
use TYPO3\Jobqueue\Common\Queue\QueueInterface;

/**
 * A queue implemented using Rabbitmq
 *
 * There are other kinds of queues, this is a Work Queue as explained here:
 * http://www.rabbitmq.com/tutorials/tutorial-two-php.html
 */
class RabbitmqWorkQueue implements QueueInterface {

	/**
	 * The name of the work queue
	 * @var string
	 */
	protected $name;

	/**
	 * This AMQP connection is the main RabbitMQ entry point.
	 * @var AMQPConnection
	 * @todo look at switching to AMQPLazyConnection
	 */
	protected $connection;

	/**
	 * The current AMQP data channel
	 * @var AMQPChannel
	 */
	protected $channel;

	/**
	 * Constructor:
	 * Create a connect to RabbitMQ and ensure that the named queue exists.
	 * If the queue already exists, it must be persistent (durable & no auto_delete)
	 *
	 * @param string $name The name of the work queue to put work in or get work from
	 * @param array $options Connection options array
	 */
	public function __construct($name, array $options = array()) {
		$this->name = $name;

		// Awkward, but standard way of getting options with TYPO3.JobQueue.*
		$clientOptions = isset($options['client']) ? $options['client'] : array();
		$host = isset($clientOptions['host']) ? $clientOptions['host'] : 'localhost';
		$port = isset($clientOptions['port']) ? $clientOptions['port'] : '5672';
		$username = isset($clientOptions['username']) ? $clientOptions['username'] : 'guest';
		$password = isset($clientOptions['password']) ? $clientOptions['password'] : 'guest';
		$vhost = isset($clientOptions['vhost']) ? $clientOptions['vhost'] : '/';

		$this->connection = new AMQPConnection($host, $port, $username, $password, $vhost);
		$this->channel = $this->connection->channel();
		$this->channel->queue_declare($this->name, false, true, false, false);
	}

	/**
	 * Cleanly close the connection.
	 */
	public function __destruct() {
		$this->channel->close();
		$this->connection->close();
	}

	/**
	 * Publish a message to the queue
	 * The state of the message will be updated according
	 * to the result of the operation.
	 * If the queue supports unique messages, the message should not be queued if
	 * another message with the same identifier already exists.
	 *
	 * @param Message $message
	 * @return string The identifier of the message under which it was queued
	 * @todo rename to submit()
	 */
	public function publish(Message $message) {

	}

	/**
	 * Wait for a message in the queue and remove the message from the queue for processing
	 * If a non-null value was returned, the message was unqueued. Otherwise a timeout
	 * occured and no message was available or received.
	 *
	 * @param integer $timeout
	 * @return Message The received message or NULL if a timeout occurred
	 */
	public function waitAndTake($timeout = NULL) {

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

		$callback = NULL;

		#specify quality of service
		$this->channel->basic_qos(null,
							1, #prefetch_count = no more than one message at a time
							null);
		$this->channel->basic_consume($this->name, #queuename
									  '', #consumer_tag
									  FALSE, #no_local
									  FALSE, #no_ack
									  FALSE, #exclusive
									  FALSE, #nowait
									  $callback
									  #ticket
									  #arguments
		);

		#$this->channel->basic_get()	 #direct access to a queue; if no message available then return null
		#$this->channel->basic_consume() #start a queue consumer
		#$this->channel->basic_recover() #redeliver unacknowledged messages
		#$this->channel->basic_ack() 	 #acknowledge one or more messages
		#$this->channel->basic_nack()	 #reject one or several received messages
		#$this->channel->basic_reject()	 #reject an incoming message

		while(count($this->channel->callbacks)) {
			$this->channel->wait(null, false, is_null($timeout) ? 0 : $timeout);
		}
	}

	/**
	 * Mark a message as done
	 *
	 * This must be called for every message that was reserved and that was
	 * processed successfully.
	 *
	 * @param Message $message
	 * @return boolean TRUE if the message could be removed
	 */
	public function finish(Message $message) {

	}

	/**
	 * Peek for messages
	 *
	 * Inspect the next messages without taking them from the queue. It is not safe to take the messages
	 * and process them, since another consumer could have received this message already!
	 *
	 * @param integer $limit
	 * @return array<\TYPO3\Jobqueue\Common\Queue\Message> The messages up to the length of limit or an empty array if no messages are present currently
	 */
	public function peek($limit = 1) {

	}

	/**
	 * Get a message by identifier
	 *
	 * @param string $identifier
	 * @return Message The message or NULL if not present
	 */
	public function getMessage($identifier) {

	}

	/**
	 * Count messages in the queue
	 *
	 * Get a count of messages currently in the queue.
	 *
	 * @return integer The number of messages in the queue
	 */
	public function count() {

	}
}