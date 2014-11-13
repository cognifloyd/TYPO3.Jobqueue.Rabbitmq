<?php
namespace TYPO3\Jobqueue\Rabbitmq\Queue;

/*                                                                        *
 * This script belongs to the TYPO3 Flow package " "                      *
 *                                                                        *
 * It is free software; you can redistribute it and/or modify it under    *
 * the terms of the GNU General Public License, either version 3 of the   *
 * License, or (at your option) any later version.                        *
 *                                                                        *
 * The TYPO3 project - inspiring people to share!                         *
 *                                                                        */

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use TYPO3\Flow\Annotations as Flow;

/**
 * Common implementation details for Rabbitmq based Queues.
 */
abstract class AbstractRabbitmqQueue {

	/**
	 * The name of the queue
	 *
	 * @var string
	 */
	protected $name;

	/**
	 * The name of the exchange to publish to
	 *
	 * @var string
	 */
	protected $exchange;

	/**
	 * This AMQP connection is the main RabbitMQ entry point.
	 * Uses LazyConnection to postpone connection until it's actually used.
	 *
	 * @var AMQPLazyConnection
	 */
	protected $connection;

	/**
	 * The current AMQP data channel
	 *
	 * @var AMQPChannel
	 */
	protected $channel;

	/**
	 * Constructor:
	 * Create a connection to RabbitMQ
	 *
	 * The class that extends this class should declare the exchange/queue as necessary.
	 *
	 * @param string $name    The name of the work queue to put work in or get work from
	 * @param array  $options Connection options array
	 */
	public function __construct($name, array $options = array()) {
		// Awkward, but standard way of getting options with TYPO3.JobQueue.*
		$clientOptions = isset($options['client']) ? $options['client'] : array();
		$host = isset($clientOptions['host']) ? $clientOptions['host'] : 'localhost';
		$port = isset($clientOptions['port']) ? $clientOptions['port'] : '5672';
		$username = isset($clientOptions['username']) ? $clientOptions['username'] : 'guest';
		$password = isset($clientOptions['password']) ? $clientOptions['password'] : 'guest';
		$vhost = isset($clientOptions['vhost']) ? $clientOptions['vhost'] : '/';

		$this->connection = new AMQPLazyConnection($host, $port, $username, $password, $vhost);
		$this->channel = $this->connection->channel();
	}

	/**
	 * Cleanly close the connection.
	 */
	public function __destruct() {
		$this->channel->close();
		$this->connection->close();
	}
}