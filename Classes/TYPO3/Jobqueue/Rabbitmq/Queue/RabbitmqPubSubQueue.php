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

use TYPO3\Flow\Annotations as Flow;

/**
 * This Queue focuses on implementing the Publish/Subscribe messaging model
 * with RabbitMQ
 */
class RabbitmqPubSubQueue extends AbstractRabbitmqQueue {

	/**
	 * Constructor:
	 * Create a connect to RabbitMQ and ensure that the named queue exists.
	 * If the queue already exists, it must be persistent (durable & no auto_delete)
	 *
	 * @param string $name    The name of the work queue to put work in or get work from
	 * @param array  $options Connection options array
	 */
	public function __construct($name, array $options = array()) {
		parent::__construct($name, $options);
		#$this->channel->exchange_declare('logs', 'fanout', FALSE, FALSE, FALSE);
		#list($queue_name, ,) = $channel->queue_declare("", FALSE, FALSE, TRUE, FALSE);
		#$channel->queue_bind($queue_name, 'logs');
		$this->channel->queue_declare($this->name, FALSE, TRUE, FALSE, FALSE);

		//publishing only needs the exchange.
		//subscribing needs the exchange, the binding, and the queue.
	}
}