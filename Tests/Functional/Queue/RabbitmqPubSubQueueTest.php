<?php
namespace TYPO3\Jobqueue\Rabbitmq\Tests\Functional\Queue;

/*                                                                        *
 * This script belongs to the TYPO3 Flow package "TYPO3.Jobqueue.Rabbitmq"*
 *                                                                        *
 * It is free software; you can redistribute it and/or modify it under    *
 * the terms of the GNU General Public License, either version 3 of the   *
 * License, or (at your option) any later version.                        *
 *                                                                        *
 * The TYPO3 project - inspiring people to share!                         *
 *                                                                        */


use PhpAmqpLib\Connection\AMQPConnection;
use TYPO3\Flow\Configuration\ConfigurationManager;
use TYPO3\Flow\Tests\FunctionalTestCase;
use TYPO3\Jobqueue\Common\Queue\Message;
use TYPO3\Jobqueue\Rabbitmq\Queue\RabbitmqPubSubQueue;

/**
 * Tests for RabbitmqWorkQueue
 */
class RabbitmqPubSubQueueTest extends FunctionalTestCase {

	/**
	 * @var RabbitmqPubSubQueue
	 */
	protected $queue;

	/**
	 * Set up dependencies before each test
	 */
	public function setUp() {
		parent::setUp();
		$configurationManager = $this->objectManager->get('TYPO3\Flow\Configuration\ConfigurationManager');
		$settings = $configurationManager->getConfiguration(ConfigurationManager::CONFIGURATION_TYPE_SETTINGS, 'TYPO3.Jobqueue.Rabbitmq');
		if (!isset($settings['testing']['enabled']) || $settings['testing']['enabled'] !== TRUE) {
			$this->markTestSkipped('Rabbitmq is not configured (TYPO3.Jobqueue.Rabbitmq.testing.enabled != TRUE)');
		}

		$queueName = 'Test-queue';
		$this->queue = new RabbitmqPubSubQueue($queueName, $settings['testing']);
		$connectionOptions = $settings['testing']['client'];

		//flush queue
		$connection = new AMQPConnection($connectionOptions['host'], $connectionOptions['port'], $connectionOptions['username'], $connectionOptions['password'], $connectionOptions['vhost']);
		$channel = $connection->channel();
		$channel->queue_declare($queueName, FALSE, TRUE, FALSE, FALSE);
		$channel->queue_purge($queueName);
		$channel->close();
		$connection->close();

	}

	/**
	 * Tear down the queue after each test
	 */
	public function tearDown() {
		$this->queue->__destruct();
		unset($this->queue);
		parent::tearDown();
	}
}

