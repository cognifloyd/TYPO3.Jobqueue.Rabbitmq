<?php
namespace TYPO3\Jobqueue\Rabbitmq\Tests\Functional\Queue;

/*                                                                        *
 * This script belongs to the TYPO3 Flow package " "                      *
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
use TYPO3\Jobqueue\Rabbitmq\Queue\RabbitmqWorkQueue;

/**
 * Tests for RabbitmqWorkQueue
 */
class RabbitmqWorkQueueTest extends FunctionalTestCase {

	/**
	 * @var RabbitmqWorkQueue
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
		$this->queue = new RabbitmqWorkQueue($queueName, $settings['testing']);
		$connectionOptions = $settings['testing']['client'];

		//flush queue
		$connection = new AMQPConnection($connectionOptions['host'], $connectionOptions['port'], $connectionOptions['username'], $connectionOptions['password'], $connectionOptions['vhost']);
		$channel = $connection->channel();
		$channel->queue_declare($queueName, false, true, false, false);
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

	/**
	 * @test
	 */
	public function countReturnsZeroByDefault() {
		$this->assertSame(0, $this->queue->count());
	}

	/**
	 * @test
	 */
	public function countReturnsNumberOfReadyJobs() {
		$message1 = new Message('First message');
		$this->queue->publish($message1);

		$message2 = new Message('Second message');
		$this->queue->publish($message2);

		$this->assertSame(2, $this->queue->count());
	}

	/**
	 * @test
	 */
	public function waitForMessageTimesOut() {
		$result = $this->queue->waitAndTake(1);
		$this->assertNull($result, 'wait should return NULL after timeout');
	}

	/**
	 * @test
	 */
	public function publishAndWaitWithMessageWorks() {
		$message = new Message('Yeah, tell someone it works!');
		$this->queue->publish($message);

		$result = $this->queue->waitAndTake(1);
		$this->assertNotNull($result, 'wait should receive message');
		$this->assertEquals($message->getPayload(), $result->getPayload(), 'message should have payload as before');
	}

	/**
	 * @test
	 */
	public function peekReturnsAnEmptyArrayIfQueueHasNoMessage() {
		$result = $this->queue->peek();
		$this->assertEquals(array(), $result, 'peek should not return a message');
	}

	/**
	 * @test
	 */
	public function peekReturnsNextMessagesIfQueueHasMessages() {
		$message = new Message('First message');
		$this->queue->publish($message);
		$message = new Message('Another message');
		$this->queue->publish($message);

		$results = $this->queue->peek(1);
		$this->assertEquals(1, count($results), 'peek should return a message');
		/** @var Message $result */
		$result = $results[0];
		$this->assertEquals('First message', $result->getPayload());
		$this->assertEquals(Message::STATE_PUBLISHED, $result->getState(), 'Message state should be published');

		$results = $this->queue->peek(1);
		$this->assertEquals(1, count($results), 'peek should return a message again');
		$result = $results[0];
		$this->assertEquals('First message', $result->getPayload(), 'second peek should return the same message again');

		$results = $this->queue->peek(2);
		$this->assertEquals(2, count($results), 'peek should return two messages with two message limit');
		$result = $results[0];
		$this->assertEquals('First message', $result->getPayload(), 'peeking two messages should show the first message first');
		$result = $results[1];
		$this->assertEquals('Another message', $result->getPayload(), 'peeking two messages should show the second message second');
	}

	/**
	 * @test
	 */
	public function waitAndReserveWithFinishRemovesMessage() {
		$initialMessage = new Message('First message');
		$this->queue->publish($initialMessage);

		$resultMessage = $this->queue->waitAndReserve(1);
		$this->assertNotNull($resultMessage, 'waitAndReserve should receive message');
		$this->assertEquals($initialMessage->getPayload(), $resultMessage->getPayload(), 'message should have payload as before');

		$emptyResult = $this->queue->peek();
		$this->assertEquals(array(), $emptyResult, 'no message should be present in queue');

		$finishResult = $this->queue->finish($resultMessage);
		$this->assertTrue($finishResult, 'message should be finished');
	}

//	/**
//	 * @test
//	 */
//	public function identifierMakesMessagesUnique() {
//		$this->markTestSkipped('RabbitMQ Does not support unique message publishing.');
//		$message = new Message('Yeah, tell someone it works!', 'test.message');
//		$identicalMessage = new Message('Yeah, tell someone it works!', 'test.message');
//		$this->queue->publish($message);
//		$this->queue->publish($identicalMessage);
//
//		$this->assertEquals(Message::STATE_NEW, $identicalMessage->getState());
//
//		$result = $this->queue->waitAndTake(1);
//		$this->assertNotNull($result, 'wait should receive message');
//
//		$result = $this->queue->waitAndTake(1);
//		$this->assertNull($result, 'message should not be queued twice');
//	}

}
