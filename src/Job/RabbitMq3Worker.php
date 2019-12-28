<?php declare(strict_types=1);
/**
 * This file is part of the daikon-cqrs/rabbitmq3-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Daikon\RabbitMq3\Job;

use Assert\Assertion;
use Daikon\AsyncJob\Job\JobDefinitionMap;
use Daikon\AsyncJob\Worker\WorkerInterface;
use Daikon\MessageBus\Envelope;
use Daikon\MessageBus\MessageBusInterface;
use Daikon\RabbitMq3\Connector\RabbitMq3Connector;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;
use RuntimeException;

final class RabbitMq3Worker implements WorkerInterface
{
    /** @var RabbitMq3Connector */
    private $connector;

    /** @var MessageBusInterface */
    private $messageBus;

    /** @var JobDefinitionMap */
    private $jobDefinitionMap;

    /** @var LoggerInterface */
    private $logger;

    /** @var array */
    private $settings;

    public function __construct(
        RabbitMq3Connector $connector,
        MessageBusInterface $messageBus,
        JobDefinitionMap $jobDefinitionMap,
        LoggerInterface $logger,
        array $settings = []
    ) {
        $this->connector = $connector;
        $this->messageBus = $messageBus;
        $this->jobDefinitionMap = $jobDefinitionMap;
        $this->logger = $logger;
        $this->settings = $settings;
    }

    public function run(array $parameters = []): void
    {
        $queue = $parameters['queue'];
        Assertion::notBlank($queue);

        $messageHandler = function (AMQPMessage $amqpMessage): void {
            $this->execute($amqpMessage);
        };

        /** @var AMQPChannel $channel */
        $channel = $this->connector->getConnection()->channel();
        $channel->basic_qos(0, 1, false);
        $channel->basic_consume($queue, '', true, false, false, false, $messageHandler);

        while (count($channel->callbacks)) {
            $channel->wait();
        }
    }

    private function execute(AMQPMessage $amqpMessage): void
    {
        $deliveryInfo = $amqpMessage->delivery_info;
        $channel = $deliveryInfo['channel'];
        $deliveryTag = $deliveryInfo['delivery_tag'];

        $envelope = Envelope::fromNative(json_decode($amqpMessage->body, true));
        $metadata = $envelope->getMetadata();
        $jobName = (string)$metadata->get('job');

        Assertion::notBlank($jobName, 'Worker job name must not be blank.');
        $job = $this->jobDefinitionMap->get($jobName);

        try {
            $this->messageBus->receive($envelope);
        } catch (RuntimeException $error) {
            $message = $envelope->getMessage();
            if ($job->getStrategy()->canRetry($envelope)) {
                $retries = $metadata->get('_retries', 0);
                $metadata = $metadata
                    ->with('_retries', ++$retries)
                    ->with('_expiration', $job->getStrategy()->getRetryInterval($envelope));
                $this->messageBus->publish($message, (string)$metadata->get('_channel'), $metadata);
            } else {
                //@todo add message/metadata to error context
                $this->logger->error("Failed handling job '$jobName'", ['exception' => $error]);
            }
        }

        $channel->basic_ack($deliveryTag);
    }
}
