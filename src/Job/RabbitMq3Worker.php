<?php
/**
 * This file is part of the daikon-cqrs/rabbitmq3-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Daikon\RabbitMq3\Job;

use Assert\Assertion;
use Daikon\AsyncJob\Event\JobFailed;
use Daikon\AsyncJob\Job\JobDefinitionMap;
use Daikon\AsyncJob\Worker\WorkerInterface;
use Daikon\MessageBus\Envelope;
use Daikon\MessageBus\EnvelopeInterface;
use Daikon\MessageBus\MessageBusInterface;
use Daikon\MessageBus\MessageInterface;
use Daikon\MessageBus\Metadata\MetadataInterface;
use Daikon\RabbitMq3\Connector\RabbitMq3Connector;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;

final class RabbitMq3Worker implements WorkerInterface
{
    /** @var RabbitMq3Connector */
    private $connector;

    /** @var MessageBusInterface */
    private $messageBus;

    /** @var JobDefinitionMap */
    private $jobDefinitionMap;

    /** @var array */
    private $settings;

    public function __construct(
        RabbitMq3Connector $connector,
        MessageBusInterface $messageBus,
        JobDefinitionMap $jobDefinitionMap,
        array $settings = []
    ) {
        $this->connector = $connector;
        $this->messageBus = $messageBus;
        $this->jobDefinitionMap = $jobDefinitionMap;
        $this->settings = $settings;
    }

    public function run(array $parameters = []): void
    {
        $queue = $parameters['queue'];
        Assertion::notBlank($queue);

        $messageHandler = function (AMQPMessage $message): void {
            $this->execute($message);
        };

        /** @var AMQPChannel $channel */
        $channel = $this->connector->getConnection()->channel();
        $channel->basic_qos(0, 1, false);
        $channel->basic_consume($queue, '', true, false, false, false, $messageHandler);

        while (count($channel->callbacks)) {
            $channel->wait();
        }
    }

    private function execute(AMQPMessage $message): void
    {
        $deliveryInfo = $message->delivery_info;
        $channel = $deliveryInfo['channel'];
        $deliveryTag = $deliveryInfo['delivery_tag'];

        $envelope = Envelope::fromNative(json_decode($message->body, true));
        $metadata = $envelope->getMetadata();
        $job = $this->jobDefinitionMap->get($metadata->get('job'));

        try {
            $this->messageBus->receive($envelope);
        } catch (\Exception $error) {
            $message = $envelope->getMessage();
            if ($job->getStrategy()->canRetry($envelope)) {
                $retries = $metadata->get('_retries', 0);
                $metadata = $metadata
                    ->with('_retries', ++$retries)
                    ->with('_expiration', $job->getStrategy()->getRetryInterval($envelope));
                $this->retry($message, $metadata);
            } else {
                $metadata = $metadata->with('_error_message', $error->getMessage());
                $this->fail($message, $metadata);
            }
        }

        $channel->basic_ack($deliveryTag);
    }

    private function retry(MessageInterface $message, MetadataInterface $metadata): void
    {
        $this->messageBus->publish($message, $metadata->get('_channel'), $metadata);
    }

    private function fail(MessageInterface $message, MetadataInterface $metadata): void
    {
        $jobFailed = JobFailed::fromNative(['failed_message' => $message]);
        $this->messageBus->publish($jobFailed, 'events', $metadata);
    }
}
