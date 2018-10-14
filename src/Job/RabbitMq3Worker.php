<?php

namespace Daikon\RabbitMq3\Job;

use Assert\Assertion;
use Daikon\AsyncJob\Event\JobFailed;
use Daikon\AsyncJob\Job\JobDefinitionMap;
use Daikon\AsyncJob\Worker\WorkerInterface;
use Daikon\MessageBus\Envelope;
use Daikon\MessageBus\EnvelopeInterface;
use Daikon\MessageBus\MessageBusInterface;
use Daikon\MessageBus\MessageInterface;
use Daikon\MessageBus\Metadata\Metadata;
use Daikon\RabbitMq3\Connector\RabbitMq3Connector;
use PhpAmqpLib\Message\AMQPMessage;

final class RabbitMq3Worker implements WorkerInterface
{
    private $connector;

    private $messageBus;

    private $jobDefinitionMap;

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

        $messageHandler = function (AMQPMessage $message) {
            $this->execute($message);
        };

        $channel = $this->connector->getConnection()->channel();
        $channel->basic_qos(null, 1, null);
        $channel->basic_consume($queue, false, true, false, false, false, $messageHandler);

        while (count($channel->callbacks)) {
            $channel->wait();
        }
    }

    private function execute(AMQPMessage $message): void
    {
        $deliveryInfo = $message->delivery_info;
        $channel = $deliveryInfo['channel'];
        $deliveryTag = $deliveryInfo['delivery_tag'];

        $envelope = Envelope::fromArray(json_decode($message->body, true));
        $metadata = $envelope->getMetadata();
        $job = $this->jobDefinitionMap->get($metadata->get('job'));

        try {
            throw new \Exception('test');
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

    private function retry(MessageInterface $message, Metadata $metadata): void
    {
        $this->messageBus->publish($message, $metadata->get('_channel'), $metadata);
    }

    private function fail(MessageInterface $message, Metadata $metadata): void
    {
        $jobFailed = JobFailed::fromArray(['failed_message' => $message]);
        $this->messageBus->publish($jobFailed, 'logging', $metadata);
    }
}
