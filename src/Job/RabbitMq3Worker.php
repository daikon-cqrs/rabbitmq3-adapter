<?php

namespace Daikon\RabbitMq3\Job;

use Assert\Assertion;
use Daikon\AsyncJob\Event\JobFailed;
use Daikon\AsyncJob\Job\JobMap;
use Daikon\AsyncJob\Worker\WorkerInterface;
use Daikon\MessageBus\Envelope;
use Daikon\MessageBus\EnvelopeInterface;
use Daikon\MessageBus\MessageBusInterface;
use Daikon\MessageBus\Metadata\Metadata;
use Daikon\RabbitMq3\Connector\RabbitMq3Connector;
use PhpAmqpLib\Message\AMQPMessage;

final class RabbitMq3Worker implements WorkerInterface
{
    private $connector;

    private $messageBus;

    private $jobMap;

    private $settings;

    public function __construct(
        RabbitMq3Connector $connector,
        MessageBusInterface $messageBus,
        JobMap $jobMap,
        array $settings = []
    ) {
        $this->connector = $connector;
        $this->messageBus = $messageBus;
        $this->jobMap = $jobMap;
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
        $job = $this->jobMap->get($metadata->get('job'));

        try {
            $this->messageBus->receive($envelope);
        } catch (\Exception $error) {
            if ($job->getStrategy()->canRetry()) {
                $this->retry($envelope);
            } else {
                $this->fail(
                    $envelope,
                    $metadata->with('_errorMessage', $error->getMessage())
                );
            }
        }

        $channel->basic_ack($deliveryTag);
    }

    private function retry(EnvelopeInterface $envelope): void
    {
        //work out retry mechanism
        //republish to message bus with expiration?
        //defer in transport?
    }

    private function fail(EnvelopeInterface $envelope, Metadata $metadata): void
    {
        $jobFailed = JobFailed::fromArray(['failed_message' => $envelope->getMessage()]);
        $this->messageBus->publish($jobFailed, 'logging', $metadata);
    }
}