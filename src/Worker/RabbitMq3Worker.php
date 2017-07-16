<?php

namespace Daikon\RabbitMq3\Worker;

use Assert\Assertion;
use Daikon\AsyncJob\Job\JobMap;
use Daikon\AsyncJob\Worker\WorkerInterface;
use Daikon\MessageBus\Envelope;
use Daikon\MessageBus\MessageBusInterface;
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

    public function run(): void
    {
        $queue = $this->settings['queue'];
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

        //@todo possibly move execution to the job
        try {
            $this->messageBus->receive($envelope);
        } catch (\Exception $error) {
            //@todo manage job retry and failure
            throw $error;
        }

        $channel->basic_ack($deliveryTag);
    }
}