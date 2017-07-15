<?php

namespace Daikon\RabbitMq3\Transport;

use Assert\Assertion;
use Daikon\MessageBus\Channel\Subscription\Transport\TransportInterface;
use Daikon\MessageBus\EnvelopeInterface;
use Daikon\MessageBus\MessageBusInterface;
use Daikon\RabbitMq3\Connector\RabbitMq3Connector;
use PhpAmqpLib\Message\AMQPMessage;

final class RabbitMq3Transport implements TransportInterface
{
    private $key;

    private $connector;

    public function __construct(string $key, RabbitMq3Connector $connector)
    {
        $this->key = $key;
        $this->connector = $connector;
    }

    public function send(EnvelopeInterface $envelope, MessageBusInterface $messageBus): bool
    {
        $metadata = $envelope->getMetadata();
        $exchange = $metadata->get('_exchange');
        $routingKey = $metadata->get('_routing_key', $metadata->get('_aggregate_alias', ''));

        Assertion::notBlank($exchange);
        Assertion::string($routingKey);

        $payload = json_encode($envelope->toArray(), true);
        $message = new AMQPMessage($payload, ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);

        $channel = $this->connector->getConnection()->channel();
        $channel->basic_publish($message, $exchange, $routingKey);

        return true;
    }

    public function getKey(): string
    {
        return $this->key;
    }
}