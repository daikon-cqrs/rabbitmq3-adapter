<?php declare(strict_types=1);
/**
 * This file is part of the daikon-cqrs/rabbitmq3-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Daikon\RabbitMq3\Transport;

use Daikon\Interop\Assertion;
use Daikon\MessageBus\Channel\Subscription\Transport\TransportInterface;
use Daikon\MessageBus\EnvelopeInterface;
use Daikon\MessageBus\MessageBusInterface;
use Daikon\RabbitMq3\Connector\RabbitMq3Connector;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

final class RabbitMq3Transport implements TransportInterface
{
    private string $key;

    private RabbitMq3Connector $connector;

    public function __construct(string $key, RabbitMq3Connector $connector)
    {
        $this->key = $key;
        $this->connector = $connector;
    }

    public function send(EnvelopeInterface $envelope, MessageBusInterface $messageBus): void
    {
        $metadata = $envelope->getMetadata();
        $exchange = $metadata->get('exchange');
        $routingKey = $metadata->get('routing_key', '');

        Assertion::notBlank($exchange, 'Exchange name must not be blank.');
        Assertion::string($routingKey, 'Routing key must be a string.');

        $payload = json_encode($envelope->toNative());
        $properties = ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT];
        if ($metadata->has('headers')) {
            $properties['application_headers'] = new AMQPTable($metadata->get('headers'));
        }
        if ($metadata->has('_expiration')) {
            $properties['expiration'] = $metadata->get('_expiration');
        }
        $message = new AMQPMessage($payload, $properties);

        $channel = $this->connector->getConnection()->channel();
        $channel->basic_publish($message, $exchange, $routingKey);
    }

    public function getKey(): string
    {
        return $this->key;
    }
}
