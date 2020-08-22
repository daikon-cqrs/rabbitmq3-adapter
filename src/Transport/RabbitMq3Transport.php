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
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

final class RabbitMq3Transport implements TransportInterface
{
    public const EXCHANGE = 'exchange';
    public const ROUTING_KEY = 'routing_key';
    public const APPLICATION_HEADERS = 'application_headers';
    public const EXPIRATION = 'expiration';

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
        $exchange = $metadata->get(self::EXCHANGE);
        $routingKey = $metadata->get(self::ROUTING_KEY, '');

        Assertion::notBlank($exchange, 'Exchange name must not be blank.');
        Assertion::string($routingKey, 'Routing key must be a string.');

        $payload = json_encode($envelope->toNative());
        $properties = ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT];
        if ($metadata->has(self::APPLICATION_HEADERS)) {
            $properties['application_headers'] = new AMQPTable($metadata->get(self::APPLICATION_HEADERS));
        }
        if ($metadata->has(self::EXPIRATION)) {
            $properties['expiration'] = $metadata->get(self::EXPIRATION);
        }
        $message = new AMQPMessage($payload, $properties);

        /** @var AMQPChannel $channel */
        $channel = $this->connector->getConnection()->channel();
        $channel->basic_publish($message, $exchange, $routingKey);
    }

    public function getKey(): string
    {
        return $this->key;
    }
}
