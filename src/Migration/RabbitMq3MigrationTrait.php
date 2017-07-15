<?php

namespace Daikon\RabbitMq3\Migration;

use Daikon\Dbal\Migration\MigrationTrait;
use GuzzleHttp\Exception\RequestException;

trait RabbitMq3MigrationTrait
{
    const WAITING_SUFFIX = '.waiting';
    const UNROUTED_SUFFIX = '.unrouted';
    const REPUB_SUFFIX = '.repub';

    use MigrationTrait;

    private function createMigrationList(string $exchange): void
    {
        $this->declareExchange($exchange, 'topic', false, true, false, true);
    }

    private function createMessagePipeline(string $exchange, int $repubInterval = 30000): void
    {
        $waitExchange = $exchange.self::WAITING_SUFFIX;
        $waitQueue = $waitExchange;
        $unroutedExchange = $exchange.self::UNROUTED_SUFFIX;
        $unroutedQueue = $unroutedExchange;
        $repubExchange = $exchange.self::REPUB_SUFFIX;
        $repubQueue = $repubExchange;

        // Setup the default exchange and queue pipelines
        $this->declareExchange($unroutedExchange, 'fanout', false, true, false, true); //internal
        $this->declareExchange($repubExchange, 'fanout', false, true, false, true); //internal
        $this->declareExchange($waitExchange, 'fanout', false, true, false);
        $this->declareExchange($exchange, 'topic', false, true, false, false, false, [
            'alternate-exchange' => $unroutedExchange
        ]);
        $this->declareQueue($waitQueue, false, true, false, false, false, [
            'x-dead-letter-exchange' => $exchange
        ]);
        $this->bindQueue($waitQueue, $waitExchange);
        $this->declareQueue($unroutedQueue, false, true, false, false, false, [
            'x-dead-letter-exchange' => $repubExchange,
            'x-message-ttl' => $repubInterval
        ]);
        $this->bindQueue($unroutedQueue, $unroutedExchange);
        $this->declareQueue($repubQueue, false, true, false, false);
        $this->bindQueue($repubQueue, $repubExchange);

        $this->createShovel($repubExchange, $exchange, $repubQueue);
    }

    private function deleteMessagePipeline(string $exchange): void
    {
        $waitExchange = $exchange.self::WAITING_SUFFIX;
        $waitQueue = $waitExchange;
        $unroutedExchange = $exchange.self::UNROUTED_SUFFIX;
        $unroutedQueue = $unroutedExchange;
        $repubExchange = $exchange.self::REPUB_SUFFIX;
        $repubQueue = $repubExchange;

        $this->deleteShovel($repubExchange);
        $this->deleteExchange($waitExchange);
        $this->deleteExchange($unroutedExchange);
        $this->deleteExchange($repubExchange);
        $this->deleteExchange($exchange);
        $this->deleteQueue($waitQueue);
        $this->deleteQueue($unroutedQueue);
        $this->deleteQueue($repubQueue);
    }

    private function declareExchange(
        string $exchange,
        string $type,
        bool $passive = false,
        bool $durable = false,
        bool $autoDelete = true,
        bool $internal = false,
        bool $noWait = false,
        array $arguments = []
    ): void {
        $uri = sprintf('/api/exchanges/%s/%s', $this->getVhost(), $exchange);
        $this->connector->getConnection()->put($uri, [
            'body' => json_encode([
                'type' => $type,
                'passive' => $passive,
                'durable' => $durable,
                'auto_delete' => $autoDelete,
                'internal' => $internal,
                'nowait' => $noWait,
                'arguments' => $arguments
            ])
        ]);
    }

    private function bindExchange(
        string $source,
        string $dest,
        string $routingKey = '',
        bool $noWait = false,
        array $arguments = []
    ): void {
        $uri = sprintf('/api/bindings/%s/e/%s/e/%s', $this->getVhost(), $source, $dest);
        $this->connector->getConnection()->post($uri, [
            'body' => json_encode([
                'routing_key' => $routingKey,
                'nowait' => $noWait,
                'arguments' => $arguments
            ])
        ]);
    }

    private function deleteExchange(string $exchange): void
    {
        $uri = sprintf('/api/exchanges/%s/%s', $this->getVhost(), $exchange);
        $this->connector->getConnection()->delete($uri);
    }

    private function declareQueue(
        string $queue,
        bool $passive = false,
        bool $durable = false,
        bool $exclusive = false,
        bool $autoDelete = true,
        bool $noWait = false,
        array $arguments = []
    ): void {
        $uri = sprintf('/api/queues/%s/%s', $this->getVhost(), $queue);
        $this->connector->getConnection()->put($uri, [
            'body' => json_encode([
                'passive' => $passive,
                'durable' => $durable,
                'exclusive' => $exclusive,
                'auto_delete' => $autoDelete,
                'nowait' => $noWait,
                'arguments' => $arguments
            ])
        ]);
    }

    private function bindQueue(
        string $queue,
        string $exchange,
        string $routingKey = '',
        bool $noWait = false,
        array $arguments = []
    ): void {
        $uri = sprintf('/api/bindings/%s/e/%s/q/%s', $this->getVhost(), $exchange, $queue);
        $this->connector->getConnection()->post($uri, [
            'body' => json_encode([
                'routing_key' => $routingKey,
                'nowait' => $noWait,
                'arguments' => $arguments
            ])
        ]);
    }

    private function deleteQueue(string $queue): void
    {
        $uri = sprintf('/api/queues/%s/%s', $this->getVhost(), $queue);
        $this->connector->getConnection()->delete($uri);
    }

    private function createShovel(string $source, string $dest, string $queue): void
    {
        $uri = sprintf('/api/parameters/shovel/%s/%s.shovel', $this->getVhost(), $source);
        $this->connector->getConnection()->put($uri, [
            'body' => json_encode([
                'value' => [
                    'src-uri' => 'amqp://',
                    'src-queue' => $queue,
                    'dest-uri' => 'amqp://',
                    'dest-exchange' => $dest,
                    'add-forward-headers' => false,
                    'ack-mode' => 'on-confirm',
                    'delete-after' => 'never'
                ]
            ])
        ]);
    }

    private function deleteShovel(string $exchange): void
    {
        $uri = sprintf('/api/parameters/shovel/%s/%s.shovel', $this->getVhost(), $exchange);
        $this->connector->getConnection()->delete($uri);
    }

    private function getVhost(): string
    {
        $connectorSettings = $this->connector->getSettings();
        return $connectorSettings['vhost'];
    }
}
