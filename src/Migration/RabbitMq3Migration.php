<?php declare(strict_types=1);
/**
 * This file is part of the daikon-cqrs/rabbitmq3-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Daikon\RabbitMq3\Migration;

use Daikon\Dbal\Migration\Migration;

abstract class RabbitMq3Migration extends Migration
{
    protected function createMigrationList(string $exchange): void
    {
        $this->declareExchange($exchange, 'topic', false, true, false, true);
    }

    protected function createMessagePipeline(string $exchange, int $repubInterval = 30000): void
    {
        $waitExchange = $exchange.'.waiting';
        $waitQueue = $waitExchange;
        $unroutedExchange = $exchange.'.unrouted';
        $unroutedQueue = $unroutedExchange;
        $repubExchange = $exchange.'.repub';
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

    protected function deleteMessagePipeline(string $exchange): void
    {
        $waitExchange = $exchange.'.waiting';
        $waitQueue = $waitExchange;
        $unroutedExchange = $exchange.'.unrouted';
        $unroutedQueue = $unroutedExchange;
        $repubExchange = $exchange.'.repub';
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

    protected function declareExchange(
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

    protected function bindExchange(
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

    protected function deleteExchange(string $exchange): void
    {
        $uri = sprintf('/api/exchanges/%s/%s', $this->getVhost(), $exchange);
        $this->connector->getConnection()->delete($uri);
    }

    protected function declareQueue(
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

    protected function bindQueue(
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

    protected function deleteQueue(string $queue): void
    {
        $uri = sprintf('/api/queues/%s/%s', $this->getVhost(), $queue);
        $this->connector->getConnection()->delete($uri);
    }

    protected function createShovel(string $source, string $dest, string $queue): void
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

    protected function deleteShovel(string $exchange): void
    {
        $uri = sprintf('/api/parameters/shovel/%s/%s.shovel', $this->getVhost(), $exchange);
        $this->connector->getConnection()->delete($uri);
    }

    protected function getVhost(): string
    {
        $connectorSettings = $this->connector->getSettings();
        return $connectorSettings['vhost'];
    }
}
