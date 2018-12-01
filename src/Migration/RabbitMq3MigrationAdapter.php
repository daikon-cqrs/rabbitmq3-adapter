<?php
/**
 * This file is part of the daikon-cqrs/rabbitmq3-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Daikon\RabbitMq3\Migration;

use Daikon\Dbal\Connector\ConnectorInterface;
use Daikon\Dbal\Migration\MigrationAdapterInterface;
use Daikon\Dbal\Migration\MigrationList;
use Daikon\RabbitMq3\Connector\RabbitMq3AdminConnector;

final class RabbitMq3MigrationAdapter implements MigrationAdapterInterface
{
    /** @var RabbitMq3AdminConnector */
    private $connector;

    /** @var array */
    private $settings;

    public function __construct(RabbitMq3AdminConnector $connector, array $settings = [])
    {
        $this->connector = $connector;
        $this->settings = $settings;
    }

    public function read(string $identifier): MigrationList
    {
        $currentMigrations= $this->loadMigrations();
        $migrations = array_filter(
            $currentMigrations,
            function (array $migration) use ($identifier): bool {
                return $migration['routing_key'] === $identifier;
            }
        );

        return $this->createMigrationList($migrations);
    }

    /*
     * We do not have a way of storing the migration list as a data structure in RabbitMQ so instead
     * we make use of internal exchange bindings with metadata as a way of tracking the migration state
     * of the messaging infrastructure.
     */
    public function write(string $identifier, MigrationList $executedMigrations): void
    {
        $exchange = $this->settings['exchange'];
        $client = $this->connector->getConnection();
        $uri = sprintf('/api/bindings/%1$s/e/%2$s/e/%2$s', $this->getVhost(), $exchange);

        // delete existing migration list entries before rewriting
        foreach ($this->loadMigrations() as $migration) {
            $client->delete($uri.'/'.$migration['properties_key']);
        }

        foreach ($executedMigrations as $migration) {
            $client->post($uri, [
                'body' => json_encode([
                    'routing_key' => $identifier,
                    'arguments' => $migration->toNative()
                ])
            ]);
        }
    }

    public function getConnector(): ConnectorInterface
    {
        return $this->connector;
    }

    private function loadMigrations(): array
    {
        $uri = sprintf('/api/exchanges/%s/%s/bindings/source', $this->getVhost(), $this->settings['exchange']);
        $response = $this->connector->getConnection()->get($uri);
        return json_decode($response->getBody()->getContents(), true);
    }

    private function createMigrationList(array $migrationData): MigrationList
    {
        $migrations = [];
        foreach ($migrationData as $migration) {
            $migrationClass = $migration['arguments']['@type'];
            $migrations[] = new $migrationClass(new \DateTimeImmutable($migration['arguments']['executedAt']));
        }

        return (new MigrationList($migrations))->sortByVersion();
    }

    private function getVhost(): string
    {
        $connectorSettings = $this->connector->getSettings();
        return $connectorSettings['vhost'];
    }
}
