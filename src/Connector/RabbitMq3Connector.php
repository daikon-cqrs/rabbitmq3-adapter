<?php declare(strict_types=1);
/**
 * This file is part of the daikon-cqrs/rabbitmq3-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Daikon\RabbitMq3\Connector;

use Daikon\Dbal\Connector\ConnectorInterface;
use Daikon\Dbal\Connector\ProvidesConnector;
use PhpAmqpLib\Connection\AMQPLazyConnection;

class RabbitMq3Connector implements ConnectorInterface
{
    use ProvidesConnector;

    protected function connect(): AMQPLazyConnection
    {
        $settings = $this->getSettings();

        // https://github.com/videlalvaro/php-amqplib/blob/master/PhpAmqpLib/Connection/AMQPStreamConnection.php#L8-L39
        return new AMQPLazyConnection(
            $settings['host'] ?? 'localhost',
            $settings['port'] ?? '5672',
            $settings['user'] ?? '',
            $settings['password'] ?? '',
            $settings['vhost'] ?? '/',
            $settings['insist'] ?? false,
            $settings['login_method'] ?? 'AMQPLAIN',
            null,
            $settings['locale'] ?? 'en_US',
            $settings['connection_timeout'] ?? 3.0,
            $settings['read_write_timeout'] ?? 3.0,
            null,
            $settings['keepalive'] ?? true,
            // setting this to NULL may lead to using the server proposed value?
            $settings['heartbeat'] ?? 0
        );
    }

    public function disconnect(): void
    {
        if ($this->isConnected()) {
            $this->connection->close();
            $this->connection = null;
        }
    }
}
