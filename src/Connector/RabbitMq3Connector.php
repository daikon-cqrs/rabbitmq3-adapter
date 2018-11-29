<?php

namespace Daikon\RabbitMq3\Connector;

use Daikon\Dbal\Connector\ConnectorInterface;
use Daikon\Dbal\Connector\ConnectorTrait;
use PhpAmqpLib\Connection\AMQPLazyConnection;

class RabbitMq3Connector implements ConnectorInterface
{
    use ConnectorTrait;

    private function connect(): AMQPLazyConnection
    {
        $settings = $this->getSettings();

        // https://github.com/videlalvaro/php-amqplib/blob/master/PhpAmqpLib/Connection/AMQPStreamConnection.php#L8-L39
        return new AMQPLazyConnection(
            $settings['host'] ?? 'localhost',
            $settings['port'] ?? 5672,
            $settings['user'] ?? null,
            $settings['password'] ?? null,
            $settings['vhost'] ?? '/',
            $settings['insist'] ?? false,
            $settings['login_method'] ?? 'AMQPLAIN',
            $settings['login_response'] ?? null,
            $settings['locale'] ?? 'en_US',
            $settings['connection_timeout'] ?? 3.0,
            $settings['read_write_timeout'] ?? 3.0,
            $settings['context'] ?? null,
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
