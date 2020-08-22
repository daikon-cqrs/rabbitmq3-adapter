<?php declare(strict_types=1);
/**
 * This file is part of the daikon-cqrs/rabbitmq3-adapter project.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Daikon\RabbitMq3\Job;

use Daikon\AsyncJob\Job\JobDefinitionInterface;
use Daikon\AsyncJob\Job\JobDefinitionMap;
use Daikon\AsyncJob\Metadata\JobMetadataEnricher;
use Daikon\AsyncJob\Worker\WorkerInterface;
use Daikon\Interop\Assertion;
use Daikon\Interop\RuntimeException;
use Daikon\MessageBus\Channel\ChannelInterface;
use Daikon\MessageBus\Envelope;
use Daikon\MessageBus\MessageBusInterface;
use Daikon\RabbitMq3\Connector\RabbitMq3Connector;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;

final class RabbitMq3Worker implements WorkerInterface
{
    private RabbitMq3Connector $connector;

    private MessageBusInterface $messageBus;

    private JobDefinitionMap $jobDefinitionMap;

    private LoggerInterface $logger;

    private array $settings;

    public function __construct(
        RabbitMq3Connector $connector,
        MessageBusInterface $messageBus,
        JobDefinitionMap $jobDefinitionMap,
        LoggerInterface $logger,
        array $settings = []
    ) {
        $this->connector = $connector;
        $this->messageBus = $messageBus;
        $this->jobDefinitionMap = $jobDefinitionMap;
        $this->logger = $logger;
        $this->settings = $settings;
    }

    public function run(array $parameters = []): void
    {
        $queue = $parameters['queue'];
        Assertion::notBlank($queue, 'Queue name must not be blank.');

        $messageHandler = function (AMQPMessage $amqpMessage): void {
            $this->execute($amqpMessage);
        };

        /** @var AMQPChannel $channel */
        $channel = $this->connector->getConnection()->channel();
        $channel->basic_qos(0, 1, false);
        $channel->basic_consume($queue, '', true, false, false, false, $messageHandler);

        while (count($channel->callbacks)) {
            $channel->wait();
        }
    }

    private function execute(AMQPMessage $amqpMessage): void
    {
        $deliveryInfo = $amqpMessage->delivery_info;
        /** @var AMQPChannel $channel */
        $channel = $deliveryInfo['channel'];
        $deliveryTag = $deliveryInfo['delivery_tag'];

        $envelope = Envelope::fromNative(json_decode($amqpMessage->body, true));
        $metadata = $envelope->getMetadata();

        $jobKey = (string)$metadata->get(JobMetadataEnricher::JOB);
        Assertion::notBlank($jobKey, 'Job key must not be blank.');
        if (!$this->jobDefinitionMap->has($jobKey)) {
            throw new RuntimeException("Job definition '$jobKey' not found.");
        }

        try {
            $this->messageBus->receive($envelope);
            $channel->basic_ack($deliveryTag);
        } catch (RuntimeException $error) {
            /** @var JobDefinitionInterface $job */
            $job = $this->jobDefinitionMap->get($jobKey);
            if ($job->getStrategy()->canRetry($envelope)) {
                $this->messageBus->publish(
                    $envelope->getMessage(),
                    (string)$metadata->get(ChannelInterface::METADATA_KEY),
                    $metadata
                );
            } else {
                //@todo add message/metadata to error context
                $this->logger->error($error->getMessage(), ['exception' => $error->getTrace()]);
            }
            $channel->basic_nack($deliveryTag, false, false);
        }
    }
}
