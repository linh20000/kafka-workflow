<?php

namespace Wf\Kafka\Kafka;

use RdKafka\Conf;

/**
 * Build RdKafka\Conf từ config array của Laravel.
 * Không đọc env() trực tiếp — nhận config từ ServiceProvider.
 */
class KafkaConfig
{
    public function __construct(private readonly array $config) {}

    public function getBrokers(): string
    {
        return $this->config['brokers'] ?? 'localhost:9092';
    }

    public function getProducerBatchSize(): int
    {
        return (int) ($this->config['producer']['batch_size'] ?? 100);
    }

    public function getConsumerConfig(): array
    {
        return $this->config['consumer'] ?? [];
    }

    public function getOutboxConfig(): array
    {
        return $this->config['outbox'] ?? [];
    }

    public function getDlqSuffix(): string
    {
        return $this->config['dlq']['suffix'] ?? '.dlq';
    }

    public function getTelegramConfig(): array
    {
        return $this->config['telegram'] ?? [];
    }

    public function getSerializationConfig(): array
    {
        return $this->config['serialization'] ?? ['driver' => 'json'];
    }

    public function getSerializationDriver(): string
    {
        return strtolower($this->config['serialization']['driver'] ?? 'json');
    }

    // ── Build Conf ─────────────────────────────────────────────────────────

    public function makeProducerConf(): Conf
    {
        $conf = new Conf();
        $this->applyCommon($conf);

        $p = $this->config['producer'] ?? [];
        $conf->set('acks',                  (string) ($p['acks']               ?? '-1')); // -1 mean wait for leader and all follower ACK
        $conf->set('message.timeout.ms',    (string) ($p['message_timeout_ms'] ?? 5000)); // time max to wait for message to be sent
        $conf->set('retries',               (string) ($p['retries']            ?? 3)); // 
        $conf->set('retry.backoff.ms',      (string) ($p['retry_backoff_ms']   ?? 500)); // time sleep before retry

        $conf->setDrMsgCb(static function (\RdKafka\Producer $producer, \RdKafka\Message $msg) {
            if ($msg->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
                error_log(sprintf('[wf-kafka][DR] FAILED topic=%s key=%s err=%s',
                    $msg->topic_name, $msg->key ?? '', $msg->errstr()));
            }
        });

        $conf->setErrorCb(static function (\RdKafka\Producer $p, int $err, string $reason) {
            error_log("[wf-kafka][PRODUCER ERR] ({$err}) {$reason}");
        });

        return $conf;
    }

    public function makeConsumerConf(string $groupId): Conf
    {
        $conf = new Conf();
        $this->applyCommon($conf);

        $c = $this->getConsumerConfig();
        $conf->set('group.id',            $groupId);
        $conf->set('auto.offset.reset',   $c['auto_offset_reset'] ?? 'latest');
        $conf->set('enable.auto.commit',  'false');

        $conf->setErrorCb(static function (\RdKafka\KafkaConsumer $c, int $err, string $reason) {
            error_log("[wf-kafka][CONSUMER ERR] ({$err}) {$reason}");
        });

        $conf->setRebalanceCb(static function (\RdKafka\KafkaConsumer $c, int $err, array $partitions) {
            if ($err === RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
                $c->assign($partitions);
            } elseif ($err === RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
                $c->assign(null);
            }
        });

        return $conf;
    }

    // ── Internal ───────────────────────────────────────────────────────────

    private function applyCommon(Conf $conf): void
    {
        $conf->set('bootstrap.servers', $this->getBrokers());
        $conf->set('socket.timeout.ms', '3000');

        $protocol = strtoupper($this->config['security_protocol'] ?? 'PLAINTEXT');
        $conf->set('security.protocol', $protocol);

        if (in_array($protocol, ['SASL_PLAINTEXT', 'SASL_SSL'], true)) {
            $sasl = $this->config['sasl'] ?? [];
            $conf->set('sasl.mechanism', $sasl['mechanism'] ?? 'SCRAM-SHA-512');
            $conf->set('sasl.username',  $sasl['username']  ?? '');
            $conf->set('sasl.password',  $sasl['password']  ?? '');
        }
    }
}
