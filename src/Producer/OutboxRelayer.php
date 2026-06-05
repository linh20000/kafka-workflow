<?php

namespace Wf\Kafka\Producer;

use Illuminate\Support\Facades\Log;
use RdKafka\Producer;
use RdKafka\ProducerTopic;
use Wf\Kafka\Kafka\KafkaConfig;
use Wf\Kafka\Models\OutboxEvent;

/**
 * Outbox Relayer — đọc PENDING/FAILED records từ kafka_outbox
 * và publish lên Kafka theo batch, một flush duy nhất mỗi batch.
 *
 * Đảm bảo At-Least-Once: chỉ mark SENT sau khi flush thành công.
 * Idempotency phía consumer được đảm bảo bởi ConsumerDispatcher (processed_events).
 */
class OutboxRelayer
{
    private Producer $rdProducer;
    /** @var ProducerTopic[] */
    private array $topicCache = [];

    private int $maxAttempts;
    private int $batchSize;

    public function __construct(private readonly KafkaConfig $config)
    {
        $this->rdProducer  = new Producer($config->makeProducerConf());
        $outbox            = $config->getOutboxConfig();
        $this->maxAttempts = (int) ($outbox['max_attempts'] ?? 3);
        $this->batchSize   = (int) ($outbox['batch_size']   ?? 100);
    }

    // ── Entry point ────────────────────────────────────────────────────────

    /**
     * Quét outbox và relay tất cả bản ghi PENDING + FAILED retryable.
     *
     * @return array{sent: int, failed: int, skipped: int}
     */
    public function relay(): array
    {
        $stats = ['sent' => 0, 'failed' => 0, 'skipped' => 0];

        $events = OutboxEvent::pending()
            ->union(OutboxEvent::retryable($this->maxAttempts))
            ->limit($this->batchSize)
            ->get();

        if ($events->isEmpty()) {
            return $stats;
        }

        Log::info('[wf-kafka][Relayer] Starting relay batch', ['count' => $events->count()]);

        // Group events theo topic để batch publish hiệu quả
        $grouped = $events->groupBy('topic');

        foreach ($grouped as $topic => $topicEvents) {
            $result = $this->relayTopicBatch($topic, $topicEvents->all());
            $stats['sent']    += $result['sent'];
            $stats['failed']  += $result['failed'];
            $stats['skipped'] += $result['skipped'];
        }

        Log::info('[wf-kafka][Relayer] Batch complete', $stats);
        return $stats;
    }

    // ── Per-topic batch ────────────────────────────────────────────────────

    /**
     * Publish tất cả events của một topic trong một lần flush duy nhất.
     *
     * @param OutboxEvent[] $events
     * @return array{sent: int, failed: int, skipped: int}
     */
    private function relayTopicBatch(string $topic, array $events): array
    {
        $stats      = ['sent' => 0, 'failed' => 0, 'skipped' => 0];
        $toPublish  = [];

        foreach ($events as $event) {
            if ($event->attempts >= $this->maxAttempts) {
                $this->markGivenUp($event);
                $stats['skipped']++;
                Log::warning('[wf-kafka][Relayer] Max attempts reached', [
                    'event_id' => $event->event_id,
                    'attempts' => $event->attempts,
                ]);
                continue;
            }
            $toPublish[] = $event;
        }

        if (empty($toPublish)) {
            return $stats;
        }

        // Enqueue tất cả vào producer internal buffer
        foreach ($toPublish as $event) {
            $this->enqueue($topic, $event);
        }

        // Flush một lần cho toàn bộ batch
        try {
            $this->flush();
            foreach ($toPublish as $event) {
                $this->markSent($event);
                $stats['sent']++;
            }
        } catch (\RuntimeException $e) {
            // Flush thất bại — đánh dấu failed cho tất cả trong batch
            foreach ($toPublish as $event) {
                $this->markFailed($event, $e->getMessage());
                $stats['failed']++;
            }
            Log::error('[wf-kafka][Relayer] Batch flush failed', [
                'topic' => $topic,
                'count' => count($toPublish),
                'error' => $e->getMessage(),
            ]);
        }

        return $stats;
    }

    // ── RdKafka ops ────────────────────────────────────────────────────────

    private function enqueue(string $topicName, OutboxEvent $event): void
    {
        $kafkaTopic = $this->getTopic($topicName);

        // Gửi raw bytes thẳng lên Kafka — không decode, không re-encode.
        // payload đã được serialize đúng format (JSON/Avro) từ OutboxWriter.
        // Điều này đảm bảo bit-perfect: bytes trong DB == bytes trên Kafka == bytes Consumer nhận.
        $kafkaTopic->produce(
            RD_KAFKA_PARTITION_UA,
            0,
            $event->getRawPayload(), // raw string, không qua Eloquent cast
            $event->event_id         // Partition key → ordering guarantee
        );

        $this->rdProducer->poll(0); // Drain delivery reports, không block
    }

    private function flush(int $timeoutMs = 5000): void
    {
        $result = $this->rdProducer->flush($timeoutMs);

        if ($result !== RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw new \RuntimeException(
                "[wf-kafka][Relayer] Flush failed after {$timeoutMs}ms (err={$result})"
            );
        }
    }

    private function getTopic(string $name): \RdKafka\ProducerTopic
    {
        return $this->topicCache[$name] ??= $this->rdProducer->newTopic($name);
    }

    // ── Status updates ─────────────────────────────────────────────────────

    private function markSent(OutboxEvent $event): void
    {
        $event->update(['status' => 'SENT', 'sent_at' => now()]);
    }

    private function markFailed(OutboxEvent $event, string $error): void
    {
        $event->update([
            'status'     => 'FAILED',
            'attempts'   => $event->attempts + 1,
            'last_error' => $error,
        ]);
    }

    private function markGivenUp(OutboxEvent $event): void
    {
        $event->update([
            'attempts'   => $this->maxAttempts,
            'last_error' => "Exceeded max {$this->maxAttempts} attempts. Manual intervention required.",
        ]);
    }
}
