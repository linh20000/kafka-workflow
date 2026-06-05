<?php

namespace Wf\Kafka\Consumer;

use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;use RdKafka\KafkaConsumer;
use RdKafka\Message;
use RdKafka\Producer;
use Wf\Kafka\Alerting\TelegramNotifier;
use Wf\Kafka\Exceptions\DeserializationException;
use Wf\Kafka\Exceptions\TransientInfraException;
use Wf\Kafka\Kafka\KafkaConfig;
use Wf\Kafka\Models\FailedEventLog;
use Wf\Kafka\Models\ProcessedEvent;
use Wf\Kafka\Serialization\MessageSerializer;

/**
 * Consumer Dispatcher — lõi của phần consumer trong package.
 *
 * Trách nhiệm của package:
 *   ✅ Poll message từ Kafka
 *   ✅ Idempotency guard (processed_events table)
 *   ✅ Dispatch message tới handler do host cung cấp
 *   ✅ Route lỗi: TransientInfraException → DLQ, còn lại → EDL
 *   ✅ Gửi Telegram alert
 *   ✅ Manual commit offset sau khi xử lý xong
 *
 * KHÔNG làm:
 *   ❌ Xử lý business logic
 *   ❌ Quyết định message có hợp lệ hay không
 *
 * Sử dụng trong host project:
 *
 *   $dispatcher = app(ConsumerDispatcher::class)
 *       ->onTopic('order-events')
 *       ->withGroup('order-service-group')
 *       ->handle(function (array $payload, array $meta) {
 *           // Business logic của bạn ở đây
 *           // Throw TransientInfraException để → DLQ
 *           // Throw PoisonPillException hoặc bất kỳ Exception nào khác → EDL
 *           OrderService::process($payload);
 *       });
 *
 *   $dispatcher->listen(); // blocking loop
 */
class ConsumerDispatcher
{
    private KafkaConsumer $rdConsumer;
    private array $topics = [];
    private string $groupId;
    private int $pollTimeoutMs;
    /** @var callable|null */
    private $handler = null;
    /** @var callable|null */
    private $onError = null;

    private Producer $dlqProducer;
    private string $dlqSuffix;
    private MessageSerializer $serializer;

    public function __construct(
        private readonly KafkaConfig      $config,
        private readonly TelegramNotifier $telegram,
        MessageSerializer                 $serializer
    ) {
        $consumer            = $config->getConsumerConfig();
        $this->groupId       = $consumer['group_id']       ?? 'laravel-consumer-group';
        $this->pollTimeoutMs = $consumer['poll_timeout_ms'] ?? 1000;
        $this->dlqSuffix     = $config->getDlqSuffix();
        $this->serializer    = $serializer;

        $this->rdConsumer  = new KafkaConsumer($config->makeConsumerConf($this->groupId));
        $this->dlqProducer = new Producer($config->makeProducerConf());
    }

    // ── Builder (fluent) ───────────────────────────────────────────────────

    public function withGroup(string $groupId): static
    {
        $clone             = clone $this;
        $clone->groupId    = $groupId;
        $clone->rdConsumer = new KafkaConsumer($this->config->makeConsumerConf($groupId));
        return $clone;
    }

    /**
     * Subscribe một hoặc nhiều topics.
     */
    public function onTopic(string ...$topics): static
    {
        $this->topics = $topics;
        return $this;
    }

    /**
     * Đăng ký handler xử lý business logic.
     *
     * Signature: function(array $payload, array $meta): void
     *   $meta = [
     *     'event_id'   => string,
     *     'event_type' => string,
     *     'topic'      => string,
     *     'partition'  => int,
     *     'offset'     => int,
     *   ]
     *
     * Host project throw exception để báo kết quả:
     *   TransientInfraException → route to DLQ (sẽ retry)
     *   PoisonPillException     → route to EDL (không retry)
     *   Bất kỳ exception nào khác → route to EDL
     */
    public function handle(callable $handler): static
    {
        $this->handler = $handler;
        return $this;
    }

    /**
     * Optional: callback khi message bị route vào DLQ hoặc EDL.
     * Signature: function(string $type, array $message, \Throwable $e): void
     *   $type = 'dlq' | 'edl'
     */
    public function onError(callable $callback): static
    {
        $this->onError = $callback;
        return $this;
    }

    // ── Listen loop ────────────────────────────────────────────────────────

    /**
     * Vào vòng lặp poll vô hạn.
     * Dừng bằng Ctrl+C hoặc signal SIGTERM.
     */
    public function listen(): void
    {
        if (empty($this->topics)) {
            throw new \LogicException('[wf-kafka] Chưa gọi onTopic() trước khi listen().');
        }
        if ($this->handler === null) {
            throw new \LogicException('[wf-kafka] Chưa đăng ký handle() trước khi listen().');
        }

        $this->rdConsumer->subscribe($this->topics);
        Log::info('[wf-kafka][Consumer] Listening', ['topics' => $this->topics, 'group' => $this->groupId]);

        while (true) {
            $message = $this->rdConsumer->consume($this->pollTimeoutMs);
            $this->dispatch($message);
        }
    }

    // ── Dispatch single message ────────────────────────────────────────────

    private function dispatch(Message $message): void
    {
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                return; // Không có message, bình thường

            case RD_KAFKA_RESP_ERR_NO_ERROR:
                $this->process($message);
                return;

            default:
                Log::error('[wf-kafka][Consumer] Poll error', [
                    'err' => $message->err,
                    'msg' => $message->errstr(),
                ]);
                return;
        }
    }

    private function process(Message $message): void
    {
        // ── BƯỚC 1: Deserialize raw bytes → array ─────────────────────────
        // Package chịu trách nhiệm decode. Nếu thất bại → EDL ngay.
        try {
            $envelope = $this->serializer->deserialize($message->payload, $message->topic_name);
        } catch (DeserializationException $e) {
            $this->routeToEdl(
                $message->topic_name,
                $message->payload,
                ['event_id' => 'DECODE_FAILED', 'event_type' => 'UNKNOWN', 'payload' => []],
                'Deserialization failed: ' . $e->getMessage()
            );
            $this->rdConsumer->commit($message);
            return;
        }

        // ── BƯỚC 2: Validate envelope tối thiểu ──────────────────────────
        // Package chỉ kiểm tra cấu trúc bắt buộc của envelope, không validate business data.
        if (!is_array($envelope) || empty($envelope['event_id'])) {
            $this->routeToEdl(
                $message->topic_name,
                $message->payload,
                ['event_id' => 'MISSING', 'event_type' => 'UNKNOWN', 'payload' => []],
                'Malformed envelope: missing event_id'
            );
            $this->rdConsumer->commit($message);
            return;
        }

        $eventId   = $envelope['event_id'];
        $eventType = $envelope['event_type'] ?? 'UNKNOWN';

        $originalPayload = array_key_exists('payload', $envelope)
            ? $envelope['payload']
            : $envelope;

        $meta = [
            'event_id'   => $eventId,
            'event_type' => $eventType,
            'topic'      => $message->topic_name,
            'partition'  => $message->partition,
            'offset'     => $message->offset,
        ];

        // ── BƯỚC 3: Idempotency guard (transaction riêng của package) ─────
        // Package tự quản lý transaction này — KHÔNG liên quan đến transaction của host.
        // Ghi trước khi gọi handler. Nếu đã tồn tại → duplicate, bỏ qua an toàn.
        try {
            DB::transaction(function () use ($eventId, $eventType, $message) {
                ProcessedEvent::create([
                    'event_id'     => $eventId,
                    'event_type'   => $eventType,
                    'topic'        => $message->topic_name,
                    'processed_at' => now(),
                ]);
            });
        } catch (QueryException $e) {
            if ($e->getCode() === '23000' || str_contains($e->getMessage(), 'Duplicate entry')) {
                // event_id đã xử lý trước đó → bỏ qua, commit offset để không bị deliver lại
                Log::info('[wf-kafka][Consumer] ⚡ Duplicate skipped', ['event_id' => $eventId]);
                $this->rdConsumer->commit($message);
                return;
            }
            // Lỗi DB thật (không phải duplicate) → DLQ, không commit
            $this->routeToDlq($message->topic_name, $envelope, 'DB error during idempotency check: ' . $e->getMessage());
            if ($this->onError) ($this->onError)('dlq', $envelope, $e);
            return;
        }

        // ── BƯỚC 4: Giao payload cho host xử lý ──────────────────────────
        // Package KHÔNG mở transaction ở đây.
        // Host project tự quyết định có cần transaction không và scope ra sao.
        // Package chỉ quan sát kết quả qua exception type.
        try {
            ($this->handler)($originalPayload, $meta);

            // Handler hoàn thành không có exception → ACK
            Log::info('[wf-kafka][Consumer] ✅ ACK', ['event_id' => $eventId]);

        } catch (TransientInfraException $e) {
            // Lỗi hạ tầng tạm thời: host báo cần retry
            // → Xóa idempotency record để lần retry tiếp theo không bị skip
            // → KHÔNG commit offset để Kafka re-deliver
            $this->rollbackIdempotency($eventId);
            $this->routeToDlq($message->topic_name, $envelope, $e->getMessage());
            if ($this->onError) ($this->onError)('dlq', $envelope, $e);
            return;

        } catch (\Throwable $e) {
            // Mọi exception khác (PoisonPillException, RuntimeException, v.v.)
            // → Poison pill, không thể retry
            // → Giữ idempotency record (tránh xử lý lại nếu redrive)
            // → Commit offset để không block queue
            $this->routeToEdl($message->topic_name, $message->payload, $envelope, $e->getMessage());
            if ($this->onError) ($this->onError)('edl', $envelope, $e);
        }

        // Commit offset: ACK hoặc EDL đều giải phóng offset
        $this->rdConsumer->commit($message);
    }

    /**
     * Xóa idempotency record khi handler báo lỗi transient (DLQ).
     * Cần thiết để lần retry tiếp theo không bị bỏ qua nhầm.
     */
    private function rollbackIdempotency(string $eventId): void
    {
        try {
            ProcessedEvent::where('event_id', $eventId)->delete();
        } catch (\Throwable $e) {
            Log::warning('[wf-kafka][Consumer] Failed to rollback idempotency record', [
                'event_id' => $eventId,
                'error'    => $e->getMessage(),
            ]);
        }
    }

    // ── DLQ ────────────────────────────────────────────────────────────────

    private function routeToDlq(string $sourceTopic, array $original, string $reason): void
    {
        $dlqTopic = $sourceTopic . $this->dlqSuffix;
        $dlqPayload = [
            'failed_at'        => now()->toIso8601String(),
            'reason'           => $reason,
            'original_message' => $original,
        ];

        try {
            $topic = $this->dlqProducer->newTopic($dlqTopic);
            $topic->produce(
                RD_KAFKA_PARTITION_UA, 0,
                json_encode($dlqPayload, JSON_UNESCAPED_UNICODE),
                $original['event_id'] ?? null
            );
            $this->dlqProducer->flush(5000);
        } catch (\Throwable $e) {
            Log::error('[wf-kafka][DLQ] Failed to publish to DLQ', ['error' => $e->getMessage()]);
        }

        // Log + persist vào DB để có thể redrive sau
        FailedEventLog::create([
            'event_id'      => $original['event_id']   ?? 'MISSING',
            'event_type'    => $original['event_type'] ?? 'UNKNOWN',
            'original_topic'=> $sourceTopic,
            'failure_type'  => 'DLQ',
            'payload'       => $original,
            'failure_reason'=> $reason,
            'status'        => 'PENDING_FIX',
            'alerted_at'    => null,
        ]);

        $this->telegram->alertDlq($sourceTopic, $original['event_id'] ?? 'N/A', $reason);

        Log::warning('[wf-kafka][Consumer] ⚠️ DLQ', [
            'topic'    => $dlqTopic,
            'event_id' => $original['event_id'] ?? 'N/A',
            'reason'   => $reason,
        ]);
    }

    // ── EDL ────────────────────────────────────────────────────────────────

    private function routeToEdl(
        string $sourceTopic,
        string $rawPayload,
        array  $parsed,
        string $reason
    ): void {
        FailedEventLog::create([
            'event_id'       => $parsed['event_id']   ?? 'MISSING',
            'event_type'     => $parsed['event_type'] ?? 'UNKNOWN',
            'original_topic' => $sourceTopic,
            'failure_type'   => 'EDL',
            'payload'        => $parsed ?: ['raw' => $rawPayload],
            'failure_reason' => $reason,
            'status'         => 'PENDING_FIX',
            'alerted_at'     => null,
        ]);

        $this->telegram->alertEdl($sourceTopic, $parsed['event_id'] ?? 'N/A', $reason);

        Log::critical('[wf-kafka][Consumer] 🚨 EDL', [
            'topic'    => $sourceTopic,
            'event_id' => $parsed['event_id'] ?? 'N/A',
            'reason'   => $reason,
        ]);
    }
}
