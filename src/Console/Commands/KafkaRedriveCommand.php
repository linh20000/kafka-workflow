<?php

namespace Wf\Kafka\Console\Commands;

use Illuminate\Console\Command;
use Wf\Kafka\Models\FailedEventLog;
use Wf\Kafka\Producer\OutboxWriter;

/**
 * Redrive: lấy các bản ghi DLQ từ kafka_failed_event_logs
 * và đẩy lại vào outbox để Debezium bắt và publish lại.
 *
 * Sử dụng:
 *   php artisan kafka:redrive --type=dlq --topic=order-events
 *   php artisan kafka:redrive --type=dlq --limit=50
 */
class KafkaRedriveCommand extends Command
{
    protected $signature = 'kafka:redrive
                            {--type=dlq   : Loại cần redrive: dlq | edl}
                            {--topic=     : Lọc theo topic gốc (tuỳ chọn)}
                            {--limit=100  : Số bản ghi tối đa}
                            {--dry-run    : Xem trước, không thực sự redrive}';

    protected $description = '[wf-kafka] Redrive DLQ/EDL records back vào outbox';

    public function handle(): int
    {
        $type    = strtoupper($this->option('type'));
        $topic   = $this->option('topic');
        $limit   = (int) $this->option('limit');
        $dryRun  = $this->option('dry-run');

        $query = FailedEventLog::where('failure_type', $type)
                               ->where('status', 'PENDING_FIX');

        if ($topic) {
            $query->where('original_topic', $topic);
        }

        $records = $query->limit($limit)->get();

        if ($records->isEmpty()) {
            $this->info('No records to redrive.');
            return Command::SUCCESS;
        }

        $this->info("Found {$records->count()} records to redrive (type={$type}).");

        if ($dryRun) {
            $this->table(
                ['ID', 'event_id', 'event_type', 'topic', 'reason'],
                $records->map(fn($r) => [$r->id, $r->event_id, $r->event_type, $r->original_topic, substr($r->failure_reason, 0, 60)])
            );
            $this->warn('Dry-run mode: no changes made.');
            return Command::SUCCESS;
        }

        $redriven = 0;
        foreach ($records as $record) {
            // Viết lại vào outbox để Debezium xử lý lại
            OutboxWriter::write(
                topic:     $record->original_topic,
                eventType: $record->event_type,
                payload:   $record->payload
            );

            // Đánh dấu đã redrive
            $record->update(['status' => 'REDRIVEN']);
            $redriven++;

            $this->line("  🔄 Redriven: event_id={$record->event_id}");
        }

        $this->info("✅ Redriven {$redriven} records into outbox.");
        return Command::SUCCESS;
    }
}
