<?php

namespace Wf\Kafka\Console\Commands;

use Illuminate\Console\Command;
use Wf\Kafka\Producer\OutboxRelayer;

class OutboxRelayCommand extends Command
{
    protected $signature = 'kafka:outbox-relay
                            {--loop             : Chạy liên tục thay vì một lần rồi thoát}
                            {--interval=5       : Giây giữa các lần scan khi dùng --loop}';

    protected $description = '[wf-kafka] Relay outbox events PENDING/FAILED lên Kafka';

    public function handle(OutboxRelayer $relayer): int
    {
        if ($this->option('loop')) {
            $interval = max(1, (int) $this->option('interval'));
            $this->info("🔄 Relayer loop started (interval={$interval}s). Press Ctrl+C to stop.");

            while (true) {
                $this->runOnce($relayer);
                sleep($interval);
            }
        }

        $this->runOnce($relayer);
        return Command::SUCCESS;
    }

    private function runOnce(OutboxRelayer $relayer): void
    {
        $stats = $relayer->relay();

        if ($stats['sent'] > 0)    $this->info("  ✅ Sent    : {$stats['sent']}");
        if ($stats['failed'] > 0)  $this->warn("  ❌ Failed  : {$stats['failed']}");
        if ($stats['skipped'] > 0) $this->warn("  ⛔ Skipped : {$stats['skipped']}");
        if (array_sum($stats) === 0) $this->line('  — No pending events.');
    }
}
