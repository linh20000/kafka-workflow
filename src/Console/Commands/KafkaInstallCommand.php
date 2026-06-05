<?php

namespace Wf\Kafka\Console\Commands;

use Illuminate\Console\Command;

/**
 * Kiểm tra môi trường và hướng dẫn / tự động cài librdkafka + ext-rdkafka.
 *
 * Sử dụng:
 *   php artisan kafka:install
 *   php artisan kafka:install --force   # Bỏ qua confirm, chạy thẳng
 *   php artisan kafka:install --check   # Chỉ kiểm tra, không cài
 */
class KafkaInstallCommand extends Command
{
    protected $signature = 'kafka:install
                            {--check  : Chỉ kiểm tra trạng thái, không cài đặt}
                            {--force  : Tự động chạy lệnh cài không hỏi confirm}';

    protected $description = '[wf-kafka] Kiểm tra và hướng dẫn cài librdkafka + ext-rdkafka';

    public function handle(): int
    {
        $this->line('');
        $this->line('╔══════════════════════════════════════════╗');
        $this->line('║      wf/kafka — Environment Check        ║');
        $this->line('╚══════════════════════════════════════════╝');
        $this->line('');

        $status = $this->checkEnvironment();

        if ($this->option('check')) {
            return $status['all_ok'] ? Command::SUCCESS : Command::FAILURE;
        }

        if ($status['all_ok']) {
            $this->info('✅ Môi trường đã sẵn sàng. Không cần cài thêm gì.');
            return Command::SUCCESS;
        }

        // Hướng dẫn hoặc tự động cài
        $this->line('');
        $this->warn('⚠️  Một số thành phần chưa sẵn sàng. Đang chuẩn bị lệnh cài đặt...');
        $this->line('');

        return $this->runInstallation($status);
    }

    // ── Environment check ──────────────────────────────────────────────────

    private function checkEnvironment(): array
    {
        $checks = [
            'php_version'    => PHP_VERSION_ID >= 80200,
            'ext_rdkafka'    => extension_loaded('rdkafka'),
            'librdkafka'     => $this->detectLibrdkafka(),
            'rdkafka_version'=> null,
        ];

        if ($checks['ext_rdkafka']) {
            $checks['rdkafka_version'] = defined('RD_KAFKA_VERSION')
                ? sprintf('%d.%d.%d',
                    (RD_KAFKA_VERSION >> 24) & 0xff,
                    (RD_KAFKA_VERSION >> 16) & 0xff,
                    (RD_KAFKA_VERSION >> 8)  & 0xff)
                : 'unknown';
        }

        $this->printChecks($checks);

        return array_merge($checks, [
            'all_ok' => $checks['ext_rdkafka'] && $checks['librdkafka'],
            'os'     => $this->detectOs(),
        ]);
    }

    private function printChecks(array $checks): void
    {
        $phpOk = $checks['php_version'];
        $this->line(sprintf('  PHP version      : %s %s',
            PHP_VERSION,
            $phpOk ? '✅' : '❌  (requires ^8.2)'
        ));

        $this->line(sprintf('  librdkafka       : %s',
            $checks['librdkafka'] ? '✅  installed' : '❌  not found'
        ));

        $rdkafkaOk = $checks['ext_rdkafka'];
        $this->line(sprintf('  ext-rdkafka      : %s%s',
            $rdkafkaOk ? '✅  loaded' : '❌  not loaded',
            $rdkafkaOk && $checks['rdkafka_version'] ? '  (v' . $checks['rdkafka_version'] . ')' : ''
        ));
    }

    // ── Installation ───────────────────────────────────────────────────────

    private function runInstallation(array $status): int
    {
        $os = $status['os'];

        $this->line("  Detected OS: <comment>{$os['name']}</comment>");
        $this->line('');

        $steps = $this->buildInstallSteps($os, $status);

        if (empty($steps)) {
            $this->warn('Không thể tự động xác định lệnh cài cho OS này.');
            $this->line('Vui lòng tham khảo: https://github.com/arnaud-lb/php-rdkafka#installation');
            return Command::FAILURE;
        }

        // Hiển thị các lệnh sẽ chạy
        $this->line('  Các lệnh cần thực hiện:');
        $this->line('');
        foreach ($steps as $i => $step) {
            $this->line("  <comment>" . ($i + 1) . ".</comment> {$step['label']}");
            $this->line("     <info>\$ {$step['cmd']}</info>");
            $this->line('');
        }

        if (!$this->option('force')) {
            if (!$this->confirm('Chạy các lệnh trên ngay bây giờ?', true)) {
                $this->line('');
                $this->line('Bạn có thể chạy thủ công các lệnh trên và thử lại.');
                return Command::FAILURE;
            }
        }

        return $this->executeSteps($steps);
    }

    private function buildInstallSteps(array $os, array $status): array
    {
        $steps = [];

        switch ($os['type']) {

            // ── macOS (Homebrew) ──────────────────────────────────────────
            case 'macos':
                if (!$status['librdkafka']) {
                    $steps[] = [
                        'label' => 'Cài librdkafka qua Homebrew',
                        'cmd'   => 'brew install librdkafka',
                        'sudo'  => false,
                    ];
                }
                if (!$status['ext_rdkafka']) {
                    $steps[] = [
                        'label' => 'Cài ext-rdkafka qua PECL',
                        'cmd'   => 'pecl install rdkafka',
                        'sudo'  => false,
                    ];
                    $steps[] = [
                        'label' => 'Kiểm tra php.ini đã có extension=rdkafka chưa (nếu chưa tự thêm)',
                        'cmd'   => 'php -r "echo php_ini_loaded_file();"',
                        'sudo'  => false,
                        'info_only' => true,
                    ];
                }
                break;

            // ── Ubuntu / Debian ───────────────────────────────────────────
            case 'debian':
                if (!$status['librdkafka']) {
                    $steps[] = [
                        'label' => 'Update apt và cài librdkafka-dev',
                        'cmd'   => 'apt-get update && apt-get install -y librdkafka-dev',
                        'sudo'  => true,
                    ];
                }
                if (!$status['ext_rdkafka']) {
                    $steps[] = [
                        'label' => 'Cài ext-rdkafka qua PECL',
                        'cmd'   => 'pecl install rdkafka',
                        'sudo'  => false,
                    ];
                    $steps[] = [
                        'label' => 'Thêm extension=rdkafka vào php.ini',
                        'cmd'   => 'echo "extension=rdkafka" >> ' . php_ini_loaded_file(),
                        'sudo'  => true,
                    ];
                }
                break;

            // ── Alpine (Docker) ───────────────────────────────────────────
            case 'alpine':
                if (!$status['librdkafka']) {
                    $steps[] = [
                        'label' => 'Cài librdkafka-dev qua apk',
                        'cmd'   => 'apk add --no-cache librdkafka-dev',
                        'sudo'  => false,
                    ];
                }
                if (!$status['ext_rdkafka']) {
                    $steps[] = [
                        'label' => 'Cài ext-rdkafka qua PECL',
                        'cmd'   => 'pecl install rdkafka && docker-php-ext-enable rdkafka',
                        'sudo'  => false,
                    ];
                }
                break;

            // ── RHEL / CentOS / Amazon Linux ─────────────────────────────
            case 'rhel':
                if (!$status['librdkafka']) {
                    $steps[] = [
                        'label' => 'Cài librdkafka-devel',
                        'cmd'   => 'yum install -y librdkafka-devel',
                        'sudo'  => true,
                    ];
                }
                if (!$status['ext_rdkafka']) {
                    $steps[] = [
                        'label' => 'Cài ext-rdkafka qua PECL',
                        'cmd'   => 'pecl install rdkafka',
                        'sudo'  => false,
                    ];
                    $steps[] = [
                        'label' => 'Thêm extension=rdkafka vào php.ini',
                        'cmd'   => 'echo "extension=rdkafka" >> ' . php_ini_loaded_file(),
                        'sudo'  => true,
                    ];
                }
                break;

            default:
                return [];
        }

        return $steps;
    }

    private function executeSteps(array $steps): int
    {
        $this->line('');
        $allOk = true;

        foreach ($steps as $i => $step) {
            $label = $step['label'];
            $cmd   = $step['cmd'];
            $sudo  = ($step['sudo'] ?? false) && posix_getuid() !== 0;

            if ($step['info_only'] ?? false) {
                $this->line("  ℹ️  {$label}");
                passthru($cmd);
                $this->line('');
                continue;
            }

            $this->line("  ▶ {$label}");

            $execCmd = $sudo ? "sudo {$cmd}" : $cmd;
            passthru($execCmd, $exitCode);

            if ($exitCode !== 0) {
                $this->error("    ❌ Lệnh thất bại (exit={$exitCode}): {$execCmd}");
                $allOk = false;
                break;
            }

            $this->info("    ✅ Thành công");
            $this->line('');
        }

        if ($allOk) {
            $this->line('');
            $this->info('✅ Cài đặt hoàn tất.');
            $this->line('');

            // Kiểm tra lại sau cài
            if (!extension_loaded('rdkafka')) {
                $this->warn('⚠️  ext-rdkafka chưa load được. Bạn cần restart PHP-FPM / web server.');
                $this->line('   Hoặc thêm thủ công vào php.ini: <info>extension=rdkafka</info>');
                $this->line('   Đường dẫn php.ini hiện tại: <comment>' . php_ini_loaded_file() . '</comment>');
            } else {
                $this->info('🎉 ext-rdkafka đã hoạt động! Bạn đã sẵn sàng dùng wf/kafka.');
            }
        }

        return $allOk ? Command::SUCCESS : Command::FAILURE;
    }

    // ── OS detection ───────────────────────────────────────────────────────

    private function detectOs(): array
    {
        $uname = strtolower(PHP_OS_FAMILY);

        if ($uname === 'darwin') {
            return ['type' => 'macos', 'name' => 'macOS'];
        }

        if ($uname === 'linux') {
            // Đọc /etc/os-release để phân biệt distro
            $osRelease = @file_get_contents('/etc/os-release') ?: '';

            if (str_contains($osRelease, 'alpine')) {
                return ['type' => 'alpine', 'name' => 'Alpine Linux'];
            }
            if (preg_match('/ID_LIKE\s*=.*(?:rhel|fedora|centos)/i', $osRelease)
                || preg_match('/^ID\s*=\s*(?:rhel|centos|amzn|fedora)/im', $osRelease)) {
                return ['type' => 'rhel', 'name' => 'RHEL/CentOS/Amazon Linux'];
            }
            // Default: Ubuntu/Debian
            return ['type' => 'debian', 'name' => 'Ubuntu/Debian'];
        }

        return ['type' => 'unknown', 'name' => PHP_OS_FAMILY];
    }

    private function detectLibrdkafka(): bool
    {
        // Nếu ext-rdkafka đã load thì librdkafka chắc chắn có
        if (extension_loaded('rdkafka')) {
            return true;
        }

        // macOS: kiểm tra qua brew
        if (PHP_OS_FAMILY === 'Darwin') {
            exec('brew list librdkafka 2>/dev/null', $out, $code);
            return $code === 0;
        }

        // Linux: kiểm tra shared library
        exec('ldconfig -p 2>/dev/null | grep librdkafka', $out, $code);
        if ($code === 0 && !empty($out)) {
            return true;
        }

        // Fallback: kiểm tra header file
        $headerPaths = [
            '/usr/include/librdkafka/rdkafka.h',
            '/usr/local/include/librdkafka/rdkafka.h',
            '/opt/homebrew/include/librdkafka/rdkafka.h',
        ];
        foreach ($headerPaths as $path) {
            if (file_exists($path)) return true;
        }

        return false;
    }
}
