#!/usr/bin/env bash
# =============================================================================
# wf/kafka — librdkafka + ext-rdkafka installer
# =============================================================================
# Sử dụng:
#   bash vendor/wf/kafka/scripts/install-rdkafka.sh
#   bash vendor/wf/kafka/scripts/install-rdkafka.sh --check
#
# Hỗ trợ: macOS (Homebrew), Ubuntu/Debian, Alpine, RHEL/CentOS/Amazon Linux
# =============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

CHECK_ONLY=0
for arg in "$@"; do
    [[ "$arg" == "--check" ]] && CHECK_ONLY=1
done

log_ok()   { echo -e "${GREEN}  ✅  $1${NC}"; }
log_warn() { echo -e "${YELLOW}  ⚠️   $1${NC}"; }
log_err()  { echo -e "${RED}  ❌  $1${NC}"; }
log_info() { echo -e "${CYAN}  ℹ️   $1${NC}"; }
log_step() { echo -e "\n${CYAN}  ▶ $1${NC}"; }

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   wf/kafka — librdkafka + ext-rdkafka setup  ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

# ── Detect OS ──────────────────────────────────────────────────────────────
detect_os() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "macos"
    elif [[ -f /etc/os-release ]]; then
        source /etc/os-release
        case "${ID:-}" in
            alpine)                      echo "alpine" ;;
            rhel|centos|amzn|fedora)     echo "rhel"   ;;
            *)
                if echo "${ID_LIKE:-}" | grep -qiE 'rhel|fedora|centos'; then
                    echo "rhel"
                else
                    echo "debian"   # Ubuntu, Debian, etc.
                fi
                ;;
        esac
    else
        echo "unknown"
    fi
}

OS=$(detect_os)

# ── Check functions ────────────────────────────────────────────────────────
check_librdkafka() {
    if php -r "extension_loaded('rdkafka') && exit(0); exit(1);" 2>/dev/null; then
        return 0  # ext loaded → librdkafka definitely present
    fi

    case "$OS" in
        macos)   brew list librdkafka &>/dev/null ;;
        alpine)  apk info -e librdkafka-dev &>/dev/null ;;
        debian)  dpkg -l librdkafka-dev &>/dev/null 2>&1 ;;
        rhel)    rpm -q librdkafka-devel &>/dev/null 2>&1 ;;
        *)       ldconfig -p 2>/dev/null | grep -q librdkafka ;;
    esac
}

check_ext_rdkafka() {
    php -r "exit(extension_loaded('rdkafka') ? 0 : 1);" 2>/dev/null
}

# ── Print status ───────────────────────────────────────────────────────────
PHP_VER=$(php -r "echo PHP_VERSION;")
echo "  PHP version  : $PHP_VER"
echo "  OS detected  : $OS"
echo ""

check_librdkafka  && log_ok "librdkafka      : installed" \
                  || log_err "librdkafka      : NOT found"

check_ext_rdkafka && log_ok "ext-rdkafka     : loaded" \
                  || log_err "ext-rdkafka     : NOT loaded"

echo ""

# ── Check only mode ────────────────────────────────────────────────────────
if [[ $CHECK_ONLY -eq 1 ]]; then
    if check_ext_rdkafka && check_librdkafka; then
        log_ok "All good — ready to use wf/kafka!"
        exit 0
    else
        log_err "Environment not ready."
        exit 1
    fi
fi

# ── Already installed? ─────────────────────────────────────────────────────
if check_ext_rdkafka; then
    log_ok "ext-rdkafka already loaded. Nothing to do!"
    exit 0
fi

# ── Install ────────────────────────────────────────────────────────────────
SUDO=""
[[ "$EUID" -ne 0 && "$OS" != "macos" ]] && SUDO="sudo"

case "$OS" in

    # ── macOS ──────────────────────────────────────────────────────────────
    macos)
        if ! command -v brew &>/dev/null; then
            log_err "Homebrew not found. Install it first: https://brew.sh"
            exit 1
        fi

        if ! check_librdkafka; then
            log_step "Installing librdkafka via Homebrew..."
            brew install librdkafka
        fi

        log_step "Installing ext-rdkafka via PECL..."
        pecl install rdkafka || true

        # Homebrew PHP: thêm vào php.ini nếu chưa có
        PHP_INI=$(php -r "echo php_ini_loaded_file();")
        if [[ -n "$PHP_INI" ]] && ! grep -q "^extension=rdkafka" "$PHP_INI"; then
            log_step "Adding extension=rdkafka to $PHP_INI"
            echo "extension=rdkafka" >> "$PHP_INI"
        fi
        ;;

    # ── Ubuntu / Debian ────────────────────────────────────────────────────
    debian)
        if ! check_librdkafka; then
            log_step "Installing librdkafka-dev..."
            $SUDO apt-get update -qq
            $SUDO apt-get install -y librdkafka-dev
        fi

        log_step "Installing ext-rdkafka via PECL..."
        $SUDO pecl install rdkafka || true

        PHP_INI=$(php -r "echo php_ini_loaded_file();")
        if [[ -n "$PHP_INI" ]] && ! grep -q "^extension=rdkafka" "$PHP_INI"; then
            log_step "Adding extension=rdkafka to $PHP_INI"
            echo "extension=rdkafka" | $SUDO tee -a "$PHP_INI" > /dev/null
        fi

        # php.d directory (Ubuntu package installs)
        PHP_CONF_DIR=$(php -r "echo PHP_MAJOR_VERSION . '.' . PHP_MINOR_VERSION;")
        for dir in "/etc/php/${PHP_CONF_DIR}/cli/conf.d" "/etc/php/${PHP_CONF_DIR}/fpm/conf.d"; do
            if [[ -d "$dir" ]] && ! ls "$dir"/*rdkafka* &>/dev/null; then
                echo "extension=rdkafka.so" | $SUDO tee "$dir/20-rdkafka.ini" > /dev/null
                log_info "Added $dir/20-rdkafka.ini"
            fi
        done
        ;;

    # ── Alpine (Docker) ────────────────────────────────────────────────────
    alpine)
        if ! check_librdkafka; then
            log_step "Installing librdkafka-dev via apk..."
            apk add --no-cache librdkafka-dev
        fi

        log_step "Installing ext-rdkafka via PECL..."
        apk add --no-cache $( \
            apk info -e php-dev &>/dev/null && echo "php-dev" || \
            apk info -e php83-dev &>/dev/null && echo "php83-dev" || \
            echo "php-dev" \
        ) || true
        pecl install rdkafka || true

        # Docker PHP official image helper
        if command -v docker-php-ext-enable &>/dev/null; then
            log_step "Enabling extension via docker-php-ext-enable..."
            docker-php-ext-enable rdkafka
        fi
        ;;

    # ── RHEL / CentOS / Amazon Linux ──────────────────────────────────────
    rhel)
        if ! check_librdkafka; then
            log_step "Installing librdkafka-devel..."
            if command -v dnf &>/dev/null; then
                $SUDO dnf install -y librdkafka-devel
            else
                $SUDO yum install -y librdkafka-devel
            fi
        fi

        log_step "Installing ext-rdkafka via PECL..."
        $SUDO pecl install rdkafka || true

        PHP_INI=$(php -r "echo php_ini_loaded_file();")
        if [[ -n "$PHP_INI" ]] && ! grep -q "^extension=rdkafka" "$PHP_INI"; then
            echo "extension=rdkafka" | $SUDO tee -a "$PHP_INI" > /dev/null
        fi
        ;;

    *)
        log_err "Unsupported OS: $OS"
        log_info "Please install manually: https://github.com/arnaud-lb/php-rdkafka#installation"
        exit 1
        ;;
esac

# ── Verify ─────────────────────────────────────────────────────────────────
echo ""
if check_ext_rdkafka; then
    RDKAFKA_VER=$(php -r "echo phpversion('rdkafka');")
    log_ok "🎉 ext-rdkafka v${RDKAFKA_VER} is ready!"
    log_ok "Run: php artisan kafka:install --check to verify"
else
    log_warn "ext-rdkafka installed but not yet active."
    log_info "You may need to restart PHP-FPM or your web server."
    log_info "PHP ini file: $(php -r "echo php_ini_loaded_file();")"
    log_info "Add manually if missing: extension=rdkafka"
    exit 1
fi
