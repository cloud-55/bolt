#!/bin/bash
set -e

# Bolt Installer
# Usage: curl -fsSL https://raw.githubusercontent.com/cloud-55/bolt/main/install.sh | bash

REPO="cloud-55/bolt"
INSTALL_DIR="${BOLT_INSTALL_DIR:-/usr/local/bin}"
GITHUB_API="https://api.github.com/repos/${REPO}/releases/latest"
GITHUB_DOWNLOAD="https://github.com/${REPO}/releases/download"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# Detect OS
detect_os() {
    local os
    os="$(uname -s)"
    case "$os" in
        Linux*)  echo "linux" ;;
        Darwin*) echo "darwin" ;;
        *)       error "Unsupported operating system: $os" ;;
    esac
}

# Detect architecture
detect_arch() {
    local arch
    arch="$(uname -m)"
    case "$arch" in
        x86_64)  echo "x86_64" ;;
        amd64)   echo "x86_64" ;;
        aarch64) echo "aarch64" ;;
        arm64)   echo "aarch64" ;;
        *)       error "Unsupported architecture: $arch" ;;
    esac
}

# Get latest version from GitHub
get_latest_version() {
    local version
    if command -v curl &> /dev/null; then
        version=$(curl -fsSL "$GITHUB_API" | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')
    elif command -v wget &> /dev/null; then
        version=$(wget -qO- "$GITHUB_API" | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')
    else
        error "Neither curl nor wget found. Please install one of them."
    fi

    if [ -z "$version" ]; then
        error "Could not determine latest version. Check your internet connection."
    fi

    echo "$version"
}

# Download file
download() {
    local url="$1"
    local output="$2"

    info "Downloading from $url"

    if command -v curl &> /dev/null; then
        curl -fsSL "$url" -o "$output"
    elif command -v wget &> /dev/null; then
        wget -q "$url" -O "$output"
    else
        error "Neither curl nor wget found."
    fi
}

main() {
    echo ""
    echo "  ⚡ Bolt Installer"
    echo "  =========================="
    echo ""

    local os=$(detect_os)
    local arch=$(detect_arch)
    local platform="bolt-${os}-${arch}"

    info "Detected platform: $os ($arch)"

    # Get latest version
    info "Fetching latest version..."
    local version=$(get_latest_version)
    info "Latest version: $version"

    # Create temp directory
    local tmp_dir=$(mktemp -d)
    trap "rm -rf $tmp_dir" EXIT

    # Download archive
    local archive_url="${GITHUB_DOWNLOAD}/${version}/${platform}.tar.gz"
    local archive_path="${tmp_dir}/${platform}.tar.gz"

    download "$archive_url" "$archive_path"

    # Verify download
    if [ ! -f "$archive_path" ]; then
        error "Download failed"
    fi

    success "Downloaded ${platform}.tar.gz"

    # Extract
    info "Extracting..."
    tar -xzf "$archive_path" -C "$tmp_dir"

    # Check if we need sudo
    local use_sudo=""
    if [ ! -w "$INSTALL_DIR" ]; then
        if [ "$EUID" -ne 0 ]; then
            warn "Installation directory $INSTALL_DIR requires root access"

            # Try user-local installation
            if [ -d "$HOME/.local/bin" ] || mkdir -p "$HOME/.local/bin" 2>/dev/null; then
                INSTALL_DIR="$HOME/.local/bin"
                warn "Installing to $INSTALL_DIR instead"
                warn "Make sure $INSTALL_DIR is in your PATH"
            else
                use_sudo="sudo"
                info "Using sudo for installation"
            fi
        fi
    fi

    # Install binaries
    info "Installing to $INSTALL_DIR..."

    $use_sudo mkdir -p "$INSTALL_DIR"
    $use_sudo cp "$tmp_dir/bolt" "$INSTALL_DIR/bolt"
    $use_sudo cp "$tmp_dir/boltctl" "$INSTALL_DIR/boltctl"
    $use_sudo chmod +x "$INSTALL_DIR/bolt"
    $use_sudo chmod +x "$INSTALL_DIR/boltctl"

    success "Installed bolt to $INSTALL_DIR/bolt"
    success "Installed boltctl to $INSTALL_DIR/boltctl"

    # Create default config file (~/.boltrc) if it doesn't exist
    local config_file="$HOME/.boltrc"
    if [ ! -f "$config_file" ]; then
        info "Creating default config at $config_file"
        cat > "$config_file" << 'EOF'
host = "127.0.0.1"
port = 8518
EOF
        chmod 600 "$config_file"
        success "Config created at $config_file"
        info "After starting the server, login with: boltctl login"
        info "The admin password is shown in the server log on first start"
        info "Set BOLT_ADMIN_PASSWORD env var to choose your own password"
    else
        info "Config file already exists at $config_file"
    fi

    # Verify installation
    echo ""
    if [ -x "$INSTALL_DIR/bolt" ] && [ -x "$INSTALL_DIR/boltctl" ]; then
        success "Installation complete!"

        if ! command -v bolt &> /dev/null; then
            warn "bolt is not in PATH"
            echo ""
            echo "Add this to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
            echo ""
            echo "  export PATH=\"$INSTALL_DIR:\$PATH\""
            echo ""
        fi
    else
        error "Installation verification failed"
    fi

    # Start server if requested
    if [ "${BOLT_START:-false}" = "true" ] || [ "${BOLT_START:-0}" = "1" ]; then
        info "Starting Bolt server in background..."
        "$INSTALL_DIR/bolt" > /dev/null 2>&1 &
        sleep 1
        if pgrep -x bolt > /dev/null; then
            success "Bolt server started (PID: $(pgrep -x bolt))"
            echo ""
            echo "Server running on port 8518"
            echo "Stop with: pkill bolt"
        else
            warn "Failed to start server"
        fi
    fi

    echo ""
    echo "Quick Start:"
    echo "  boltctl put key value   # Store a value"
    echo "  boltctl get key         # Retrieve a value"
    echo ""
    echo "Authentication:"
    echo "  Start the server and check the log for the admin password"
    echo "  Login: boltctl login"
    echo "  Or set BOLT_ADMIN_PASSWORD env var before starting the server"
    echo ""
    echo "Documentation: https://github.com/${REPO}"
    echo ""
}

main "$@"
