#!/bin/bash

# MongoDB Export CLI Installation Script
# This script detects your platform and downloads the appropriate binary

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# GitHub repository
REPO="djkeshawa/mongo-exporter"
BINARY_NAME="mongo-exporter"

# Default installation directory
INSTALL_DIR="/usr/local/bin"

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to detect platform
detect_platform() {
    local os=$(uname -s | tr '[:upper:]' '[:lower:]')
    local arch=$(uname -m)
    
    case "$os" in
        linux*)
            case "$arch" in
                x86_64|amd64)
                    echo "x86_64-unknown-linux-gnu"
                    ;;
                *)
                    print_error "Unsupported architecture: $arch"
                    exit 1
                    ;;
            esac
            ;;
        darwin*)
            case "$arch" in
                x86_64)
                    echo "x86_64-apple-darwin"
                    ;;
                arm64)
                    echo "aarch64-apple-darwin"
                    ;;
                *)
                    print_error "Unsupported architecture: $arch"
                    exit 1
                    ;;
            esac
            ;;
        mingw*|msys*|cygwin*)
            case "$arch" in
                x86_64|amd64)
                    echo "x86_64-pc-windows-msvc"
                    ;;
                *)
                    print_error "Unsupported architecture: $arch"
                    exit 1
                    ;;
            esac
            ;;
        *)
            print_error "Unsupported operating system: $os"
            exit 1
            ;;
    esac
}

# Function to get latest release tag
get_latest_release() {
    local api_url="https://api.github.com/repos/$REPO/releases/latest"
    
    if command -v curl >/dev/null 2>&1; then
        curl -s "$api_url" | grep -o '"tag_name": *"[^"]*"' | cut -d'"' -f4
    elif command -v wget >/dev/null 2>&1; then
        wget -qO- "$api_url" | grep -o '"tag_name": *"[^"]*"' | cut -d'"' -f4
    else
        print_error "Neither curl nor wget is available. Please install one of them."
        exit 1
    fi
}

# Function to download and extract binary
download_and_install() {
    local platform=$1
    local version=$2
    local temp_dir=$(mktemp -d)
    
    # Determine file extension and extraction command
    local file_ext
    local extract_cmd
    if [[ "$platform" == *"windows"* ]]; then
        file_ext=".zip"
        extract_cmd="unzip -q"
    else
        file_ext=".tar.gz"
        extract_cmd="tar -xzf"
    fi
    
    local filename="${BINARY_NAME}-${platform}${file_ext}"
    local download_url="https://github.com/$REPO/releases/download/$version/$filename"
    
    print_status "Downloading $filename..."
    
    cd "$temp_dir"
    
    if command -v curl >/dev/null 2>&1; then
        curl -sL "$download_url" -o "$filename"
    elif command -v wget >/dev/null 2>&1; then
        wget -q "$download_url" -O "$filename"
    else
        print_error "Neither curl nor wget is available."
        exit 1
    fi
    
    if [[ ! -f "$filename" ]]; then
        print_error "Failed to download $filename"
        exit 1
    fi
    
    print_status "Extracting archive..."
    $extract_cmd "$filename"
    
    # Find the binary (handle both .exe and no extension)
    local binary_path
    if [[ "$platform" == *"windows"* ]]; then
        binary_path="$BINARY_NAME.exe"
    else
        binary_path="$BINARY_NAME"
    fi
    
    if [[ ! -f "$binary_path" ]]; then
        print_error "Binary not found in archive"
        exit 1
    fi
    
    # Install binary
    if [[ "$EUID" -eq 0 ]] || [[ -w "$INSTALL_DIR" ]]; then
        print_status "Installing to $INSTALL_DIR..."
        cp "$binary_path" "$INSTALL_DIR/"
        chmod +x "$INSTALL_DIR/$binary_path"
    else
        print_warning "No write permission to $INSTALL_DIR. Trying with sudo..."
        sudo cp "$binary_path" "$INSTALL_DIR/"
        sudo chmod +x "$INSTALL_DIR/$binary_path"
    fi
    
    # Cleanup
    cd - >/dev/null
    rm -rf "$temp_dir"
    
    print_success "MongoDB Export CLI installed successfully!"
}

# Function to verify installation
verify_installation() {
    if command -v "$BINARY_NAME" >/dev/null 2>&1; then
        local version_output=$($BINARY_NAME --version 2>/dev/null || echo "unknown")
        print_success "Installation verified: $version_output"
        print_status "You can now run: $BINARY_NAME --help"
    else
        print_warning "Binary installed but not found in PATH. You may need to:"
        print_warning "  1. Restart your terminal"
        print_warning "  2. Add $INSTALL_DIR to your PATH"
        print_warning "  3. Or run directly: $INSTALL_DIR/$BINARY_NAME"
    fi
}

# Main installation flow
main() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "                    MongoDB Export CLI - Installation Script"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --dir)
                INSTALL_DIR="$2"
                shift 2
                ;;
            --version)
                VERSION="$2"
                shift 2
                ;;
            --help)
                echo "Usage: $0 [OPTIONS]"
                echo
                echo "Options:"
                echo "  --dir DIR        Installation directory (default: /usr/local/bin)"
                echo "  --version VER    Specific version to install (default: latest)"
                echo "  --help           Show this help message"
                echo
                echo "Example:"
                echo "  curl -sSL https://raw.githubusercontent.com/$REPO/main/install.sh | bash"
                echo "  curl -sSL https://raw.githubusercontent.com/$REPO/main/install.sh | bash -s -- --dir ~/.local/bin"
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
    
    # Check if binary already exists
    if command -v "$BINARY_NAME" >/dev/null 2>&1; then
        print_warning "$BINARY_NAME is already installed"
        read -p "Do you want to reinstall? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            print_status "Installation cancelled"
            exit 0
        fi
    fi
    
    # Detect platform
    print_status "Detecting platform..."
    platform=$(detect_platform)
    print_status "Detected platform: $platform"
    
    # Get version to install
    if [[ -z "$VERSION" ]]; then
        print_status "Getting latest release version..."
        VERSION=$(get_latest_release)
        if [[ -z "$VERSION" ]]; then
            print_error "Failed to get latest release version"
            exit 1
        fi
    fi
    print_status "Installing version: $VERSION"
    
    # Create install directory if it doesn't exist
    if [[ ! -d "$INSTALL_DIR" ]]; then
        print_status "Creating installation directory: $INSTALL_DIR"
        if [[ "$EUID" -eq 0 ]] || mkdir -p "$INSTALL_DIR" 2>/dev/null; then
            :
        else
            sudo mkdir -p "$INSTALL_DIR"
        fi
    fi
    
    # Download and install
    download_and_install "$platform" "$VERSION"
    
    # Verify installation
    verify_installation
    
    echo
    print_success "Installation complete!"
    echo
    echo "Quick start:"
    echo "  $BINARY_NAME export --help"
    echo "  $BINARY_NAME export --uri 'mongodb://localhost:27017'"
    echo
    echo "For more information, visit: https://github.com/$REPO"
}

# Run main function
main "$@"