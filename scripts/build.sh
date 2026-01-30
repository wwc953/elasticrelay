#!/bin/bash

# 构建脚本 - 支持版本信息注入和多平台编译
#
# 用法:
#   ./build.sh              # 仅构建当前平台
#   ./build.sh --all        # 构建所有平台
#   ./build.sh --linux      # 仅构建 Linux 平台 (amd64 + arm64)
#   ./build.sh --darwin     # 仅构建 macOS 平台 (amd64 + arm64)
#   ./build.sh --windows    # 仅构建 Windows 平台 (amd64)
#
# 环境变量:
#   VERSION     - 版本号 (默认: dev)
#   OUTPUT_DIR  - 输出目录 (默认: bin)
#   CGO_ENABLED - CGO 开关 (多平台构建默认: 0)

set -e

# 默认值
VERSION=${VERSION:-"dev"}
OUTPUT_DIR=${OUTPUT_DIR:-"bin"}
BINARY_NAME="elasticrelay"

# 获取Git信息
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME=$(date -u '+%Y-%m-%d_%H:%M:%S_UTC')

# 版本信息注入的包路径
VERSION_PKG="github.com/yogoosoft/elasticrelay/internal/version"

# 构建标志
LDFLAGS="-X '${VERSION_PKG}.Version=${VERSION}' -X '${VERSION_PKG}.GitCommit=${GIT_COMMIT}' -X '${VERSION_PKG}.BuildTime=${BUILD_TIME}' -s -w"

# 创建输出目录
mkdir -p "${OUTPUT_DIR}"

echo "=========================================="
echo "Building ElasticRelay"
echo "=========================================="
echo "Version:    ${VERSION}"
echo "Git Commit: ${GIT_COMMIT}"
echo "Build Time: ${BUILD_TIME}"
echo ""

# 定义支持的平台
PLATFORMS_LINUX="linux/amd64 linux/arm64"
PLATFORMS_DARWIN="darwin/amd64 darwin/arm64"
PLATFORMS_WINDOWS="windows/amd64"
PLATFORMS_ALL="${PLATFORMS_LINUX} ${PLATFORMS_DARWIN} ${PLATFORMS_WINDOWS}"

# 构建单个平台的函数
build_platform() {
    local platform=$1
    local os=$(echo ${platform} | cut -d'/' -f1)
    local arch=$(echo ${platform} | cut -d'/' -f2)
    local output_name="${BINARY_NAME}-${os}-${arch}"
    
    # Windows 需要 .exe 后缀
    if [ "${os}" = "windows" ]; then
        output_name="${output_name}.exe"
    fi
    
    echo "Building for ${os}/${arch}..."
    
    CGO_ENABLED=${CGO_ENABLED:-0} GOOS=${os} GOARCH=${arch} go build \
        -ldflags "${LDFLAGS}" \
        -o "${OUTPUT_DIR}/${output_name}" \
        ./cmd/elasticrelay
    
    echo "  -> ${OUTPUT_DIR}/${output_name}"
}

# 构建多个平台
build_platforms() {
    local platforms=$1
    for platform in ${platforms}; do
        build_platform "${platform}"
    done
}

# 构建当前平台
build_current() {
    echo "Building for current platform..."
    
    go build \
        -ldflags "${LDFLAGS}" \
        -o "${OUTPUT_DIR}/${BINARY_NAME}" \
        ./cmd/elasticrelay
    
    echo "  -> ${OUTPUT_DIR}/${BINARY_NAME}"
    
    # 显示版本信息
    echo ""
    echo "Version info:"
    "${OUTPUT_DIR}/${BINARY_NAME}" --version 2>/dev/null || echo "Binary built successfully"
}

# 解析命令行参数
case "${1:-}" in
    --all|-a)
        echo "Target: All platforms"
        echo ""
        build_platforms "${PLATFORMS_ALL}"
        ;;
    --linux|-l)
        echo "Target: Linux platforms"
        echo ""
        build_platforms "${PLATFORMS_LINUX}"
        ;;
    --darwin|-d|--macos|-m)
        echo "Target: macOS platforms"
        echo ""
        build_platforms "${PLATFORMS_DARWIN}"
        ;;
    --windows|-w)
        echo "Target: Windows platforms"
        echo ""
        build_platforms "${PLATFORMS_WINDOWS}"
        ;;
    --help|-h)
        echo "用法:"
        echo "  ./build.sh              # 仅构建当前平台"
        echo "  ./build.sh --all        # 构建所有平台"
        echo "  ./build.sh --linux      # 仅构建 Linux 平台 (amd64 + arm64)"
        echo "  ./build.sh --darwin     # 仅构建 macOS 平台 (amd64 + arm64)"
        echo "  ./build.sh --windows    # 仅构建 Windows 平台 (amd64)"
        echo ""
        echo "环境变量:"
        echo "  VERSION=v1.0.0          # 设置版本号"
        echo "  OUTPUT_DIR=dist         # 设置输出目录"
        exit 0
        ;;
    "")
        echo "Target: Current platform"
        echo ""
        build_current
        ;;
    *)
        echo "未知参数: $1"
        echo "使用 --help 查看帮助"
        exit 1
        ;;
esac

echo ""
echo "=========================================="
echo "Build completed!"
echo "=========================================="
ls -lh "${OUTPUT_DIR}/"
