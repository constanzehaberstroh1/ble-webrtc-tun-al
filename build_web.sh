#!/usr/bin/env bash
set -e

SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="$HOME/ble_build"
NODE_DIR="$HOME/node"

echo "[1] install node (no sudo)"
if [ ! -x "$NODE_DIR/bin/node" ]; then
  cd "$HOME"
  rm -f node.tar.xz
  echo "  downloading node v22.16.0..."
  curl -fsSL -o node.tar.xz https://nodejs.org/dist/v22.16.0/node-v22.16.0-linux-x64.tar.xz
  rm -rf "$NODE_DIR"
  mkdir -p "$NODE_DIR"
  tar -xf node.tar.xz -C "$NODE_DIR" --strip-components=1
  rm -f node.tar.xz
fi
export PATH="$NODE_DIR/bin:$PATH"
echo "  node: $(node -v)"
echo "  npm:  $(npm -v)"

echo "[2] prepare native work dir"
rm -rf "$WORK"
mkdir -p "$WORK/web"
rsync -a --exclude="node_modules" --exclude="dist" "$SRC/web/" "$WORK/web/"

echo "[3] npm install"
cd "$WORK/web"
npm install --no-fund --no-audit 2>&1 | tail -15

echo "[4] ensure esbuild native binary"
node node_modules/esbuild/install.js 2>&1 || npm rebuild esbuild 2>&1 || true
node -e "const e=require('esbuild'); e.build({entryPoints:[],bundle:true}).catch(()=>{}); console.log('esbuild loaded OK')"

echo "[5] vite build"
npm run build 2>&1 | tail -25

echo "[6] verify dist"
ls -la "$WORK/web/dist" | head

echo "[7] sync dist -> internal/webui/dist"
rm -rf "$SRC/internal/webui/dist"
mkdir -p "$SRC/internal/webui/dist"
cp -r "$WORK/web/dist/." "$SRC/internal/webui/dist/"
# Also sync to web/dist for Makefile compatibility
rm -rf "$SRC/web/dist"
mkdir -p "$SRC/web/dist"
cp -r "$WORK/web/dist/." "$SRC/web/dist/"
echo "  synced files:"
ls "$SRC/internal/webui/dist" | head

echo "[8] DONE"
