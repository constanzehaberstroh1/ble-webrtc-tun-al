import React, { useEffect, useRef, useState, useCallback } from 'react';
import { Button, Tag, Space, Tooltip, Input, Typography, Badge } from 'antd';
import {
  CodeOutlined,
  ExpandAltOutlined,
  ShrinkOutlined,
  SearchOutlined,
  CloseCircleOutlined,
  ReloadOutlined,
  CloudServerOutlined,
  DesktopOutlined,
  DisconnectOutlined,
  CheckCircleFilled,
  LoadingOutlined,
} from '@ant-design/icons';
import { Terminal } from '@xterm/xterm';
import { FitAddon } from '@xterm/addon-fit';
import { WebLinksAddon } from '@xterm/addon-web-links';
import { SearchAddon } from '@xterm/addon-search';
import { Unicode11Addon } from '@xterm/addon-unicode11';
import '@xterm/xterm/css/xterm.css';
import { useTheme } from '../../ThemeContext';
import { api } from '../../api';

const { Text } = Typography;

// Dark theme color palette
const DARK_THEME = {
  background: '#0a0e17',
  foreground: '#e2e8f0',
  cursor: '#7c3aed',
  cursorAccent: '#0a0e17',
  selectionBackground: 'rgba(124, 58, 237, 0.3)',
  selectionForeground: '#ffffff',
  black: '#1e293b',
  red: '#f87171',
  green: '#4ade80',
  yellow: '#fbbf24',
  blue: '#60a5fa',
  magenta: '#c084fc',
  cyan: '#22d3ee',
  white: '#e2e8f0',
  brightBlack: '#475569',
  brightRed: '#fca5a5',
  brightGreen: '#86efac',
  brightYellow: '#fde68a',
  brightBlue: '#93c5fd',
  brightMagenta: '#d8b4fe',
  brightCyan: '#67e8f9',
  brightWhite: '#f8fafc',
};

// Light theme color palette
const LIGHT_THEME = {
  background: '#f8fafc',
  foreground: '#1e293b',
  cursor: '#7c3aed',
  cursorAccent: '#f8fafc',
  selectionBackground: 'rgba(124, 58, 237, 0.15)',
  selectionForeground: '#0f172a',
  black: '#0f172a',
  red: '#dc2626',
  green: '#16a34a',
  yellow: '#ca8a04',
  blue: '#2563eb',
  magenta: '#9333ea',
  cyan: '#0891b2',
  white: '#e2e8f0',
  brightBlack: '#94a3b8',
  brightRed: '#ef4444',
  brightGreen: '#22c55e',
  brightYellow: '#eab308',
  brightBlue: '#3b82f6',
  brightMagenta: '#a855f7',
  brightCyan: '#06b6d4',
  brightWhite: '#f8fafc',
};

type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'error';
type TerminalMode = 'direct' | 'vpn_proxy' | 'unknown';

interface TerminalInfo {
  role: string;
  mode: TerminalMode;
  status: string;
}

export function TerminalPage() {
  const { settings } = useTheme();
  const terminalRef = useRef<HTMLDivElement>(null);
  const termInstanceRef = useRef<Terminal | null>(null);
  const fitAddonRef = useRef<FitAddon | null>(null);
  const searchAddonRef = useRef<SearchAddon | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<any>(null);
  const dataListenerRef = useRef<any>(null);
  const binaryListenerRef = useRef<any>(null);
  const resizeListenerRef = useRef<any>(null);

  const [connState, setConnState] = useState<ConnectionState>('disconnected');
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [showSearch, setShowSearch] = useState(false);
  const [searchText, setSearchText] = useState('');
  const [termInfo, setTermInfo] = useState<TerminalInfo | null>(null);

  const isDark = settings.mode === 'dark';

  // Fetch terminal info on mount
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const info = await api.terminalInfo();
        if (!cancelled) setTermInfo(info);
      } catch {
        if (!cancelled) setTermInfo({ role: 'unknown', mode: 'unknown', status: 'ready' });
      }
    })();
    return () => { cancelled = true; };
  }, []);

  // Initialize terminal
  useEffect(() => {
    if (!terminalRef.current) return;

    const term = new Terminal({
      fontFamily: "'JetBrains Mono', 'Fira Code', 'Cascadia Code', 'SF Mono', Menlo, monospace",
      fontSize: 14,
      lineHeight: 1.35,
      letterSpacing: 0.5,
      cursorBlink: true,
      cursorStyle: 'bar' as const,
      cursorWidth: 2,
      scrollback: 10000,
      allowTransparency: true,
      allowProposedApi: true,
      theme: isDark ? DARK_THEME : LIGHT_THEME,
      convertEol: true,
    });

    const fitAddon = new FitAddon();
    const searchAddon = new SearchAddon();
    const webLinksAddon = new WebLinksAddon();
    const unicode11Addon = new Unicode11Addon();

    term.loadAddon(fitAddon);
    term.loadAddon(searchAddon);
    term.loadAddon(webLinksAddon);
    term.loadAddon(unicode11Addon);

    try { term.unicode.activeVersion = '11'; } catch {}

    term.open(terminalRef.current);

    // Delay fit to ensure container is fully rendered
    requestAnimationFrame(() => {
      try { fitAddon.fit(); } catch {}
    });

    termInstanceRef.current = term;
    fitAddonRef.current = fitAddon;
    searchAddonRef.current = searchAddon;

    // Handle resize
    const el = terminalRef.current;
    const resizeObserver = new ResizeObserver(() => {
      requestAnimationFrame(() => {
        try { fitAddon.fit(); } catch {}
      });
    });
    resizeObserver.observe(el);

    // Welcome banner
    term.writeln('');
    term.writeln('  \x1b[1;38;5;141m╔══════════════════════════════════════╗\x1b[0m');
    term.writeln('  \x1b[1;38;5;141m║\x1b[0m    \x1b[1;38;5;81m⚡ BLE Tunnel Terminal\x1b[0m            \x1b[1;38;5;141m║\x1b[0m');
    term.writeln('  \x1b[1;38;5;141m╚══════════════════════════════════════╝\x1b[0m');
    term.writeln('');
    term.writeln('  \x1b[90mInitializing connection...\x1b[0m');
    term.writeln('');

    return () => {
      resizeObserver.disconnect();
      // Dispose listeners
      if (dataListenerRef.current) { dataListenerRef.current.dispose(); dataListenerRef.current = null; }
      if (binaryListenerRef.current) { binaryListenerRef.current.dispose(); binaryListenerRef.current = null; }
      if (resizeListenerRef.current) { resizeListenerRef.current.dispose(); resizeListenerRef.current = null; }
      term.dispose();
      termInstanceRef.current = null;
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Update theme when mode changes
  useEffect(() => {
    if (termInstanceRef.current) {
      termInstanceRef.current.options.theme = isDark ? DARK_THEME : LIGHT_THEME;
    }
  }, [isDark]);

  // Connect WebSocket
  const connect = useCallback(() => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      return;
    }

    const term = termInstanceRef.current;
    if (!term) return;

    setConnState('connecting');

    // Build WebSocket URL with auth as query parameter (WebSocket subprotocols can't have spaces)
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const authHeader = sessionStorage.getItem('auth') || '';
    const encodedAuth = encodeURIComponent(authHeader);
    const wsUrl = `${proto}//${window.location.host}/api/terminal/ws?auth=${encodedAuth}`;

    try {
      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;
      ws.binaryType = 'arraybuffer';

      ws.onopen = () => {
        setConnState('connected');
        term.writeln('\x1b[32m  ✓ Connected\x1b[0m');
        term.writeln('');

        // Send initial resize
        try {
          const dims = fitAddonRef.current?.proposeDimensions();
          if (dims) {
            ws.send(JSON.stringify({ type: 'resize', cols: dims.cols, rows: dims.rows }));
          }
        } catch {}

        // Clear reconnect timer
        if (reconnectTimerRef.current) {
          clearTimeout(reconnectTimerRef.current);
          reconnectTimerRef.current = null;
        }
      };

      ws.onmessage = (event) => {
        try {
          if (event.data instanceof ArrayBuffer) {
            term.write(new Uint8Array(event.data));
          } else {
            term.write(event.data);
          }
        } catch {}
      };

      ws.onclose = (event) => {
        setConnState('disconnected');
        if (!event.wasClean) {
          term.writeln('');
          term.writeln('\x1b[31m  ✗ Connection lost\x1b[0m');
          term.writeln('\x1b[90m  Reconnecting in 3s...\x1b[0m');
          reconnectTimerRef.current = setTimeout(connect, 3000);
        }
      };

      ws.onerror = () => {
        setConnState('error');
      };

      // Dispose old listeners before adding new ones
      if (dataListenerRef.current) { dataListenerRef.current.dispose(); }
      if (binaryListenerRef.current) { binaryListenerRef.current.dispose(); }
      if (resizeListenerRef.current) { resizeListenerRef.current.dispose(); }

      // Terminal data → WebSocket
      dataListenerRef.current = term.onData((data: string) => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(data);
        }
      });

      // Terminal binary → WebSocket
      binaryListenerRef.current = term.onBinary((data: string) => {
        if (ws.readyState === WebSocket.OPEN) {
          const buf = new Uint8Array(data.length);
          for (let i = 0; i < data.length; i++) {
            buf[i] = data.charCodeAt(i) & 255;
          }
          ws.send(buf.buffer);
        }
      });

      // Terminal resize → WebSocket
      resizeListenerRef.current = term.onResize(({ cols, rows }: { cols: number; rows: number }) => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'resize', cols, rows }));
        }
      });

    } catch (err: any) {
      setConnState('error');
      term.writeln(`\x1b[31m  ✗ Failed to connect: ${err?.message || err}\x1b[0m`);
    }
  }, []);

  // Auto-connect on mount
  useEffect(() => {
    const timer = setTimeout(connect, 500);
    return () => {
      clearTimeout(timer);
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
      }
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [connect]);

  // Refit on fullscreen toggle
  useEffect(() => {
    const t = setTimeout(() => {
      try { fitAddonRef.current?.fit(); } catch {}
    }, 300);
    return () => clearTimeout(t);
  }, [isFullscreen]);

  // Keyboard shortcuts
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key === 'F') {
        e.preventDefault();
        setShowSearch(prev => !prev);
      }
      if (e.key === 'Escape' && showSearch) {
        setShowSearch(false);
      }
      if (e.key === 'F11') {
        e.preventDefault();
        setIsFullscreen(prev => !prev);
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [showSearch]);

  const handleDisconnect = () => {
    if (wsRef.current) {
      wsRef.current.close(1000, 'user_disconnect');
      wsRef.current = null;
    }
    if (reconnectTimerRef.current) {
      clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
    setConnState('disconnected');
    termInstanceRef.current?.writeln('\r\n\x1b[33m  ⏻ Disconnected by user\x1b[0m\r\n');
  };

  const handleReconnect = () => {
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }
    termInstanceRef.current?.clear();
    termInstanceRef.current?.writeln('\x1b[90m  Reconnecting...\x1b[0m');
    setTimeout(connect, 200);
  };

  const handleSearch = () => {
    if (searchAddonRef.current && searchText) {
      try {
        searchAddonRef.current.findNext(searchText);
      } catch {}
    }
  };

  const getStatusInfo = () => {
    switch (connState) {
      case 'connected': return { color: '#4ade80', icon: <CheckCircleFilled />, text: 'Connected' };
      case 'connecting': return { color: '#fbbf24', icon: <LoadingOutlined spin />, text: 'Connecting...' };
      case 'error': return { color: '#f87171', icon: <DisconnectOutlined />, text: 'Error' };
      default: return { color: '#94a3b8', icon: <DisconnectOutlined />, text: 'Disconnected' };
    }
  };

  const getModeInfo = () => {
    if (!termInfo) return { label: 'Terminal', detail: '' };
    if (termInfo.mode === 'vpn_proxy') {
      return { label: 'Remote Shell', detail: 'via VPN tunnel' };
    }
    return { label: 'Local Shell', detail: 'direct PTY' };
  };

  const status = getStatusInfo();
  const modeInfo = getModeInfo();

  return (
    <div
      className={`transition-all duration-300 ${isFullscreen ? 'fixed inset-0 z-50' : ''}`}
      style={isFullscreen ? { padding: 0 } : {}}
    >
      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center justify-between mb-2">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl flex items-center justify-center"
              style={{
                background: 'linear-gradient(135deg, #7c3aed, #6366f1)',
                boxShadow: '0 4px 16px rgba(124, 58, 237, 0.3)',
              }}
            >
              <CodeOutlined style={{ fontSize: 20, color: 'white' }} />
            </div>
            <div>
              <h2 className="text-xl font-bold m-0" style={{ lineHeight: 1.2 }}>
                Web Terminal
              </h2>
              <Text className="text-xs opacity-60">
                {modeInfo.label}
                {modeInfo.detail && <span className="ml-1">• {modeInfo.detail}</span>}
              </Text>
            </div>
          </div>

          <Space size="small">
            <Tooltip title="Search (Ctrl+Shift+F)">
              <Button
                type="text"
                size="small"
                icon={<SearchOutlined />}
                onClick={() => setShowSearch(!showSearch)}
                className="opacity-60 hover:opacity-100"
              />
            </Tooltip>
            <Tooltip title={isFullscreen ? 'Exit Fullscreen (F11)' : 'Fullscreen (F11)'}>
              <Button
                type="text"
                size="small"
                icon={isFullscreen ? <ShrinkOutlined /> : <ExpandAltOutlined />}
                onClick={() => setIsFullscreen(!isFullscreen)}
                className="opacity-60 hover:opacity-100"
              />
            </Tooltip>
            {connState === 'connected' ? (
              <Button
                size="small"
                danger
                icon={<DisconnectOutlined />}
                onClick={handleDisconnect}
              >
                Disconnect
              </Button>
            ) : (
              <Button
                size="small"
                type="primary"
                icon={<ReloadOutlined />}
                onClick={handleReconnect}
                loading={connState === 'connecting'}
                style={{
                  background: 'linear-gradient(135deg, #7c3aed, #6366f1)',
                  border: 'none',
                }}
              >
                {connState === 'connecting' ? 'Connecting...' : 'Connect'}
              </Button>
            )}
          </Space>
        </div>

        {/* Search bar */}
        {showSearch && (
          <div
            className="mt-2 flex items-center gap-2 px-3 py-2 rounded-lg"
            style={{
              background: isDark ? 'rgba(30, 41, 59, 0.8)' : 'rgba(241, 245, 249, 0.9)',
              border: `1px solid ${isDark ? 'rgba(124, 58, 237, 0.2)' : 'rgba(0, 0, 0, 0.1)'}`,
            }}
          >
            <SearchOutlined className="opacity-50" />
            <Input
              size="small"
              placeholder="Search terminal output..."
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              onPressEnter={handleSearch}
              className="flex-1 border-0 bg-transparent"
              style={{ boxShadow: 'none' }}
            />
            <Button size="small" type="text" onClick={handleSearch}>Find</Button>
            <Button size="small" type="text" icon={<CloseCircleOutlined />} onClick={() => setShowSearch(false)} />
          </div>
        )}
      </div>

      {/* Terminal container */}
      <div
        className="overflow-hidden transition-all duration-300"
        style={{
          background: isDark ? '#0a0e17' : '#f8fafc',
          border: `1px solid ${isDark ? 'rgba(124, 58, 237, 0.15)' : 'rgba(0, 0, 0, 0.08)'}`,
          borderRadius: 16,
          boxShadow: isDark
            ? '0 8px 32px rgba(0, 0, 0, 0.4), 0 0 1px rgba(124, 58, 237, 0.2), inset 0 1px 0 rgba(255, 255, 255, 0.03)'
            : '0 4px 24px rgba(0, 0, 0, 0.06), 0 1px 3px rgba(0, 0, 0, 0.04)',
          height: isFullscreen ? '100vh' : 'calc(100vh - 260px)',
          minHeight: 400,
          display: 'flex',
          flexDirection: 'column' as const,
        }}
      >
        {/* Terminal toolbar */}
        <div
          className="flex items-center justify-between px-4 py-2"
          style={{
            borderBottom: `1px solid ${isDark ? 'rgba(124, 58, 237, 0.1)' : 'rgba(0, 0, 0, 0.05)'}`,
            background: isDark ? 'rgba(15, 23, 42, 0.6)' : 'rgba(241, 245, 249, 0.8)',
            backdropFilter: 'blur(8px)',
            flexShrink: 0,
          }}
        >
          <div className="flex items-center gap-3">
            {/* macOS-style traffic lights */}
            <div className="flex gap-1.5">
              <div className="w-3 h-3 rounded-full" style={{ background: connState === 'connected' ? '#ff5f57' : '#3a3a3c' }} />
              <div className="w-3 h-3 rounded-full" style={{ background: connState === 'connected' ? '#febc2e' : '#3a3a3c' }} />
              <div className="w-3 h-3 rounded-full" style={{ background: connState === 'connected' ? '#28c840' : '#3a3a3c' }} />
            </div>

            <div className="flex items-center gap-2">
              <Badge
                status={connState === 'connected' ? 'success' : connState === 'connecting' ? 'processing' : 'default'}
              />
              <span
                className="font-mono text-xs font-medium"
                style={{ color: status.color }}
              >
                {status.text}
              </span>
            </div>
          </div>

          <div className="flex items-center gap-2">
            {termInfo && (
              <Tag
                className="border-0 font-mono text-xs"
                style={{
                  background: isDark ? 'rgba(124, 58, 237, 0.12)' : 'rgba(124, 58, 237, 0.08)',
                  color: isDark ? '#c084fc' : '#7c3aed',
                }}
              >
                {termInfo.role === 'server' ? '🖥 Server' : '💻 Client'} • {termInfo.mode === 'vpn_proxy' ? 'VPN Proxy' : 'Direct'}
              </Tag>
            )}
          </div>
        </div>

        {/* xterm.js container */}
        <div
          ref={terminalRef}
          style={{
            flex: 1,
            padding: '8px 8px 8px 12px',
            overflow: 'hidden',
          }}
        />
      </div>
    </div>
  );
}
