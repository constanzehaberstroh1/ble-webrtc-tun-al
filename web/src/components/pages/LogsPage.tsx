import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Button, Card, Typography, Tag, Space, Select, Input, Badge, Tooltip,
  Dropdown, Segmented, Switch, Statistic, Empty
} from 'antd';
import {
  CodeOutlined, DownloadOutlined, ReloadOutlined, ClearOutlined,
  PauseCircleOutlined, PlayCircleOutlined, FilterOutlined,
  FileTextOutlined, SearchOutlined, VerticalAlignBottomOutlined,
  ClockCircleOutlined, CloudDownloadOutlined
} from '@ant-design/icons';
import { api, createLogStream } from '../../api';
import { useTheme } from '../../ThemeContext';

const { Text, Title } = Typography;

interface LogEntry {
  timestamp: string;
  level: string;
  component: string;
  message: string;
}

interface LogFile {
  name: string;
  component: string;
  size: number;
  size_human: string;
}

const LEVEL_COLORS: Record<string, string> = {
  DEBUG: '#1890ff',
  INFO: '#52c41a',
  WARN: '#faad14',
  ERROR: '#f5222d',
  FATAL: '#a855f7',
};

const LEVEL_BG: Record<string, string> = {
  DEBUG: 'rgba(24,144,255,0.08)',
  INFO: 'rgba(82,196,26,0.08)',
  WARN: 'rgba(250,173,20,0.08)',
  ERROR: 'rgba(245,34,45,0.08)',
  FATAL: 'rgba(168,85,247,0.08)',
};

const COMPONENT_COLORS: Record<string, string> = {
  BALE: 'blue',
  SFU: 'purple',
  TUNNEL: 'cyan',
  MAIN: 'magenta',
  API: 'green',
  ROUTER: 'orange',
  DB: 'gold',
  MUX: 'lime',
  PROXY: 'volcano',
  POOL: 'geekblue',
  WEBRTC: 'purple',
  RTPCONN: 'cyan',
  SYNC: 'blue',
  ACCOUNTS: 'green',
  ADMIN: 'magenta',
  WEBUI: 'default',
  LONGPOLL: 'blue',
  BACKUP: 'gold',
};

export function LogsPage() {
  const { settings } = useTheme();
  const isDark = settings.mode === 'dark';

  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [paused, setPaused] = useState(false);
  const [autoScroll, setAutoScroll] = useState(true);
  const [connected, setConnected] = useState(false);
  const [search, setSearch] = useState('');
  const [levelFilter, setLevelFilter] = useState<string>('');
  const [componentFilter, setComponentFilter] = useState<string>('');
  const [logFiles, setLogFiles] = useState<LogFile[]>([]);
  const [logCount, setLogCount] = useState(0);
  const [viewMode, setViewMode] = useState<'live' | 'files'>('live');

  const containerRef = useRef<HTMLDivElement>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const pausedRef = useRef(false);
  const pauseBufferRef = useRef<LogEntry[]>([]);

  // Keep pausedRef in sync
  useEffect(() => {
    pausedRef.current = paused;
    if (!paused && pauseBufferRef.current.length > 0) {
      setLogs(prev => {
        const combined = [...prev, ...pauseBufferRef.current];
        return combined.slice(-5000);
      });
      pauseBufferRef.current = [];
    }
  }, [paused]);

  // Connect WebSocket
  const connectWS = useCallback(() => {
    if (wsRef.current) {
      wsRef.current.close();
    }

    const ws = createLogStream({
      level: levelFilter || undefined,
      component: componentFilter || undefined,
    });

    ws.onopen = () => setConnected(true);
    ws.onclose = () => {
      setConnected(false);
      // Auto-reconnect after 3s
      setTimeout(() => {
        if (viewMode === 'live') connectWS();
      }, 3000);
    };
    ws.onerror = () => ws.close();

    ws.onmessage = (e) => {
      try {
        const entry: LogEntry = JSON.parse(e.data);
        if (pausedRef.current) {
          pauseBufferRef.current.push(entry);
          // Cap pause buffer
          if (pauseBufferRef.current.length > 2000) {
            pauseBufferRef.current = pauseBufferRef.current.slice(-1000);
          }
        } else {
          setLogs(prev => {
            const next = [...prev, entry];
            return next.length > 5000 ? next.slice(-4000) : next;
          });
          setLogCount(c => c + 1);
        }
      } catch {}
    };

    wsRef.current = ws;
  }, [levelFilter, componentFilter, viewMode]);

  useEffect(() => {
    if (viewMode === 'live') {
      connectWS();
    }
    return () => {
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [connectWS, viewMode]);

  // Auto scroll
  useEffect(() => {
    if (autoScroll && containerRef.current && !paused) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [logs, autoScroll, paused]);

  // Load log files
  useEffect(() => {
    if (viewMode === 'files') {
      api.getLogFiles().then((res: any) => {
        if (res?.files) setLogFiles(res.files);
      }).catch(() => {});
    }
  }, [viewMode]);

  const handleScroll = () => {
    if (containerRef.current) {
      const { scrollTop, scrollHeight, clientHeight } = containerRef.current;
      const isBottom = Math.abs(scrollHeight - clientHeight - scrollTop) < 15;
      setAutoScroll(isBottom);
    }
  };

  const clearLogs = () => {
    setLogs([]);
    setLogCount(0);
  };

  const scrollToBottom = () => {
    if (containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
      setAutoScroll(true);
    }
  };

  // Extract unique components from current logs
  const components = Array.from(new Set(logs.map(l => l.component))).sort();

  // Filter logs by search
  const filteredLogs = search
    ? logs.filter(l =>
        l.message.toLowerCase().includes(search.toLowerCase()) ||
        l.component.toLowerCase().includes(search.toLowerCase())
      )
    : logs;

  const downloadFile = (component: string) => {
    const url = api.getLogDownloadURL(component);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${component}.log`;
    a.click();
  };

  const bgPrimary = isDark ? '#0a0e1a' : '#f0f2f5';
  const bgCard = isDark ? '#141821' : '#ffffff';
  const bgTerminal = isDark ? '#0d1117' : '#fafbfc';
  const borderColor = isDark ? '#1e2740' : '#e8e8e8';
  const textPrimary = isDark ? '#e2e8f0' : '#1a202c';
  const textSecondary = isDark ? '#8892b0' : '#718096';
  const textMuted = isDark ? '#4a5568' : '#a0aec0';

  return (
    <div style={{ maxWidth: 1400, margin: '0 auto' }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
          <Space align="center" size={12}>
            <div style={{
              width: 40, height: 40, borderRadius: 12,
              background: 'linear-gradient(135deg, #10b981, #06b6d4)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              boxShadow: '0 4px 16px rgba(16,185,129,0.3)',
            }}>
              <CodeOutlined style={{ color: '#fff', fontSize: 20 }} />
            </div>
            <div>
              <Title level={3} style={{ margin: 0, color: textPrimary }}>System Logs</Title>
              <Text style={{ color: textSecondary, fontSize: 13 }}>
                Real-time log streaming & file management
              </Text>
            </div>
          </Space>
          <Space>
            {viewMode === 'live' && (
              <Badge
                status={connected ? 'processing' : 'error'}
                text={
                  <Text style={{ color: connected ? '#52c41a' : '#f5222d', fontSize: 12 }}>
                    {connected ? 'Connected' : 'Disconnected'}
                  </Text>
                }
              />
            )}
            <Segmented
              value={viewMode}
              onChange={(v) => setViewMode(v as 'live' | 'files')}
              options={[
                { label: '🔴 Live', value: 'live' },
                { label: '📁 Files', value: 'files' },
              ]}
              style={{
                background: isDark ? '#1a1f35' : '#f0f0f0',
              }}
            />
          </Space>
        </div>
      </div>

      {viewMode === 'live' ? (
        <>
          {/* Controls Bar */}
          <Card
            size="small"
            style={{
              marginBottom: 12,
              background: bgCard,
              borderColor,
              borderRadius: 12,
            }}
            styles={{ body: { padding: '10px 16px' } }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
              <Input
                prefix={<SearchOutlined style={{ color: textMuted }} />}
                placeholder="Search logs..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                allowClear
                style={{
                  width: 240,
                  background: isDark ? '#0d1117' : '#f5f5f5',
                  borderColor: isDark ? '#2a3050' : '#d9d9d9',
                }}
              />

              <Select
                placeholder="Level"
                value={levelFilter || undefined}
                onChange={(v) => setLevelFilter(v || '')}
                allowClear
                style={{ width: 120 }}
                options={[
                  { label: '🔵 DEBUG', value: 'DEBUG' },
                  { label: '🟢 INFO', value: 'INFO' },
                  { label: '🟡 WARN', value: 'WARN' },
                  { label: '🔴 ERROR', value: 'ERROR' },
                  { label: '🟣 FATAL', value: 'FATAL' },
                ]}
              />

              <Select
                placeholder="Component"
                value={componentFilter || undefined}
                onChange={(v) => setComponentFilter(v || '')}
                allowClear
                style={{ width: 150 }}
                options={components.map(c => ({ label: c, value: c }))}
              />

              <div style={{ flex: 1 }} />

              <Space size={6}>
                <Tooltip title={paused ? 'Resume streaming' : 'Pause streaming'}>
                  <Button
                    type={paused ? 'primary' : 'default'}
                    icon={paused ? <PlayCircleOutlined /> : <PauseCircleOutlined />}
                    onClick={() => setPaused(!paused)}
                    size="small"
                    danger={paused}
                  />
                </Tooltip>
                <Tooltip title="Scroll to bottom">
                  <Button
                    icon={<VerticalAlignBottomOutlined />}
                    onClick={scrollToBottom}
                    size="small"
                    disabled={autoScroll}
                  />
                </Tooltip>
                <Tooltip title="Clear logs">
                  <Button
                    icon={<ClearOutlined />}
                    onClick={clearLogs}
                    size="small"
                  />
                </Tooltip>
                <Tooltip title="Reconnect WebSocket">
                  <Button
                    icon={<ReloadOutlined />}
                    onClick={connectWS}
                    size="small"
                  />
                </Tooltip>
              </Space>

              <Tag color={isDark ? '#1a1f35' : '#f0f0f0'} style={{ marginLeft: 4 }}>
                <span style={{ color: textSecondary, fontFamily: 'monospace', fontSize: 11 }}>
                  {filteredLogs.length} entries
                  {paused && ` (${pauseBufferRef.current.length} buffered)`}
                </span>
              </Tag>
            </div>
          </Card>

          {/* Log Viewer */}
          <Card
            style={{
              background: bgTerminal,
              borderColor,
              borderRadius: 12,
              overflow: 'hidden',
            }}
            styles={{ body: { padding: 0 } }}
          >
            {/* Status bar */}
            <div style={{
              display: 'flex', alignItems: 'center', justifyContent: 'space-between',
              padding: '8px 16px',
              borderBottom: `1px solid ${borderColor}`,
              background: isDark ? '#0a0f1e' : '#f5f5f5',
            }}>
              <Space size={8}>
                <div style={{
                  width: 8, height: 8, borderRadius: '50%',
                  background: connected ? (paused ? '#faad14' : '#52c41a') : '#f5222d',
                  boxShadow: connected
                    ? (paused ? '0 0 8px rgba(250,173,20,0.5)' : '0 0 8px rgba(82,196,26,0.5)')
                    : '0 0 8px rgba(245,34,45,0.5)',
                  animation: connected && !paused ? 'pulse 2s infinite' : 'none',
                }} />
                <Text style={{ color: textSecondary, fontSize: 12, fontFamily: 'monospace' }}>
                  {connected ? (paused ? 'PAUSED' : 'STREAMING') : 'DISCONNECTED'}
                </Text>
              </Space>
              <Space size={16}>
                <Text style={{ color: textMuted, fontSize: 11, fontFamily: 'monospace' }}>
                  <ClockCircleOutlined style={{ marginRight: 4 }} />
                  {new Date().toLocaleTimeString()}
                </Text>
                {!autoScroll && (
                  <Tag color="warning" style={{ fontSize: 10, lineHeight: '16px', height: 18 }}>
                    Auto-scroll paused
                  </Tag>
                )}
              </Space>
            </div>

            {/* Logs content */}
            <div
              ref={containerRef}
              onScroll={handleScroll}
              style={{
                height: 'calc(100vh - 340px)',
                minHeight: 400,
                overflowY: 'auto',
                padding: '8px 12px',
                fontFamily: "'SFMono-Regular', 'Menlo', 'Consolas', 'DejaVu Sans Mono', monospace",
                fontSize: 12,
                lineHeight: 1.65,
                background: bgTerminal,
              }}
            >
              {filteredLogs.length === 0 ? (
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
                  <Empty
                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                    description={
                      <Text style={{ color: textMuted }}>
                        {connected ? 'Waiting for log entries...' : 'Not connected to log stream'}
                      </Text>
                    }
                  />
                </div>
              ) : (
                filteredLogs.map((log, i) => (
                  <div
                    key={i}
                    style={{
                      display: 'flex',
                      alignItems: 'flex-start',
                      padding: '2px 6px',
                      borderRadius: 4,
                      background: log.level === 'ERROR' || log.level === 'FATAL'
                        ? (LEVEL_BG[log.level] || 'transparent')
                        : 'transparent',
                      transition: 'background 0.15s',
                    }}
                    onMouseEnter={(e) => {
                      if (log.level !== 'ERROR' && log.level !== 'FATAL') {
                        (e.currentTarget as HTMLDivElement).style.background = isDark ? 'rgba(255,255,255,0.03)' : 'rgba(0,0,0,0.02)';
                      }
                    }}
                    onMouseLeave={(e) => {
                      if (log.level !== 'ERROR' && log.level !== 'FATAL') {
                        (e.currentTarget as HTMLDivElement).style.background = 'transparent';
                      }
                    }}
                  >
                    <span style={{ color: textMuted, marginRight: 10, whiteSpace: 'nowrap', userSelect: 'none' }}>
                      {log.timestamp.split(' ')[1] || log.timestamp}
                    </span>
                    <span style={{
                      color: LEVEL_COLORS[log.level] || '#999',
                      fontWeight: 700,
                      width: 50,
                      flexShrink: 0,
                    }}>
                      {log.level}
                    </span>
                    <Tag
                      color={COMPONENT_COLORS[log.component] || 'default'}
                      style={{
                        marginRight: 8, border: 0, opacity: 0.85,
                        fontSize: 10, lineHeight: '14px', height: 16,
                        flexShrink: 0,
                      }}
                    >
                      {log.component}
                    </Tag>
                    <span style={{ color: isDark ? '#c9d1d9' : '#24292f', wordBreak: 'break-all' }}>
                      {log.message}
                    </span>
                  </div>
                ))
              )}
            </div>
          </Card>
        </>
      ) : (
        /* Files View */
        <Card
          style={{
            background: bgCard,
            borderColor,
            borderRadius: 12,
          }}
          styles={{ body: { padding: 0 } }}
        >
          <div style={{
            padding: '16px 20px',
            borderBottom: `1px solid ${borderColor}`,
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          }}>
            <Space>
              <FileTextOutlined style={{ color: '#1890ff', fontSize: 16 }} />
              <Text strong style={{ color: textPrimary }}>Log Files</Text>
              <Tag color="blue">{logFiles.length} files</Tag>
            </Space>
            <Button
              icon={<ReloadOutlined />}
              size="small"
              onClick={() => api.getLogFiles().then((r: any) => r?.files && setLogFiles(r.files))}
            >
              Refresh
            </Button>
          </div>

          {logFiles.length === 0 ? (
            <div style={{ padding: 40 }}>
              <Empty description="No log files found" />
            </div>
          ) : (
            <div style={{ padding: 8 }}>
              {logFiles.map((file) => (
                <div
                  key={file.name}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    padding: '12px 16px',
                    borderRadius: 8,
                    margin: '4px 0',
                    transition: 'all 0.2s',
                    cursor: 'pointer',
                    background: 'transparent',
                    borderBottom: `1px solid ${isDark ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.04)'}`,
                  }}
                  onMouseEnter={(e) => {
                    (e.currentTarget as HTMLDivElement).style.background = isDark ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.02)';
                  }}
                  onMouseLeave={(e) => {
                    (e.currentTarget as HTMLDivElement).style.background = 'transparent';
                  }}
                >
                  <Space size={12}>
                    <div style={{
                      width: 36, height: 36, borderRadius: 10,
                      background: isDark ? 'rgba(24,144,255,0.1)' : 'rgba(24,144,255,0.06)',
                      display: 'flex', alignItems: 'center', justifyContent: 'center',
                    }}>
                      <FileTextOutlined style={{ color: '#1890ff', fontSize: 16 }} />
                    </div>
                    <div>
                      <Text strong style={{ color: textPrimary, fontSize: 14, display: 'block' }}>
                        {file.name}
                      </Text>
                      <Text style={{ color: textMuted, fontSize: 12 }}>
                        {file.component} component
                      </Text>
                    </div>
                  </Space>
                  <Space size={16}>
                    <Tag style={{
                      fontFamily: 'monospace', fontSize: 11,
                      background: isDark ? '#1a1f35' : '#f5f5f5',
                      border: 'none',
                      color: textSecondary,
                    }}>
                      {file.size_human}
                    </Tag>
                    <Tooltip title="Download log file">
                      <Button
                        type="primary"
                        ghost
                        icon={<CloudDownloadOutlined />}
                        size="small"
                        onClick={() => downloadFile(file.component)}
                      >
                        Download
                      </Button>
                    </Tooltip>
                  </Space>
                </div>
              ))}
            </div>
          )}
        </Card>
      )}

      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.4; }
        }
      `}</style>
    </div>
  );
}
