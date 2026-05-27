import React, { useState, useEffect, useCallback } from 'react';
import { Card, Row, Col, Statistic, Table, Typography, Spin, Tag, Space, Alert, Progress, message, Badge } from 'antd';
import {
  FireOutlined,
  DesktopOutlined,
  MobileOutlined,
  ClockCircleOutlined,
  CloudUploadOutlined,
  CloudDownloadOutlined,
  CheckCircleOutlined,
  LoadingOutlined,
  CloseCircleOutlined,
  ApiOutlined,
  ThunderboltOutlined,
  PhoneOutlined,
} from '@ant-design/icons';
import { api } from '../../api';
import { useTheme } from '../../ThemeContext';

const { Title, Text } = Typography;

const PHASE_LABELS: Record<string, { label: string; color: string; icon: React.ReactNode }> = {
  INITIALIZING:        { label: 'Initializing',       color: 'default',    icon: <LoadingOutlined spin /> },
  CONNECTING_TO_BALE:  { label: 'Connecting to Bale',  color: 'processing', icon: <LoadingOutlined spin /> },
  CALLING_SERVER:      { label: 'Calling Server',      color: 'processing', icon: <LoadingOutlined spin /> },
  WAITING_FOR_ACCEPT:  { label: 'Waiting for Accept',  color: 'warning',    icon: <LoadingOutlined spin /> },
  CONNECTING_TO_SFU:   { label: 'Connecting to SFU',   color: 'processing', icon: <LoadingOutlined spin /> },
  WAITING_FOR_TRACK:   { label: 'Waiting for Track',   color: 'warning',    icon: <LoadingOutlined spin /> },
  SETTING_UP_TUNNEL:   { label: 'Setting up Tunnel',   color: 'processing', icon: <LoadingOutlined spin /> },
  TUNNEL_ACTIVE:       { label: 'Active',              color: 'success',    icon: <CheckCircleOutlined /> },
  DISCONNECTED:        { label: 'Disconnected',        color: 'default',    icon: <CloseCircleOutlined /> },
  ERROR:               { label: 'Error',               color: 'error',      icon: <CloseCircleOutlined /> },
};

export function DashboardPage() {
  const [stats, setStats] = useState<any>(null);
  const [tunnel, setTunnel] = useState<any>(null);
  const [tunnelLoading, setTunnelLoading] = useState(false);
  const [tunnelError, setTunnelError] = useState<string | null>(null);
  const [forceEndState, setForceEndState] = useState<'idle' | 'sending' | 'success' | 'error'>('idle');
  const [forceEndResult, setForceEndResult] = useState<any>(null);
  const { settings } = useTheme();

  const load = useCallback(async () => {
    try {
      const s = await api.getStats();
      setStats(s);
      try {
        const ts = await api.tunnelStatus();
        setTunnel(ts);
      } catch { /* server mode */ }
    } catch (e) {
      console.error(e);
    }
  }, []);

  useEffect(() => {
    load();
    // Fast polling when connecting (every 1s), normal otherwise (3s)
    const interval = setInterval(load, tunnel?.active && tunnel?.phase === 'CONNECTING' ? 1000 : 3000);
    return () => clearInterval(interval);
  }, [load, tunnel?.active, tunnel?.phase]);

  if (!stats) {
    return (
      <div className="flex items-center justify-center h-64">
        <Spin size="large" />
      </div>
    );
  }

  const fmt = (b: number) => {
    if (!b) return '0 B';
    const u = ['B', 'KB', 'MB', 'GB', 'TB'];
    let i = 0, v = b;
    while (v >= 1024 && i < 4) { v /= 1024; i++; }
    return v.toFixed(i > 0 ? 1 : 0) + ' ' + u[i];
  };

  const tunnelActive = tunnel?.active === true;
  const isConnecting = tunnelActive && tunnel?.phase === 'CONNECTING';
  const isConnected = tunnelActive && tunnel?.phase === 'CONNECTED';

  const toggleTunnel = async () => {
    setTunnelLoading(true);
    setTunnelError(null);
    try {
      if (tunnelActive) {
        await api.tunnelStop();
        message.success('Tunnel disconnected');
      } else {
        await api.tunnelStart();
        message.success('Tunnel connecting...');
      }
      load();
    } catch (e: any) {
      setTunnelError(e.message || 'Failed to toggle tunnel');
      message.error(e.message || 'Failed');
    } finally {
      setTunnelLoading(false);
    }
  };

  const handleForceEndCall = async () => {
    setForceEndState('sending');
    setForceEndResult(null);
    try {
      const result = await api.tunnelForceEndCall();
      setForceEndResult(result);
      if (result.success > 0) {
        setForceEndState('success');
        message.success(`Force end call: ${result.success}/${result.total} channels ended successfully`);
      } else {
        setForceEndState('error');
        message.warning('Force end call: no channels responded with ACK');
      }
      // Reset to idle after 5 seconds
      setTimeout(() => {
        setForceEndState('idle');
        setForceEndResult(null);
      }, 5000);
      load();
    } catch (e: any) {
      setForceEndState('error');
      setForceEndResult({ error: e.message });
      message.error(e.message || 'Force end call failed');
      setTimeout(() => {
        setForceEndState('idle');
        setForceEndResult(null);
      }, 5000);
    }
  };

  const channelColumns = [
    {
      title: 'Channel',
      key: 'label',
      render: (_: any, r: any) => <Text strong>{r.label}</Text>,
    },
    {
      title: 'Server (Bale ID)',
      key: 'server',
      render: (_: any, r: any) => <Tag color="blue" className="font-mono">{r.server_bale_id}</Tag>,
    },
    {
      title: 'Phase',
      key: 'phase',
      render: (_: any, r: any) => {
        const p = PHASE_LABELS[r.phase] || { label: r.phase, color: 'default', icon: null };
        return (
          <Space>
            {p.icon}
            <Tag color={p.color}>{p.label}</Tag>
          </Space>
        );
      },
    },
    {
      title: 'Traffic',
      key: 'traffic',
      render: (_: any, r: any) => (
        <Space size="small">
          <Text type="success" className="font-mono text-xs">↑{fmt(r.bytes_sent)}</Text>
          <Text type="secondary" className="font-mono text-xs">↓{fmt(r.bytes_received)}</Text>
        </Space>
      ),
    },
    {
      title: 'Error',
      key: 'error',
      render: (_: any, r: any) => r.error ? <Text type="danger" style={{ fontSize: 12 }}>{r.error}</Text> : <Text type="secondary">—</Text>,
    },
  ];

  return (
    <div className="space-y-6">
      <div className="mb-8 flex justify-between items-center">
        <div>
          <Title level={2} style={{ margin: 0 }}>Dashboard</Title>
          <Text type="secondary">Real-time system overview</Text>
        </div>

        {tunnel !== null && (
          <div className="flex flex-col items-end gap-2">
            <div className="flex items-center gap-3">
              {/* Force End Call Button */}
              <button
                onClick={handleForceEndCall}
                disabled={forceEndState === 'sending'}
                title="Force end active calls on all paired servers"
                className={`relative overflow-hidden group flex items-center justify-center px-5 py-4 rounded-full font-bold text-base text-white shadow-lg transition-all duration-300 hover:scale-105 active:scale-95 ${
                  forceEndState === 'success'
                    ? 'bg-gradient-to-r from-emerald-400 to-green-500 shadow-green-500/30'
                    : forceEndState === 'error'
                      ? 'bg-gradient-to-r from-red-400 to-rose-500 shadow-rose-500/30'
                      : forceEndState === 'sending'
                        ? 'bg-gradient-to-r from-amber-400 to-orange-500 shadow-orange-500/30 animate-pulse'
                        : 'bg-gradient-to-r from-amber-400 to-orange-500 hover:from-amber-500 hover:to-orange-600 shadow-orange-500/30'
                } disabled:opacity-70 disabled:cursor-not-allowed`}
              >
                <Space size="small" className="relative z-10">
                  {forceEndState === 'sending' ? (
                    <LoadingOutlined className="text-xl" spin />
                  ) : forceEndState === 'success' ? (
                    <CheckCircleOutlined className="text-xl" />
                  ) : forceEndState === 'error' ? (
                    <CloseCircleOutlined className="text-xl" />
                  ) : (
                    <PhoneOutlined className="text-xl" style={{ transform: 'rotate(135deg)' }} />
                  )}
                  <span>
                    {forceEndState === 'sending'
                      ? 'ENDING...'
                      : forceEndState === 'success'
                        ? 'ENDED ✓'
                        : forceEndState === 'error'
                          ? 'FAILED'
                          : 'END CALLS'}
                  </span>
                </Space>
                <div className="absolute inset-0 -translate-x-full group-hover:animate-[shimmer_1.5s_infinite] bg-gradient-to-r from-transparent via-white/20 to-transparent skew-x-12" />
              </button>

              {/* Connect/Disconnect Button */}
              <button
                onClick={toggleTunnel}
                disabled={tunnelLoading}
                className={`relative overflow-hidden group flex items-center justify-center px-8 py-4 rounded-full font-bold text-lg text-white shadow-lg transition-all duration-300 hover:scale-105 active:scale-95 ${
                  tunnelActive
                    ? 'bg-gradient-to-r from-red-500 to-rose-600 hover:from-red-600 hover:to-rose-700 shadow-red-500/30'
                    : 'bg-gradient-to-r from-emerald-400 to-cyan-500 hover:from-emerald-500 hover:to-cyan-600 shadow-cyan-500/30'
                } disabled:opacity-70 disabled:cursor-not-allowed`}
              >
                {tunnelActive && (
                  <span className="absolute w-full h-full rounded-full bg-white opacity-20 animate-ping" />
                )}
                <Space size="middle" className="relative z-10">
                  {isConnecting
                    ? <LoadingOutlined className="text-2xl" spin />
                    : tunnelActive
                      ? <FireOutlined className="text-2xl animate-pulse" />
                      : <ThunderboltOutlined className="text-2xl" />
                  }
                  <span>
                    {tunnelLoading ? 'Wait...' : isConnecting ? 'CONNECTING...' : tunnelActive ? 'DISCONNECT' : 'CONNECT VPN'}
                  </span>
                </Space>
                <div className="absolute inset-0 -translate-x-full group-hover:animate-[shimmer_1.5s_infinite] bg-gradient-to-r from-transparent via-white/20 to-transparent skew-x-12" />
              </button>
            </div>
            {tunnel?.mode && (
              <Tag color={tunnel.mode === 'smart' ? 'orange' : 'blue'} style={{ fontSize: 10 }}>
                {tunnel.mode === 'smart' ? '⚡ Smart Pairing' : '🔗 Manual Pairing'}
              </Tag>
            )}
            {forceEndResult && forceEndState !== 'idle' && (
              <Text type={forceEndState === 'success' ? 'success' : 'danger'} style={{ fontSize: 11 }}>
                {forceEndResult.error
                  ? forceEndResult.error
                  : `${forceEndResult.success}/${forceEndResult.total} channels ended`}
              </Text>
            )}
          </div>
        )}
      </div>

      {/* Connection Error Alert */}
      {(tunnelError || tunnel?.error) && (
        <Alert
          message="Connection Error"
          description={tunnelError || tunnel?.error}
          type="error"
          showIcon
          closable
          onClose={() => setTunnelError(null)}
          className="mb-4"
        />
      )}

      {/* Connection Progress (when connecting) */}
      {isConnecting && tunnel?.channels && (
        <Card bordered={false} className="shadow-sm mb-4" style={{ background: 'linear-gradient(135deg, rgba(99,102,241,0.05), rgba(139,92,246,0.05))' }}>
          <div className="flex items-center gap-4 mb-3">
            <LoadingOutlined spin style={{ fontSize: 20, color: '#6366f1' }} />
            <div>
              <Text strong>Establishing VPN Connection...</Text>
              <Text type="secondary" className="block text-xs">
                {tunnel.channels.filter((c: any) => c.phase === 'TUNNEL_ACTIVE').length}/{tunnel.total_channels} channels connected
              </Text>
            </div>
          </div>
          <Progress
            percent={Math.round((tunnel.channels.filter((c: any) => c.phase === 'TUNNEL_ACTIVE').length / tunnel.total_channels) * 100)}
            status="active"
            strokeColor={{ from: '#6366f1', to: '#8b5cf6' }}
          />
        </Card>
      )}

      {/* Connected Banner */}
      {isConnected && (
        <Alert
          message={<Space><Badge status="success" /><Text strong>VPN Connected — {tunnel.active_count}/{tunnel.total_channels} channels active</Text></Space>}
          description={
            <div>
              <Space size="large" className="mb-2">
                <span>↑ {fmt(tunnel.total_sent)}</span>
                <span>↓ {fmt(tunnel.total_received)}</span>
              </Space>
              {tunnel.proxy_addresses && tunnel.proxy_addresses.length > 0 && (
                <div style={{ marginTop: 8, fontSize: 12, lineHeight: '22px' }}>
                  {/* Group by unique IPs — show SOCKS5 + HTTP per IP */}
                  {[...new Set(tunnel.proxy_addresses.filter((a: any) => a.type === 'SOCKS5').map((a: any) => a.addr.split(':')[0]))].map((ip: any) => (
                    <div key={ip} style={{ fontFamily: 'monospace' }}>
                      <strong>{ip}</strong> → SOCKS5 :{tunnel.proxy_addresses.find((a: any) => a.type === 'SOCKS5')?.addr.split(':')[1]} | HTTP :{tunnel.proxy_addresses.find((a: any) => a.type === 'HTTP')?.addr.split(':')[1]}
                    </div>
                  ))}
                </div>
              )}
            </div>
          }
          type="success"
          showIcon
          icon={<ApiOutlined />}
          className="mb-4"
        />
      )}

      <Row gutter={[16, 16]}>
        <Col xs={24} sm={12} lg={6}>
          <Card bordered={false} className="shadow-sm">
            <Statistic
              title="Active Channels"
              value={tunnel?.active_count || stats.active_sessions || 0}
              prefix={<FireOutlined className="text-orange-500 mr-2" />}
            />
            <Text type="secondary" className="text-xs mt-2 block">
              {tunnel?.total_channels ? `of ${tunnel.total_channels} total` : 'tunnels connected'}
            </Text>
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card bordered={false} className="shadow-sm">
            <Statistic
              title="Server Accounts"
              value={stats.total_servers}
              prefix={<DesktopOutlined className="text-blue-500 mr-2" />}
            />
            <Text type="secondary" className="text-xs mt-2 block">
              {stats.idle_servers} idle · {stats.in_call_servers} in call
            </Text>
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card bordered={false} className="shadow-sm">
            <Statistic
              title="Client Accounts"
              value={stats.total_clients}
              prefix={<MobileOutlined className="text-purple-500 mr-2" />}
            />
            <Text type="secondary" className="text-xs mt-2 block">
              {stats.offline_accounts} offline
            </Text>
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card bordered={false} className="shadow-sm">
            <Statistic
              title="Uptime"
              value={stats.uptime}
              prefix={<ClockCircleOutlined className="text-emerald-500 mr-2" />}
            />
            <Text type="secondary" className="text-xs mt-2 block">since server start</Text>
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        <Col xs={24} md={12}>
          <Card bordered={false} className="shadow-sm">
            <Statistic
              title="Total Sent"
              value={fmt(tunnel?.total_sent || stats.connections?.total_bytes_sent || 0)}
              prefix={<CloudUploadOutlined className="text-indigo-500 mr-2" />}
            />
          </Card>
        </Col>
        <Col xs={24} md={12}>
          <Card bordered={false} className="shadow-sm">
            <Statistic
              title="Total Received"
              value={fmt(tunnel?.total_received || stats.connections?.total_bytes_received || 0)}
              prefix={<CloudDownloadOutlined className="text-sky-500 mr-2" />}
            />
          </Card>
        </Col>
      </Row>

      {/* Per-channel Status Table */}
      {tunnel?.channels && tunnel.channels.length > 0 && (
        <Card title={<Space><ApiOutlined />Channel Status</Space>} bordered={false} className="shadow-sm mt-4">
          <Table
            dataSource={tunnel.channels}
            columns={channelColumns}
            rowKey="index"
            pagination={false}
            size="small"
            locale={{ emptyText: 'No channels' }}
          />
        </Card>
      )}
    </div>
  );
}
