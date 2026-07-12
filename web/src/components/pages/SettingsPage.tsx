import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Card, Typography, Select, Button, Space, Row, Col, Tag, message, Descriptions, Badge, Alert, Upload, Divider } from 'antd';
import { SyncOutlined, CloudSyncOutlined, CheckCircleOutlined, DownloadOutlined, UploadOutlined, DatabaseOutlined, ExclamationCircleOutlined, LinkOutlined, GlobalOutlined } from '@ant-design/icons';
import { useTheme, THEMES, MODES } from '../../ThemeContext';
import { api } from '../../api';

const { Title, Text } = Typography;

const SWATCH_COLORS: Record<string, [string, string]> = {
  indigo:  ['#6366f1', '#818cf8'],
  violet:  ['#8b5cf6', '#a78bfa'],
  emerald: ['#10b981', '#34d399'],
  rose:    ['#f43f5e', '#fb7185'],
  amber:   ['#f59e0b', '#fbbf24'],
  cyan:    ['#06b6d4', '#22d3ee'],
};

export function SettingsPage() {
  const { settings, update } = useTheme();
  const [syncStatus, setSyncStatus] = useState<any>(null);
  const [syncing, setSyncing] = useState(false);
  const [pushing, setPushing] = useState<string | null>(null);
  const [backingUp, setBackingUp] = useState(false);
  const [restoring, setRestoring] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [panelRole, setPanelRole] = useState<string>('');
  const [serverURLs, setServerURLs] = useState<string[]>([]);
  const [remoteServerURL, setRemoteServerURL] = useState<string>('');
  const [baleConstants, setBaleConstants] = useState<any>(null);
  const [baleSyncing, setBaleSyncing] = useState(false);

  const loadSyncStatus = useCallback(async () => {
    try {
      const status = await api.syncStatus();
      setSyncStatus(status);
      setPanelRole(status.role === 'client' ? 'CLIENT' : 'SERVER');
    } catch { /* ignore */ }
    try {
      const stats = await api.getStats();
      if (stats.server_urls) setServerURLs(stats.server_urls);
      if (stats.remote_server_url) setRemoteServerURL(stats.remote_server_url);
    } catch { /* ignore */ }
  }, []);

  const loadBaleConstants = useCallback(async () => {
    try {
      const data = await api.getBaleConstants();
      setBaleConstants(data);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => { loadSyncStatus(); loadBaleConstants(); }, [loadSyncStatus, loadBaleConstants]);

  const handleBaleSync = async () => {
    setBaleSyncing(true);
    try {
      const data = await api.syncBaleConstants();
      if (data.status === 'success') {
        setBaleConstants(data);
        message.success('Bale constants synchronized from upstream');
      } else {
        message.error(data.message || 'Sync failed');
      }
    } catch (e: any) {
      message.error(e.message || 'Failed to sync Bale constants');
    } finally {
      setBaleSyncing(false);
    }
  };

  const handleManualSync = async () => {
    setSyncing(true);
    try {
      // 1. Push all local accounts to the remote server
      const pushRes = await api.remoteSyncPushAccounts();
      if (pushRes.pushed > 0) {
        message.success(`Pushed ${pushRes.pushed} accounts to remote server`);
      }

      // 2. Sync audit events (local -> remote)
      try {
        const localEvents = await api.syncPull(0);
        if (localEvents.events && localEvents.events.length > 0) {
          await api.remoteSyncPush(localEvents.events);
        }
      } catch (e) { console.warn('Audit sync push failed', e); }

      message.success('Synchronized with remote server successfully');
      await loadSyncStatus();
    } catch (e: any) {
      message.error(e.message || 'Sync failed');
    } finally {
      setSyncing(false);
    }
  };


  return (
    <div className="space-y-6">
      <div className="mb-8">
        <Title level={2} style={{ margin: 0 }}>Settings</Title>
        <Text type="secondary">Customize your admin panel experience</Text>
      </div>

      <Card title="Appearance" bordered={false} className="shadow-sm mb-6">
        <div className="mb-8">
          <Text type="secondary" strong className="block mb-4 uppercase text-xs tracking-wider">Mode</Text>
          <div className="flex gap-4">
            {MODES.map((mode) => (
              <div
                key={mode}
                onClick={() => update('mode', mode as 'dark' | 'light')}
                className={`flex-1 p-4 rounded-xl border-2 transition-all cursor-pointer flex items-center justify-center gap-4 ${
                  settings.mode === mode
                    ? 'border-indigo-500 bg-indigo-500/10'
                    : 'border-transparent bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700'
                }`}
              >
                <span className="text-2xl">{mode === 'dark' ? '🌙' : '☀️'}</span>
                <div>
                  <div className={`text-sm font-semibold ${settings.mode === mode ? 'text-indigo-600 dark:text-indigo-400' : 'text-slate-700 dark:text-slate-300'}`}>
                    {mode === 'dark' ? 'Dark Mode' : 'Light Mode'}
                  </div>
                  <div className="text-xs text-slate-500">
                    {mode === 'dark' ? 'Easy on the eyes' : 'Bright and clean'}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
        <div>
          <Text type="secondary" strong className="block mb-4 uppercase text-xs tracking-wider">Accent Color</Text>
          <div className="grid grid-cols-3 sm:grid-cols-6 gap-4">
            {Object.entries(THEMES).map(([key, theme]) => {
              const [c1, c2] = SWATCH_COLORS[key];
              const isActive = settings.color === key;
              return (
                <div
                  key={key}
                  onClick={() => update('color', key)}
                  className={`flex flex-col items-center gap-2 p-3 rounded-xl cursor-pointer transition-all ${
                    isActive ? 'bg-slate-100 dark:bg-slate-800' : 'hover:bg-slate-50 dark:hover:bg-slate-800/50'
                  }`}
                >
                  <div
                    className={`w-10 h-10 rounded-full transition-transform ${isActive ? 'scale-110 shadow-lg ring-2 ring-offset-2 ring-offset-white dark:ring-offset-slate-900' : ''}`}
                    style={{ background: `linear-gradient(135deg, ${c1}, ${c2})` }}
                  />
                  <Text className={`text-xs ${isActive ? 'font-bold' : ''}`}>{theme.name}</Text>
                </div>
              );
            })}
          </div>
        </div>
      </Card>

      <Card title="Panel Options" bordered={false} className="shadow-sm mb-6">
        <Row gutter={[24, 24]}>
          <Col xs={24} md={12}>
            <Text type="secondary" strong className="block mb-2 uppercase text-xs tracking-wider">Auto-refresh Interval</Text>
            <Select className="w-full" size="large" value={settings.refreshInterval} onChange={(v) => update('refreshInterval', Number(v))}>
              <Select.Option value={3}>3 seconds</Select.Option>
              <Select.Option value={5}>5 seconds</Select.Option>
              <Select.Option value={10}>10 seconds</Select.Option>
              <Select.Option value={30}>30 seconds</Select.Option>
              <Select.Option value={60}>60 seconds</Select.Option>
            </Select>
          </Col>
          <Col xs={24} md={12}>
            <Text type="secondary" strong className="block mb-2 uppercase text-xs tracking-wider">Connection History</Text>
            <Select className="w-full" size="large" value={settings.historyLimit || 50} onChange={(v) => update('historyLimit', Number(v))}>
              <Select.Option value={25}>Last 25 sessions</Select.Option>
              <Select.Option value={50}>Last 50 sessions</Select.Option>
              <Select.Option value={100}>Last 100 sessions</Select.Option>
            </Select>
          </Col>
        </Row>
      </Card>

      {/* Bale Client Constants — Dynamic Upstream Parameter Extraction */}
      <Card
        title={<><CloudSyncOutlined className="mr-2" />Bale Client Constants</>}
        bordered={false}
        className="shadow-sm mb-6"
        extra={
          <Space>
            <Button
              type="primary"
              icon={<SyncOutlined spin={baleSyncing} />}
              loading={baleSyncing}
              onClick={handleBaleSync}
            >
              Sync from Bale
            </Button>
          </Space>
        }
      >
        <Alert
          message="Dynamic Upstream Parameter Extraction"
          description="Bale updates client protocol parameters with each release. Click 'Sync from Bale' to scrape the live web bundle (web.bale.ai) and hot-swap these constants — new connections immediately use the updated values without a restart."
          type="info"
          showIcon
          icon={<CloudSyncOutlined />}
          className="mb-4"
        />

        {baleConstants ? (
          <>
            <Descriptions column={{ xs: 1, md: 2 }} size="small" bordered>
              <Descriptions.Item label="App Version">
                <Tag color="blue" className="font-mono">{baleConstants.app_version || '—'}</Tag>
              </Descriptions.Item>
              <Descriptions.Item label="LiveKit SDK Version">
                <Tag color="geekblue" className="font-mono">{baleConstants.livekit_sdk_version || '—'}</Tag>
              </Descriptions.Item>
              <Descriptions.Item label="LiveKit Protocol">
                <Tag color="geekblue" className="font-mono">v{baleConstants.livekit_protocol_version || '—'}</Tag>
                <Text type="secondary" className="ml-2 text-xs">subprotocol: lk-protocol-{baleConstants.livekit_protocol_version || '?'}</Text>
              </Descriptions.Item>
              <Descriptions.Item label="Browser Version">
                <Tag color="cyan" className="font-mono">{baleConstants.browser_version || '—'}</Tag>
              </Descriptions.Item>
              <Descriptions.Item label="Bale WS URL" span={2}>
                <Text className="font-mono text-xs">{baleConstants.bale_ws_url || '—'}</Text>
              </Descriptions.Item>
              <Descriptions.Item label="Bale gRPC Base" span={2}>
                <Text className="font-mono text-xs">{baleConstants.bale_grpc_base || '—'}</Text>
              </Descriptions.Item>
              <Descriptions.Item label="LiveKit Origin">
                <Text className="font-mono text-xs">{baleConstants.livekit_origin || '—'}</Text>
              </Descriptions.Item>
              <Descriptions.Item label="Bale Web Origin">
                <Text className="font-mono text-xs">{baleConstants.bale_web_origin || '—'}</Text>
              </Descriptions.Item>
              <Descriptions.Item label="Web API Key" span={2}>
                <Text className="font-mono text-xs" style={{ wordBreak: 'break-all' }}>
                  {baleConstants.web_api_key ? `${baleConstants.web_api_key.slice(0, 12)}...${baleConstants.web_api_key.slice(-8)}` : '—'}
                </Text>
              </Descriptions.Item>
            </Descriptions>

            <div className="mt-3 text-xs text-slate-400">
              <Badge
                status={baleSyncing ? 'processing' : (baleConstants.last_synced_at ? 'success' : 'default')}
                text={
                  baleSyncing ? 'Extracting parameters from upstream...'
                    : baleConstants.last_synced_at
                      ? `Last synced: ${baleConstants.last_synced_at}`
                      : 'Never synced — using default values'
                }
              />
            </div>
          </>
        ) : (
          <Text type="secondary">Loading Bale client constants...</Text>
        )}
      </Card>

      {/* Server URLs */}
      {(serverURLs.length > 0 || remoteServerURL) && (
        <Card
          title={<><GlobalOutlined className="mr-2" />Server Addresses</>}
          bordered={false}
          className="shadow-sm mb-6"
        >
          <Text type="secondary" className="block mb-3">
            {panelRole === 'SERVER'
              ? 'This server is accessible at the following URLs:'
              : 'Connected remote server addresses:'}
          </Text>
          <Space direction="vertical" className="w-full">
            {serverURLs.map((url: string, idx: number) => (
              <div key={idx} className="flex items-center gap-2 p-2 rounded-lg" style={{ background: 'rgba(99,102,241,0.06)' }}>
                <LinkOutlined className="text-indigo-400" />
                <Text strong className="font-mono text-sm flex-1">{url}</Text>
                <Button
                  type="link"
                  size="small"
                  icon={<LinkOutlined />}
                  onClick={() => window.open(url, '_blank')}
                >
                  Open
                </Button>
              </div>
            ))}
            {remoteServerURL && !serverURLs.includes(remoteServerURL) && (
              <div className="flex items-center gap-2 p-2 rounded-lg" style={{ background: 'rgba(16,185,129,0.06)' }}>
                <LinkOutlined className="text-emerald-400" />
                <Text strong className="font-mono text-sm flex-1">{remoteServerURL}</Text>
                <Tag color="green" className="mr-0">Remote</Tag>
                <Button
                  type="link"
                  size="small"
                  icon={<LinkOutlined />}
                  onClick={() => window.open(remoteServerURL, '_blank')}
                >
                  Open
                </Button>
              </div>
            )}
          </Space>
        </Card>
      )}

      <Card
        title={<><CloudSyncOutlined className="mr-2" />Synchronization</>}
        bordered={false}
        className="shadow-sm mb-6"
        extra={
          panelRole === 'CLIENT' ? (
            <Space>
              <Button type="primary" icon={<SyncOutlined spin={syncing} />} loading={syncing} onClick={handleManualSync}>
                Sync Now
              </Button>
            </Space>
          ) : null
        }
      >
        {panelRole === 'CLIENT' && (
          <>
            <Alert
              message="Automatic Sync Active"
              description="This client admin panel automatically syncs with the server admin panel via long-polling. Account and pairing changes are pushed to the server immediately and pulled from the server in real-time."
              type="success"
              showIcon
              icon={<CheckCircleOutlined />}
              className="mb-4"
            />

            <Card type="inner" title="Push to Server (Restore)" size="small" className="mb-4">
              <Text type="secondary" className="block mb-3">
                If the server lost its data after a redeployment, use these buttons to push your local accounts and pairings to the server to restore them.
              </Text>
              <Space wrap>
                <Button
                  icon={<CloudSyncOutlined />}
                  loading={pushing === 'accounts'}
                  onClick={async () => {
                    setPushing('accounts');
                    try {
                      const res = await api.remoteSyncPushAccounts();
                      message.success(`Pushed ${res.pushed} accounts (${res.failed} failed)`);
                    } catch (e: any) { message.error(e.message); }
                    finally { setPushing(null); }
                  }}
                >
                  Push All Accounts
                </Button>
                <Button
                  icon={<CloudSyncOutlined />}
                  loading={pushing === 'pairings'}
                  onClick={async () => {
                    setPushing('pairings');
                    try {
                      await api.remotePushAllPairings();
                      message.success('All pairings pushed to server');
                    } catch (e: any) { message.error(e.message); }
                    finally { setPushing(null); }
                  }}
                >
                  Push All Pairings
                </Button>
                <Button
                  type="primary"
                  icon={<SyncOutlined />}
                  loading={pushing === 'full'}
                  onClick={async () => {
                    setPushing('full');
                    try {
                      const accRes = await api.remoteSyncPushAccounts();
                      await api.remotePushAllPairings();
                      message.success(`Full restore: ${accRes.pushed} accounts + pairings pushed`);
                    } catch (e: any) { message.error(e.message); }
                    finally { setPushing(null); }
                  }}
                >
                  Full Restore to Server
                </Button>
              </Space>
            </Card>
          </>
        )}

        {panelRole === 'SERVER' && (
          <Alert
            message="Server Admin Panel"
            description="This is the server admin panel. Data is stored locally and synced from client admin panels. If data was lost after a redeployment, use 'Full Restore to Server' from the client admin panel to push accounts and pairings back."
            type="info"
            showIcon
            className="mb-4"
          />
        )}

        {syncStatus ? (
          <Descriptions column={{ xs: 1, md: 2 }} size="small">
            <Descriptions.Item label="Role">
              <Tag color={syncStatus.role === 'server' ? 'blue' : 'green'}>{syncStatus.role?.toUpperCase()}</Tag>
            </Descriptions.Item>
            <Descriptions.Item label="Latest Event ID">
              <Badge status="processing" text={syncStatus.latest_event_id || 0} />
            </Descriptions.Item>
            <Descriptions.Item label="Accounts">{syncStatus.accounts || 0}</Descriptions.Item>
            <Descriptions.Item label="Pairings">{syncStatus.pairings || 0}</Descriptions.Item>
            <Descriptions.Item label="Last Sync Seq">{syncStatus.last_sync_seq || 'Never'}</Descriptions.Item>
            <Descriptions.Item label="Timestamp">{syncStatus.timestamp || '—'}</Descriptions.Item>
          </Descriptions>
        ) : (
          <Text type="secondary">Loading sync status...</Text>
        )}
      </Card>

      <Card
        title={<><DatabaseOutlined className="mr-2" />Backup & Restore</>}
        bordered={false}
        className="shadow-sm mb-6"
      >
        <Alert
          message="Full Database Backup"
          description="Export all accounts (including tokens), pairings, and settings to a JSON file. You can restore this backup on any panel — even after a full redeployment."
          type="info"
          showIcon
          icon={<ExclamationCircleOutlined />}
          className="mb-4"
        />

        <Row gutter={[16, 16]}>
          <Col xs={24} md={12}>
            <Card type="inner" title="Download Backup" size="small">
              <Text type="secondary" className="block mb-3">
                Downloads a JSON file containing all accounts, pairings, and settings.
              </Text>
              <Button
                type="primary"
                icon={<DownloadOutlined />}
                loading={backingUp}
                onClick={async () => {
                  setBackingUp(true);
                  try {
                    const data = await api.dbBackup();
                    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = `ble-tunnel-backup-${new Date().toISOString().slice(0,10)}.json`;
                    a.click();
                    URL.revokeObjectURL(url);
                    message.success(`Backup downloaded: ${data.accounts?.length || 0} accounts, ${data.pairings?.length || 0} pairings`);
                  } catch (e: any) {
                    message.error(e.message || 'Backup failed');
                  } finally {
                    setBackingUp(false);
                  }
                }}
                block
              >
                Download Backup
              </Button>
            </Card>
          </Col>
          <Col xs={24} md={12}>
            <Card type="inner" title="Restore from Backup" size="small">
              <Text type="secondary" className="block mb-3">
                Upload a backup JSON file to restore accounts, pairings, and settings.
              </Text>
              <input
                ref={fileInputRef}
                type="file"
                accept=".json"
                style={{ display: 'none' }}
                onChange={async (e) => {
                  const file = e.target.files?.[0];
                  if (!file) return;
                  setRestoring(true);
                  try {
                    const text = await file.text();
                    const data = JSON.parse(text);
                    if (!data.version || !data.accounts) {
                      throw new Error('Invalid backup file format');
                    }
                    const res = await api.dbRestore(data);
                    const parts: string[] = [];
                    if (res.accounts_created > 0) parts.push(`${res.accounts_created} accounts created`);
                    if (res.accounts_updated > 0) parts.push(`${res.accounts_updated} accounts updated`);
                    if (res.pairings_created > 0) parts.push(`${res.pairings_created} pairings created`);
                    if (res.settings_restored > 0) parts.push(`${res.settings_restored} settings restored`);
                    if (parts.length > 0) {
                      message.success(`Restore complete: ${parts.join(', ')}`);
                    } else {
                      message.info('Restore complete — all data already up to date');
                    }
                    if (res.accounts_failed > 0 || res.pairings_failed > 0) {
                      message.warning(`${res.accounts_failed} accounts and ${res.pairings_failed} pairings failed`);
                    }
                    loadSyncStatus();
                  } catch (e: any) {
                    message.error(e.message || 'Restore failed');
                  } finally {
                    setRestoring(false);
                    if (fileInputRef.current) fileInputRef.current.value = '';
                  }
                }}
              />
              <Button
                icon={<UploadOutlined />}
                loading={restoring}
                onClick={() => fileInputRef.current?.click()}
                block
                danger
              >
                Upload & Restore
              </Button>
            </Card>
          </Col>
        </Row>
      </Card>

      <Card title="Theme Preview" bordered={false} className="shadow-sm">
        <Row gutter={[24, 24]}>
          <Col xs={24} md={8}>
            <Card type="inner" title="Inner Card" size="small">Inner card content</Card>
          </Col>
          <Col xs={24} md={8}>
            <Space direction="vertical" className="w-full">
              <Button type="primary" block>Primary Button</Button>
              <Button block>Default Button</Button>
              <Button danger block>Danger Button</Button>
            </Space>
          </Col>
          <Col xs={24} md={8}>
            <Space>
              <Tag color="success">Active</Tag>
              <Tag color="processing">Processing</Tag>
              <Tag color="error">Error</Tag>
            </Space>
          </Col>
        </Row>
      </Card>
    </div>
  );
}
