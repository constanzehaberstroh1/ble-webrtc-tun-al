import React, { useState } from 'react';
import { Layout, Button, Typography, Space, Tooltip } from 'antd';
import { Link, useRouterState } from '@tanstack/react-router';
import {
  DashboardOutlined,
  UserOutlined,
  LinkOutlined,
  HistoryOutlined,
  SettingOutlined,
  LogoutOutlined,
  ThunderboltFilled,
  BookOutlined,
  CloudSyncOutlined,
  CodeOutlined,
  FileSearchOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
} from '@ant-design/icons';
import { useTheme } from '../../ThemeContext';

const { Sider, Content } = Layout;
const { Title } = Typography;

const NAV_ITEMS = [
  { path: '/', icon: <DashboardOutlined />, label: 'Dashboard' },
  { path: '/accounts', icon: <UserOutlined />, label: 'Accounts' },
  { path: '/pairings', icon: <LinkOutlined />, label: 'Pairings' },
  { path: '/connections', icon: <HistoryOutlined />, label: 'History' },
  { path: '/terminal', icon: <CodeOutlined />, label: 'Terminal' },
  { path: '/logs', icon: <FileSearchOutlined />, label: 'Logs' },
  { path: '/guide', icon: <BookOutlined />, label: 'Guide' },
  { path: '/settings', icon: <SettingOutlined />, label: 'Settings' },
];

export function AppLayout({
  onLogout,
  onSyncFromServer,
  syncing,
  children,
}: {
  onLogout: () => void;
  onSyncFromServer?: () => void;
  syncing?: boolean;
  children: React.ReactNode;
}) {
  const { settings } = useTheme();
  const [collapsed, setCollapsed] = useState(false);
  const routerState = useRouterState();
  const currentPath = routerState.location.pathname;

  return (
    <Layout className="min-h-screen">
      <Sider
        collapsible
        collapsed={collapsed}
        onCollapse={(value) => setCollapsed(value)}
        trigger={null}
        theme={settings.mode === 'dark' ? 'dark' : 'light'}
        className="z-20 border-r border-slate-200 dark:border-slate-800/60"
        style={{
          background: settings.mode === 'dark' ? '#141414' : '#ffffff',
        }}
      >
        <div className="h-16 flex items-center justify-center border-b border-slate-200 dark:border-slate-800/60">
          <Space>
            <div className="w-8 h-8 rounded-lg bg-indigo-600 flex items-center justify-center text-white">
              <ThunderboltFilled />
            </div>
            {!collapsed && (
              <Title level={5} style={{ margin: 0, fontWeight: 600 }} className="text-slate-800 dark:text-slate-200 tracking-tight">
                BLE Tunnel
              </Title>
            )}
          </Space>
        </div>
        
        <div className="flex flex-col h-[calc(100vh-64px)]">
          {/* Navigation Links */}
          <nav className="flex-1 py-4 px-2 space-y-1 overflow-y-auto">
            {NAV_ITEMS.map((item) => {
              const isActive = item.path === '/'
                ? currentPath === '/'
                : currentPath.startsWith(item.path);

              return (
                <Link
                  key={item.path}
                  to={item.path}
                  className={`flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all duration-200 no-underline ${
                    isActive
                      ? settings.mode === 'dark'
                        ? 'bg-indigo-500/15 text-indigo-300 border border-indigo-500/25'
                        : 'bg-indigo-50 text-indigo-700 border border-indigo-200/60'
                      : settings.mode === 'dark'
                        ? 'text-slate-400 hover:text-slate-200 hover:bg-white/5 border border-transparent'
                        : 'text-slate-600 hover:text-slate-900 hover:bg-slate-50 border border-transparent'
                  }`}
                >
                  <span className="text-base" style={{ minWidth: 20, textAlign: 'center' }}>{item.icon}</span>
                  {!collapsed && <span>{item.label}</span>}
                </Link>
              );
            })}
          </nav>
          
          {/* Bottom actions */}
          <div className="p-3 border-t border-slate-200 dark:border-slate-800 space-y-2">
            {/* Collapse toggle */}
            <Button
              type="text"
              icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
              onClick={() => setCollapsed(!collapsed)}
              className="w-full flex items-center justify-center"
              style={{ color: settings.mode === 'dark' ? '#94a3b8' : '#64748b' }}
            />

            {onSyncFromServer && (
              <Tooltip title="Pull all server accounts & pairings to local" placement="right">
                <Button
                  type="primary"
                  icon={<CloudSyncOutlined />}
                  onClick={onSyncFromServer}
                  loading={syncing}
                  className="w-full"
                  style={{
                    background: 'linear-gradient(135deg, #13c2c2 0%, #1890ff 100%)',
                    border: 'none',
                    height: 36,
                    fontWeight: 500,
                  }}
                >
                  {!collapsed && 'Sync from Server'}
                </Button>
              </Tooltip>
            )}
            <Button
              type="text"
              danger
              icon={<LogoutOutlined />}
              onClick={onLogout}
              className="w-full flex items-center justify-center"
            >
              {!collapsed && 'Logout'}
            </Button>
          </div>
        </div>
      </Sider>

      <Layout className="transition-colors duration-300" style={{ background: settings.mode === 'dark' ? '#000000' : '#f0f2f5' }}>
        <Content className="p-8 w-full relative z-10 mx-auto max-w-7xl">
          {children}
        </Content>
      </Layout>
    </Layout>
  );
}

export * from './AccountDetailsDrawer';
export * from './FloatingLogConsole';
