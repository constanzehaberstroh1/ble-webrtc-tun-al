import React, { useState, useEffect, useRef } from 'react';
import { Button, Card, Typography, Tag, Space, Badge } from 'antd';
import { CodeOutlined, CloseOutlined, ExpandAltOutlined, ShrinkOutlined } from '@ant-design/icons';
import { api } from '../../api';

const { Text } = Typography;

export function FloatingLogConsole() {
  const [open, setOpen] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [logs, setLogs] = useState<any[]>([]);
  const [autoScroll, setAutoScroll] = useState(true);
  const logsEndRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const fetchLogs = async () => {
    if (!open) return;
    try {
      const res = await api.getLogs(500);
      if (res && res.logs) {
        setLogs(res.logs);
      }
    } catch (e) {
      console.error('Failed to fetch logs:', e);
    }
  };

  useEffect(() => {
    let interval: any;
    if (open) {
      fetchLogs();
      interval = setInterval(fetchLogs, 1500);
    }
    return () => clearInterval(interval);
  }, [open]);

  useEffect(() => {
    if (autoScroll && logsEndRef.current && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [logs, autoScroll]);

  const handleScroll = () => {
    if (containerRef.current) {
      const { scrollTop, scrollHeight, clientHeight } = containerRef.current;
      const isBottom = Math.abs(scrollHeight - clientHeight - scrollTop) < 10;
      setAutoScroll(isBottom);
    }
  };

  const getLevelColor = (level: string) => {
    switch (level) {
      case 'ERROR': case 'FATAL': return '#f5222d';
      case 'WARN': return '#faad14';
      case 'INFO': return '#52c41a';
      default: return '#1890ff';
    }
  };

  const getComponentColor = (comp: string) => {
    switch (comp) {
      case 'BALE': return 'blue';
      case 'SFU': return 'purple';
      case 'TUNNEL': return 'cyan';
      case 'MAIN': return 'magenta';
      default: return 'default';
    }
  };

  if (!open) {
    return (
      <Button
        type="primary"
        shape="round"
        icon={<CodeOutlined />}
        size="large"
        className="z-50 shadow-xl flex items-center justify-center bg-gray-900 hover:bg-gray-800 border-gray-700"
        style={{ position: 'fixed', right: '24px', bottom: '24px' }}
        onClick={() => setOpen(true)}
      >
        Live Logs
      </Button>
    );
  }

  return (
    <Card
      className="z-50 shadow-2xl transition-all duration-300 flex flex-col"
      style={{
        position: 'fixed',
        right: '24px',
        bottom: 0,
        width: expanded ? '80vw' : '500px',
        height: expanded ? '80vh' : '400px',
        backgroundColor: '#1e1e1e',
        borderColor: '#333',
        borderBottom: 'none',
        borderBottomLeftRadius: 0,
        borderBottomRightRadius: 0,
      }}
      bodyStyle={{ padding: 0, display: 'flex', flexDirection: 'column', height: '100%' }}
    >
      <div className="flex justify-between items-center px-4 py-2 bg-gray-900 border-b border-gray-800 rounded-t-lg">
        <Space>
          <CodeOutlined style={{ color: '#52c41a' }} />
          <span className="text-gray-200 font-mono font-bold">System Terminal</span>
          {autoScroll ? (
            <Badge status="processing" text={<span className="text-xs text-gray-400">Live</span>} />
          ) : (
            <span className="text-xs text-gray-500 ml-2">(Auto-scroll paused)</span>
          )}
        </Space>
        <Space>
          <Button
            type="text"
            size="small"
            icon={expanded ? <ShrinkOutlined /> : <ExpandAltOutlined />}
            onClick={() => setExpanded(!expanded)}
            style={{ color: '#888' }}
          />
          <Button
            type="text"
            size="small"
            icon={<CloseOutlined />}
            onClick={() => setOpen(false)}
            style={{ color: '#888' }}
          />
        </Space>
      </div>

      <div
        ref={containerRef}
        onScroll={handleScroll}
        className="flex-1 overflow-y-auto p-3 font-mono text-xs bg-[#1e1e1e] text-gray-300 custom-scrollbar"
        style={{ scrollBehavior: autoScroll ? 'auto' : 'auto' }}
      >
        {logs.map((log, i) => (
          <div key={i} className="mb-1 flex items-start break-words hover:bg-gray-800 px-1 py-0.5 rounded">
            <span className="text-gray-500 mr-2 shrink-0">{log.timestamp.split(' ')[1]}</span>
            <span style={{ color: getLevelColor(log.level), width: '45px' }} className="shrink-0 font-bold">
              {log.level}
            </span>
            <Tag color={getComponentColor(log.component)} className="mr-2 border-0 opacity-80 shrink-0 text-[10px] leading-3 h-4">
              {log.component}
            </Tag>
            <span className="text-gray-300 break-all">{log.message}</span>
          </div>
        ))}
        <div ref={logsEndRef} />
      </div>

      <style>{`
        .custom-scrollbar::-webkit-scrollbar {
          width: 8px;
        }
        .custom-scrollbar::-webkit-scrollbar-track {
          background: #1e1e1e;
        }
        .custom-scrollbar::-webkit-scrollbar-thumb {
          background: #444;
          border-radius: 4px;
        }
        .custom-scrollbar::-webkit-scrollbar-thumb:hover {
          background: #555;
        }
      `}</style>
    </Card>
  );
}
