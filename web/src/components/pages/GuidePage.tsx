import React, { useState, useEffect, useCallback } from 'react';
import { Card, Typography, Spin, message, Button, Tooltip } from 'antd';
import { BookOutlined, ArrowUpOutlined } from '@ant-design/icons';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import rehypeSlug from 'rehype-slug';
import { api } from '../../api';

const { Title, Text } = Typography;

export function GuidePage() {
  const [content, setContent] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(true);
  const [showScrollTop, setShowScrollTop] = useState(false);

  const load = useCallback(async () => {
    try {
      setLoading(true);
      const res = await api.getGuide();
      setContent(res.content);
    } catch (e: any) {
      message.error(e.message || 'Failed to load guide');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  // Track scroll position for "back to top" button.
  // The scrollable container is the ant Layout Content area, not window.
  useEffect(() => {
    const scrollContainer = document.querySelector('.ant-layout-content')?.parentElement?.querySelector('.ant-layout-content') || document.querySelector('main') || window;
    
    const handleScroll = () => {
      const scrollY = window.scrollY || document.documentElement.scrollTop || document.body.scrollTop;
      setShowScrollTop(scrollY > 300);
    };

    window.addEventListener('scroll', handleScroll, true);
    return () => window.removeEventListener('scroll', handleScroll, true);
  }, []);

  const scrollToTop = () => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
    // Also try scrolling the layout content area
    const contentEl = document.querySelector('.ant-layout-content');
    if (contentEl) {
      contentEl.scrollTo({ top: 0, behavior: 'smooth' });
    }
  };

  // Handle anchor clicks — scroll to the element instead of navigating
  const handleClick = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    const target = e.target as HTMLElement;
    const anchor = target.closest('a');
    if (anchor) {
      const href = anchor.getAttribute('href');
      if (href?.startsWith('#')) {
        e.preventDefault();
        const id = href.slice(1);
        const el = document.getElementById(id);
        if (el) {
          el.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
      }
    }
  }, []);

  return (
    <div className="space-y-6" style={{ overflow: 'visible', minHeight: 0 }}>
      <div className="mb-8">
        <Title level={2} style={{ margin: 0 }}>Guide</Title>
        <Text type="secondary">Documentation and usage instructions</Text>
      </div>

      <Card bordered={false} className="shadow-sm" style={{ minHeight: 500, overflow: 'visible' }}>
        {loading ? (
          <div className="flex items-center justify-center h-64">
            <Spin size="large" />
          </div>
        ) : content ? (
          <div
            className="prose prose-slate dark:prose-invert max-w-none prose-headings:font-semibold prose-a:text-indigo-500 hover:prose-a:text-indigo-600"
            onClick={handleClick}
            style={{ overflow: 'visible' }}
          >
            <ReactMarkdown remarkPlugins={[remarkGfm]} rehypePlugins={[rehypeSlug]}>
              {content}
            </ReactMarkdown>
          </div>
        ) : (
          <div className="py-8 text-center text-slate-400">
            <BookOutlined className="text-4xl mb-2" />
            <p>Guide content is empty or could not be loaded.</p>
          </div>
        )}
      </Card>

      {/* Scroll to top button */}
      {showScrollTop && (
        <Tooltip title="Back to top" placement="left">
          <Button
            type="primary"
            shape="circle"
            size="large"
            icon={<ArrowUpOutlined />}
            onClick={scrollToTop}
            style={{
              position: 'fixed',
              bottom: 32,
              right: 32,
              zIndex: 1000,
              width: 48,
              height: 48,
              boxShadow: '0 4px 16px rgba(99, 102, 241, 0.4)',
              background: 'linear-gradient(135deg, #6366f1, #8b5cf6)',
              border: 'none',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              transition: 'all 0.3s ease',
            }}
          />
        </Tooltip>
      )}
    </div>
  );
}
