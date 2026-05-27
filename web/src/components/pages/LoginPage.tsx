import React, { useState } from 'react';
import { Button, Input, Form, Typography, Card, Alert } from 'antd';
import { LockOutlined, UserOutlined } from '@ant-design/icons';
import { useRouter } from '@tanstack/react-router';
import { testAuth } from '../../api';

const { Title, Text } = Typography;

export function LoginPage() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const router = useRouter();

  const onFinish = async (values: any) => {
    setError('');
    setLoading(true);
    try {
      await testAuth(values.username, values.password);
      router.navigate({ to: '/' });
    } catch (err: any) {
      setError(err.message || 'Invalid credentials');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center px-4 bg-slate-50 dark:bg-[#141414] transition-colors">
      <div className="relative w-full max-w-sm z-10">
        <div className="text-center mb-8">
          <div className="w-16 h-16 mx-auto rounded-2xl bg-indigo-600 flex items-center justify-center text-3xl shadow-lg mb-5 text-white">
            ⚡
          </div>
          <Title level={2} style={{ margin: 0, fontWeight: 600 }} className="text-slate-800 dark:text-slate-200 tracking-tight">BLE Tunnel</Title>
          <Text type="secondary">Admin Console</Text>
        </div>

        <Card 
          className="shadow-sm border-slate-200 dark:border-slate-800"
          bordered={true}
        >
          <Form
            name="login"
            onFinish={onFinish}
            layout="vertical"
            size="large"
          >
            <Form.Item
              name="username"
              rules={[{ required: true, message: 'Please input your Username!' }]}
            >
              <Input prefix={<UserOutlined className="text-slate-400" />} placeholder="admin" autoFocus />
            </Form.Item>
            <Form.Item
              name="password"
              rules={[{ required: true, message: 'Please input your Password!' }]}
            >
              <Input.Password prefix={<LockOutlined className="text-slate-400" />} placeholder="••••••••" />
            </Form.Item>

            {error && (
              <Alert message={error} type="error" showIcon className="mb-4" />
            )}

            <Form.Item className="mb-0">
              <Button type="primary" htmlType="submit" className="w-full" loading={loading}>
                Sign in
              </Button>
            </Form.Item>
          </Form>
        </Card>
        
        <p className="text-center text-xs text-slate-500 dark:text-slate-400 mt-6">
          Credentials stored in database
        </p>
      </div>
    </div>
  );
}
