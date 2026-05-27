import React, { useState, useEffect } from 'react';
import { Drawer, Descriptions, Tag, Button, Space, Typography, Popconfirm, message, Form, Input, Divider, Alert } from 'antd';
import { DeleteOutlined, SyncOutlined, EditOutlined, SaveOutlined, CloseOutlined, KeyOutlined, UserOutlined } from '@ant-design/icons';
import { api } from '../../api';

const { Text, Paragraph } = Typography;
const { TextArea } = Input;

export interface AccountDetailsDrawerProps {
  accountId: number | null;
  onClose: () => void;
  onUpdate: () => void;
}

export function AccountDetailsDrawer({ accountId, onClose, onUpdate }: AccountDetailsDrawerProps) {
  const [account, setAccount] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [editing, setEditing] = useState(false);
  const [form] = Form.useForm();

  useEffect(() => {
    if (accountId) {
      loadAccount();
    } else {
      setAccount(null);
      setEditing(false);
    }
  }, [accountId]);

  const loadAccount = async () => {
    if (!accountId) return;
    setLoading(true);
    try {
      const data = await api.getAccount(accountId);
      setAccount(data);
      form.setFieldsValue({
        role: data.role,
        token: data.token,
      });
    } catch (e: any) {
      message.error('Failed to load account details');
      onClose();
    } finally {
      setLoading(false);
    }
  };

  const handleRefreshBale = async () => {
    if (!accountId) return;
    setRefreshing(true);
    try {
      const updated = await api.refreshAccount(accountId);
      setAccount(updated);
      message.success('Account information refreshed from Bale');
      onUpdate();
    } catch (e: any) {
      message.error(e.message || 'Failed to refresh account info. Token might be invalid.');
    } finally {
      setRefreshing(false);
    }
  };

  const handleSaveEdit = async () => {
    try {
      const values = await form.validateFields();
      setLoading(true);
      await api.updateAccount(accountId!, { token: values.token });
      message.success('Account updated successfully');
      setEditing(false);
      loadAccount();
      onUpdate();
    } catch (e: any) {
      if (e.errorFields) return; // Validation error
      message.error(e.message || 'Failed to update account');
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = async () => {
    if (!accountId) return;
    try {
      await api.deleteAccount(accountId);
      message.success('Account deleted successfully');
      onClose();
      onUpdate();
    } catch (e: any) {
      message.error('Failed to delete account');
    }
  };

  const getRoleColor = (role: string) => (role === 'SERVER' ? 'blue' : 'purple');
  const getStatusColor = (status: string) => {
    switch (status) {
      case 'IDLE': return 'blue';
      case 'IN_CALL': return 'green';
      case 'RESERVED': return 'orange';
      case 'OFFLINE': return 'default';
      case 'ERROR': return 'red';
      default: return 'default';
    }
  };

  return (
    <Drawer
      title="Advanced Account Details"
      width={600}
      open={!!accountId}
      onClose={onClose}
      extra={
        <Space>
          <Button icon={<SyncOutlined spin={refreshing} />} onClick={handleRefreshBale}>
            Sync with Bale
          </Button>
          <Popconfirm
            title="Delete Account"
            description="Are you sure you want to delete this account?"
            onConfirm={handleDelete}
            okText="Yes"
            cancelText="No"
            okButtonProps={{ danger: true }}
          >
            <Button danger icon={<DeleteOutlined />}>Delete</Button>
          </Popconfirm>
        </Space>
      }
    >
      {account ? (
        <div className="space-y-6">
          {/* Header Info */}
          <div className="flex items-center space-x-4 mb-6">
            <div className="w-16 h-16 rounded-full bg-slate-100 flex items-center justify-center border border-slate-200 shadow-sm">
              <UserOutlined className="text-3xl text-slate-400" />
            </div>
            <div>
              <h2 className="text-xl font-bold m-0">{account.display_name || 'Unknown Name'}</h2>
              <Text type="secondary" className="text-md">{account.phone || 'No phone number'}</Text>
            </div>
            <div className="ml-auto">
              <Tag color={getStatusColor(account.status)} className="text-sm px-3 py-1">
                {account.status}
              </Tag>
            </div>
          </div>

          <Divider />

          {!editing ? (
            <Descriptions column={1} bordered size="middle" className="shadow-sm rounded-lg overflow-hidden">
              <Descriptions.Item label="System ID"><Text strong>{account.id}</Text></Descriptions.Item>
              <Descriptions.Item label="Bale User ID"><Tag color="cyan" className="font-mono">{account.bale_user_id}</Tag></Descriptions.Item>
              <Descriptions.Item label="Role">
                <Tag color={getRoleColor(account.role)}>{account.role}</Tag>
              </Descriptions.Item>
              <Descriptions.Item label="Enabled">
                {account.enabled ? <Tag color="success">Yes</Tag> : <Tag color="error">No</Tag>}
              </Descriptions.Item>
              <Descriptions.Item label="Created At">
                {new Date(account.created_at).toLocaleString()}
              </Descriptions.Item>
              <Descriptions.Item label="Last Seen">
                {new Date(account.last_seen).toLocaleString()}
              </Descriptions.Item>
              <Descriptions.Item label="JWT Token">
                <div className="flex justify-between items-center w-full">
                  <Text type="secondary" className="font-mono text-xs truncate max-w-xs">
                    {account.token ? account.token.substring(0, 30) + '...' : 'None'}
                  </Text>
                  <Button size="small" type="link" icon={<EditOutlined />} onClick={() => setEditing(true)}>
                    Edit Settings
                  </Button>
                </div>
              </Descriptions.Item>
            </Descriptions>
          ) : (
            <Form form={form} layout="vertical" className="bg-slate-50 p-6 rounded-lg border border-slate-200 shadow-inner">
              <div className="flex justify-between items-center mb-4">
                <h3 className="text-lg font-semibold m-0 flex items-center gap-2">
                  <EditOutlined /> Edit Account Config
                </h3>
                <Space>
                  <Button size="small" icon={<CloseOutlined />} onClick={() => { setEditing(false); form.resetFields(); }}>
                    Cancel
                  </Button>
                  <Button size="small" type="primary" icon={<SaveOutlined />} loading={loading} onClick={handleSaveEdit}>
                    Save Changes
                  </Button>
                </Space>
              </div>

              <Form.Item label="Account Role">
                <Tag color={getRoleColor(account?.role)}>{account?.role}</Tag>
                <Text type="secondary" style={{ marginLeft: 8, fontSize: 12 }}>Auto-determined by panel</Text>
              </Form.Item>

              <Form.Item label="Bale JWT Token" name="token" rules={[{ required: true }]}>
                <TextArea rows={6} className="font-mono text-xs" />
              </Form.Item>
              
              <Alert 
                message="Updating Token" 
                description="Changing the JWT token will require the system to reconnect to Bale servers. Ensure the token is valid." 
                type="warning" 
                showIcon 
                className="mt-2"
              />
            </Form>
          )}

          {!editing && (
            <div className="mt-8 bg-blue-50 p-4 rounded-lg border border-blue-100">
              <h4 className="text-blue-800 font-semibold mb-2 flex items-center gap-2">
                <KeyOutlined /> Token Verification
              </h4>
              <p className="text-sm text-blue-600 mb-4">
                Clicking "Sync with Bale" at the top will verify if the current token is still valid by contacting Bale APIs. If it is invalid, it will show an error and you may need to update the token or re-login.
              </p>
              <Button type="primary" ghost block icon={<SyncOutlined />} onClick={handleRefreshBale} loading={refreshing}>
                Verify Token Validity Now
              </Button>
            </div>
          )}
        </div>
      ) : (
        <div className="flex justify-center items-center h-full">
          <SyncOutlined spin className="text-3xl text-slate-300" />
        </div>
      )}
    </Drawer>
  );
}
