import { useState } from 'react'
import { Outlet, useRouter, useMatches } from '@tanstack/react-router'
import { message } from 'antd'
import { clearAuth, api, loadAuth } from './api'
import { AppLayout, FloatingLogConsole } from './components/organisms'

export function RootLayout() {
  const router = useRouter()
  const matches = useMatches()
  const [syncing, setSyncing] = useState(false)

  // Check if we're on the login page
  const isLoginPage = matches.some(m => m.fullPath === '/login')
  const isAuthed = loadAuth()

  const handleLogout = () => {
    clearAuth()
    router.navigate({ to: '/login' })
  }

  const handleSyncFromServer = async () => {
    setSyncing(true)
    try {
      const result = await api.remoteSyncFromServer()
      const parts: string[] = []
      if (result.accounts_inserted > 0) parts.push(`${result.accounts_inserted} accounts added`)
      if (result.accounts_updated > 0) parts.push(`${result.accounts_updated} accounts updated`)
      if (result.pairings_inserted > 0) parts.push(`${result.pairings_inserted} pairings added`)
      if (parts.length > 0) {
        message.success(`Synced from server: ${parts.join(', ')}`)
      } else {
        message.info('Already in sync — no new data from server')
      }
    } catch (e: any) {
      message.error(e.message || 'Failed to sync from server')
    } finally {
      setSyncing(false)
    }
  }

  // Login page renders without the sidebar layout
  if (isLoginPage || !isAuthed) {
    return <Outlet />
  }

  // Authenticated pages render inside AppLayout
  return (
    <>
      <AppLayout
        onLogout={handleLogout}
        onSyncFromServer={handleSyncFromServer}
        syncing={syncing}
      >
        <Outlet />
      </AppLayout>
      <FloatingLogConsole />
    </>
  )
}
