import {
  createRouter,
  createRoute,
  createRootRoute,
  redirect,
} from '@tanstack/react-router'
import { RootLayout } from './RootLayout'
import { DashboardPage } from './components/pages/DashboardPage'
import { AccountsPage } from './components/pages/AccountsPage'
import { PairingsPage } from './components/pages/PairingsPage'
import { ConnectionsPage } from './components/pages/ConnectionsPage'
import { SettingsPage } from './components/pages/SettingsPage'
import { GuidePage } from './components/pages/GuidePage'
import { TerminalPage } from './components/pages/TerminalPage'
import { LogsPage } from './components/pages/LogsPage'
import { LoginPage } from './components/pages/LoginPage'
import { loadAuth } from './api'

// Root route with the shared layout
const rootRoute = createRootRoute({
  component: RootLayout,
})

// Login route — no layout chrome
const loginRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/login',
  component: LoginPage,
})

// Dashboard (index route)
const dashboardRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  component: DashboardPage,
  beforeLoad: () => {
    if (!loadAuth()) throw redirect({ to: '/login' })
  },
})

// Accounts
const accountsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/accounts',
  component: AccountsPage,
  beforeLoad: () => {
    if (!loadAuth()) throw redirect({ to: '/login' })
  },
})

// Pairings
const pairingsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/pairings',
  component: PairingsPage,
  beforeLoad: () => {
    if (!loadAuth()) throw redirect({ to: '/login' })
  },
})

// Connections / History
const connectionsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/connections',
  component: ConnectionsPage,
  beforeLoad: () => {
    if (!loadAuth()) throw redirect({ to: '/login' })
  },
})

// Terminal
const terminalRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/terminal',
  component: TerminalPage,
  beforeLoad: () => {
    if (!loadAuth()) throw redirect({ to: '/login' })
  },
})

// Logs
const logsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/logs',
  component: LogsPage,
  beforeLoad: () => {
    if (!loadAuth()) throw redirect({ to: '/login' })
  },
})

// Guide
const guideRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/guide',
  component: GuidePage,
  beforeLoad: () => {
    if (!loadAuth()) throw redirect({ to: '/login' })
  },
})

// Settings
const settingsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/settings',
  component: SettingsPage,
  beforeLoad: () => {
    if (!loadAuth()) throw redirect({ to: '/login' })
  },
})

// Build the route tree
const routeTree = rootRoute.addChildren([
  loginRoute,
  dashboardRoute,
  accountsRoute,
  pairingsRoute,
  connectionsRoute,
  terminalRoute,
  logsRoute,
  guideRoute,
  settingsRoute,
])

// Create the router instance
export const router = createRouter({
  routeTree,
  defaultPreload: 'intent',
})

// Type-safe router declaration
declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router
  }
}
