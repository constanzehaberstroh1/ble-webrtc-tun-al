import React, { createContext, useContext, useState, useEffect } from 'react';
import { ConfigProvider, theme as antdTheme } from 'antd';

export const THEMES: Record<string, { name: string; color: string }> = {
  indigo:  { name: 'Indigo',  color: '#6366f1' },
  violet:  { name: 'Violet',  color: '#8b5cf6' },
  emerald: { name: 'Emerald', color: '#10b981' },
  rose:    { name: 'Rose',    color: '#f43f5e' },
  amber:   { name: 'Amber',   color: '#f59e0b' },
  cyan:    { name: 'Cyan',    color: '#06b6d4' },
};

export const MODES = ['dark', 'light'];

export type ThemeMode = 'dark' | 'light';

export interface PanelSettings {
  color: string;
  mode: ThemeMode;
  sidebarCollapsed: boolean;
  refreshInterval: number;
  historyLimit: number;
}

const defaults: PanelSettings = {
  color: 'indigo',
  mode: 'dark',
  sidebarCollapsed: false,
  refreshInterval: 5,
  historyLimit: 50,
};

interface ThemeContextType {
  settings: PanelSettings;
  update: <K extends keyof PanelSettings>(key: K, value: PanelSettings[K]) => void;
}

const ThemeContext = createContext<ThemeContextType>({
  settings: defaults,
  update: () => {},
});

export function ThemeProvider({ children }: { children: React.ReactNode }) {
  const [settings, setSettings] = useState<PanelSettings>(() => {
    try {
      const saved = localStorage.getItem('panel_settings');
      return saved ? { ...defaults, ...JSON.parse(saved) } : defaults;
    } catch {
      return defaults;
    }
  });

  useEffect(() => {
    localStorage.setItem('panel_settings', JSON.stringify(settings));
    
    // Toggle class for Tailwind dark mode so standard tailwind "dark:" variants work
    if (settings.mode === 'dark') {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
    
    document.documentElement.setAttribute('data-theme', settings.color);
    document.documentElement.setAttribute('data-mode', settings.mode);
  }, [settings]);

  const update = <K extends keyof PanelSettings>(key: K, value: PanelSettings[K]) => {
    setSettings((prev) => ({ ...prev, [key]: value }));
  };

  const currentThemeColor = THEMES[settings.color]?.color || THEMES.indigo.color;

  return (
    <ThemeContext.Provider value={{ settings, update }}>
      <ConfigProvider
        theme={{
          algorithm: settings.mode === 'dark' ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
          token: {
            colorPrimary: currentThemeColor,
            colorInfo: currentThemeColor,
            borderRadius: 8,
            fontFamily: "'Inter Variable', 'Inter', ui-sans-serif, system-ui, sans-serif",
          },
          components: {
            Layout: {
              bodyBg: 'transparent',
              headerBg: 'transparent',
            },
          },
        }}
      >
        {children}
      </ConfigProvider>
    </ThemeContext.Provider>
  );
}

export function useTheme() {
  return useContext(ThemeContext);
}
