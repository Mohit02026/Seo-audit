import { useState } from 'react'
import { NavLink, useNavigate } from 'react-router-dom'
import { useAuth } from '../context/AuthContext'
import {
  LayoutDashboard, GitCompare, CalendarClock, Settings,
  LogOut, ChevronLeft, ChevronRight, Activity
} from 'lucide-react'
import clsx from 'clsx'

const nav = [
  { to: '/', icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/diff', icon: GitCompare, label: 'Diff' },
  { to: '/schedules', icon: CalendarClock, label: 'Schedules' },
  { to: '/settings/gsc', icon: Settings, label: 'GSC Connect' },
]

export default function Layout({ children }) {
  const [collapsed, setCollapsed] = useState(false)
  const { user, logout } = useAuth()
  const navigate = useNavigate()

  const handleLogout = () => { logout(); navigate('/login') }

  return (
    <div className="flex h-screen overflow-hidden">
      {/* Sidebar */}
      <aside className={clsx(
        'flex flex-col bg-[#1E293B] border-r border-[#334155] transition-all duration-200 shrink-0',
        collapsed ? 'w-16' : 'w-56'
      )}>
        {/* Logo */}
        <div className="flex items-center gap-3 px-4 py-5 border-b border-[#334155]">
          <div className="w-8 h-8 rounded-lg bg-blue-500 flex items-center justify-center shrink-0">
            <Activity size={16} className="text-white" />
          </div>
          {!collapsed && <span className="font-semibold text-sm text-white tracking-tight">SEO Crawler</span>}
        </div>

        {/* Nav */}
        <nav className="flex-1 py-4 space-y-1 px-2">
          {nav.map(({ to, icon: Icon, label }) => (
            <NavLink key={to} to={to} end={to === '/'}
              className={({ isActive }) => clsx(
                'flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors',
                isActive
                  ? 'bg-blue-500/10 text-blue-400'
                  : 'text-slate-400 hover:text-white hover:bg-[#263348]'
              )}>
              <Icon size={18} className="shrink-0" />
              {!collapsed && label}
            </NavLink>
          ))}
        </nav>

        {/* User + collapse */}
        <div className="border-t border-[#334155] p-2 space-y-1">
          {!collapsed && user && (
            <div className="px-3 py-2">
              <p className="text-xs text-slate-400 truncate">{user.email}</p>
              <p className="text-xs text-slate-600 capitalize">{user.role}</p>
            </div>
          )}
          <button onClick={handleLogout}
            className="flex items-center gap-3 px-3 py-2.5 w-full rounded-lg text-sm text-slate-400 hover:text-red-400 hover:bg-red-500/10 transition-colors">
            <LogOut size={18} className="shrink-0" />
            {!collapsed && 'Logout'}
          </button>
          <button onClick={() => setCollapsed(c => !c)}
            className="flex items-center gap-3 px-3 py-2.5 w-full rounded-lg text-sm text-slate-500 hover:text-white hover:bg-[#263348] transition-colors">
            {collapsed ? <ChevronRight size={18} /> : <><ChevronLeft size={18} /><span>Collapse</span></>}
          </button>
        </div>
      </aside>

      {/* Main */}
      <main className="flex-1 overflow-y-auto bg-[#0F172A]">
        {children}
      </main>
    </div>
  )
}
