import { useState } from 'react'
import { NavLink, useNavigate } from 'react-router-dom'
import { useAuth } from '../context/AuthContext'
import {
  LayoutDashboard, GitCompare, CalendarClock, Settings,
  LogOut, ChevronLeft, ChevronRight, Activity, Menu, X
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
  const [mobileOpen, setMobileOpen] = useState(false)
  const { user, logout } = useAuth()
  const navigate = useNavigate()

  const showLabels = mobileOpen || !collapsed
  const handleLogout = () => { logout(); setMobileOpen(false); navigate('/login') }

  return (
    <div className="relative flex h-screen overflow-hidden">
      <div className="pointer-events-none absolute -left-20 -top-20 h-72 w-72 rounded-full bg-sky-500/15 blur-3xl" />
      <div className="pointer-events-none absolute -right-20 top-40 h-72 w-72 rounded-full bg-cyan-400/10 blur-3xl" />

      {mobileOpen && (
        <button
          aria-label="Close menu"
          onClick={() => setMobileOpen(false)}
          className="fixed inset-0 z-20 bg-black/50 md:hidden"
        />
      )}

      {/* Sidebar */}
      <aside className={clsx(
        'fixed md:relative z-30 md:z-10 inset-y-0 left-0 flex flex-col surface-blur panel-surface border-r transition-all duration-300 shrink-0',
        'shadow-[0_28px_56px_-36px_rgba(2,10,20,0.9)]',
        'w-[88vw] max-w-[360px] md:w-auto md:max-w-none',
        mobileOpen ? 'translate-x-0' : '-translate-x-full md:translate-x-0',
        collapsed ? 'md:w-28' : 'md:w-80'
      )}>
        {/* Logo */}
        <div className="flex items-center gap-3.5 px-6 py-7 border-b border-[var(--border-soft)]">
          <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-[#4f9cfb] to-[#24b5e7] flex items-center justify-center shrink-0 shadow-[0_10px_20px_-12px_rgba(79,156,251,0.9)]">
            <Activity size={20} className="text-white" />
          </div>
          {showLabels && <span className="font-semibold text-xl text-white tracking-tight">SEO Crawler</span>}
          <button
            onClick={() => setMobileOpen(false)}
            className="ml-auto text-[#9bb2d2] hover:text-white md:hidden"
            aria-label="Close sidebar"
          >
            <X size={20} />
          </button>
        </div>

        {/* Nav */}
        <nav className="flex-1 py-6 space-y-2.5 px-4">
          {nav.map(item => (
            <NavLink key={item.to} to={item.to} end={item.to === '/'}
              className={({ isActive }) => clsx(
                'flex items-center gap-3.5 px-5 py-3.5 rounded-xl text-base sm:text-lg font-medium transition-all',
                isActive
                  ? 'bg-gradient-to-r from-[#4f9cfb]/20 to-[#24b5e7]/10 text-[#d7e8ff] border border-[#5f8bc2]/40'
                  : 'text-[#9bb2d2] hover:text-white hover:bg-[#223552]/60'
              )}
              onClick={() => setMobileOpen(false)}
            >
              <item.icon size={20} className="shrink-0" />
              {showLabels && item.label}
            </NavLink>
          ))}
        </nav>

        {/* User + collapse */}
        <div className="border-t border-[var(--border-soft)] p-4 space-y-2.5">
          {showLabels && user && (
            <div className="px-3 py-3">
              <p className="text-base text-[#d0e1f9] break-words">{user.email}</p>
              <p className="text-sm text-[#7f95b7] capitalize mt-0.5">{user.role}</p>
            </div>
          )}
          <button onClick={handleLogout}
            className="flex items-center gap-3 px-5 py-3.5 w-full rounded-xl text-base sm:text-lg text-[#9bb2d2] hover:text-rose-200 hover:bg-rose-500/15 transition-all">
            <LogOut size={20} className="shrink-0" />
            {showLabels && 'Logout'}
          </button>
          <button onClick={() => setCollapsed(c => !c)}
            className="hidden md:flex items-center gap-3 px-5 py-3.5 w-full rounded-xl text-base sm:text-lg text-[#8fa7c8] hover:text-white hover:bg-[#213552]/90 transition-all">
            {collapsed ? <ChevronRight size={20} /> : <><ChevronLeft size={20} /><span>Collapse</span></>}
          </button>
        </div>
      </aside>

      {/* Main */}
      <main className="relative flex-1 overflow-y-auto">
        <div className="sticky top-0 z-20 md:hidden px-5 py-4 border-b border-[var(--border-soft)] surface-blur flex items-center justify-between">
          <button
            onClick={() => setMobileOpen(true)}
            className="inline-flex items-center gap-2 text-[#d7e8ff] bg-[#223552]/75 border border-[#446186]/40 px-4 py-2.5 rounded-lg"
          >
            <Menu size={19} />
            <span className="text-base font-medium">Menu</span>
          </button>
          <span className="text-lg font-semibold text-white">SEO Crawler</span>
        </div>
        <div className="relative z-10">{children}</div>
      </main>
    </div>
  )
}


