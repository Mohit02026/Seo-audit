import clsx from 'clsx'

export function Card({ children, className }) {
  return (
    <div className={clsx('bg-[#1E293B] border border-[#334155] rounded-xl', className)}>
      {children}
    </div>
  )
}

export function Badge({ variant = 'default', children }) {
  const styles = {
    default: 'bg-slate-700 text-slate-300',
    success: 'bg-green-500/15 text-green-400 border border-green-500/20',
    warning: 'bg-amber-500/15 text-amber-400 border border-amber-500/20',
    danger: 'bg-red-500/15 text-red-400 border border-red-500/20',
    info: 'bg-blue-500/15 text-blue-400 border border-blue-500/20',
    queued: 'bg-slate-500/15 text-slate-400 border border-slate-500/20',
    running: 'bg-blue-500/15 text-blue-400 border border-blue-500/20 animate-pulse',
  }
  return (
    <span className={clsx('inline-flex items-center px-2 py-0.5 rounded-md text-xs font-medium', styles[variant] || styles.default)}>
      {children}
    </span>
  )
}

export function StatusBadge({ status }) {
  const map = {
    completed: ['success', '✓ Done'],
    running: ['running', '⟳ Running'],
    queued: ['queued', '· Queued'],
    error: ['danger', '✕ Error'],
  }
  const [variant, label] = map[status] || ['default', status]
  return <Badge variant={variant}>{label}</Badge>
}

export function ScoreBadge({ score }) {
  if (score == null) return <span className="text-slate-600 text-xs">—</span>
  const variant = score >= 90 ? 'success' : score >= 50 ? 'warning' : 'danger'
  return <Badge variant={variant}>{score}</Badge>
}

export function Skeleton({ className }) {
  return <div className={clsx('animate-pulse bg-[#334155] rounded', className)} />
}

export function Button({ children, variant = 'primary', size = 'md', className, ...props }) {
  const variants = {
    primary: 'bg-blue-500 hover:bg-blue-600 text-white',
    secondary: 'bg-[#334155] hover:bg-[#3d4f66] text-slate-200',
    danger: 'bg-red-500/10 hover:bg-red-500/20 text-red-400 border border-red-500/20',
    ghost: 'hover:bg-[#263348] text-slate-400 hover:text-white',
  }
  const sizes = {
    sm: 'px-3 py-1.5 text-xs',
    md: 'px-4 py-2 text-sm',
    lg: 'px-5 py-2.5 text-sm',
  }
  return (
    <button className={clsx(
      'inline-flex items-center gap-2 rounded-lg font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed',
      variants[variant], sizes[size], className
    )} {...props}>
      {children}
    </button>
  )
}

export function Input({ className, ...props }) {
  return (
    <input className={clsx(
      'w-full bg-[#0F172A] border border-[#334155] text-slate-200 rounded-lg px-3 py-2 text-sm',
      'placeholder-slate-600 focus:outline-none focus:border-blue-500 transition-colors',
      className
    )} {...props} />
  )
}

export function Select({ className, children, ...props }) {
  return (
    <select className={clsx(
      'w-full bg-[#0F172A] border border-[#334155] text-slate-200 rounded-lg px-3 py-2 text-sm',
      'focus:outline-none focus:border-blue-500 transition-colors',
      className
    )} {...props}>
      {children}
    </select>
  )
}

export function PageHeader({ title, subtitle, action }) {
  return (
    <div className="flex items-start justify-between mb-6">
      <div>
        <h1 className="text-xl font-semibold text-white">{title}</h1>
        {subtitle && <p className="text-sm text-slate-500 mt-0.5">{subtitle}</p>}
      </div>
      {action && <div>{action}</div>}
    </div>
  )
}

export function StatCard({ label, value, sub, icon: Icon, color = 'blue' }) {
  const colors = {
    blue: 'text-blue-400 bg-blue-500/10',
    green: 'text-green-400 bg-green-500/10',
    amber: 'text-amber-400 bg-amber-500/10',
    red: 'text-red-400 bg-red-500/10',
    slate: 'text-slate-400 bg-slate-500/10',
  }
  return (
    <Card className="p-5">
      <div className="flex items-start justify-between">
        <div>
          <p className="text-xs text-slate-500 uppercase tracking-wider mb-1">{label}</p>
          <p className="text-2xl font-semibold text-white">{value ?? '—'}</p>
          {sub && <p className="text-xs text-slate-500 mt-1">{sub}</p>}
        </div>
        {Icon && (
          <div className={clsx('w-9 h-9 rounded-lg flex items-center justify-center', colors[color])}>
            <Icon size={18} />
          </div>
        )}
      </div>
    </Card>
  )
}

export function Table({ headers, children, empty = 'No data' }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-[#334155]">
            {headers.map(h => (
              <th key={h} className="text-left px-4 py-3 text-xs font-medium text-slate-500 uppercase tracking-wider whitespace-nowrap">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {children || (
            <tr>
              <td colSpan={headers.length} className="px-4 py-8 text-center text-slate-600 text-sm">
                {empty}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  )
}

export function Tabs({ tabs, active, onChange }) {
  return (
    <div className="flex gap-1 border-b border-[#334155] mb-6">
      {tabs.map(t => (
        <button key={t.id} onClick={() => onChange(t.id)}
          className={clsx(
            'px-4 py-2.5 text-sm font-medium border-b-2 -mb-px transition-colors',
            active === t.id
              ? 'border-blue-500 text-blue-400'
              : 'border-transparent text-slate-500 hover:text-slate-300'
          )}>
          {t.label}
          {t.count != null && (
            <span className={clsx('ml-2 text-xs px-1.5 py-0.5 rounded-full',
              active === t.id ? 'bg-blue-500/20' : 'bg-[#334155]')}>
              {t.count}
            </span>
          )}
        </button>
      ))}
    </div>
  )
}

export function EmptyState({ icon: Icon, title, description }) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <div className="w-14 h-14 rounded-2xl bg-[#334155] flex items-center justify-center mb-4">
        {Icon && <Icon size={24} className="text-slate-500" />}
      </div>
      <p className="text-white font-medium mb-1">{title}</p>
      <p className="text-sm text-slate-500 max-w-xs">{description}</p>
    </div>
  )
}

export function Toggle({ checked, onChange, label }) {
  return (
    <label className="flex items-center gap-2 cursor-pointer">
      <div className="relative">
        <input type="checkbox" className="sr-only" checked={checked} onChange={e => onChange(e.target.checked)} />
        <div className={clsx('w-9 h-5 rounded-full transition-colors', checked ? 'bg-blue-500' : 'bg-[#334155]')} />
        <div className={clsx('absolute top-0.5 left-0.5 w-4 h-4 bg-white rounded-full shadow transition-transform',
          checked ? 'translate-x-4' : 'translate-x-0')} />
      </div>
      {label && <span className="text-sm text-slate-400">{label}</span>}
    </label>
  )
}
