import clsx from 'clsx'

export function Card({ children, className }) {
  return (
    <div
      className={clsx(
        'surface-blur panel-surface rounded-2xl sm:rounded-[1.35rem]',
        'transition-all duration-300 hover:border-[var(--border-soft-2)]',
        className
      )}
    >
      {children}
    </div>
  )
}

export function Badge({ variant = 'default', children }) {
  const styles = {
    default: 'bg-[#223654]/75 text-[#d6e7ff] border border-[#4a668c]/45',
    success: 'bg-emerald-500/12 text-emerald-300 border border-emerald-300/28',
    warning: 'bg-amber-500/12 text-amber-200 border border-amber-300/28',
    danger: 'bg-rose-500/12 text-rose-200 border border-rose-300/30',
    info: 'bg-sky-500/12 text-sky-200 border border-sky-300/30',
    queued: 'bg-slate-500/16 text-slate-300 border border-slate-300/22',
    running: 'bg-sky-500/16 text-sky-100 border border-sky-300/35 pulse-glow',
  }

  return (
    <span
      className={clsx(
        'inline-flex items-center px-4 py-2 rounded-lg text-sm sm:text-base font-semibold tracking-wide',
        styles[variant] || styles.default
      )}
    >
      {children}
    </span>
  )
}

export function StatusBadge({ status }) {
  const map = {
    completed: ['success', 'Done'],
    running: ['running', 'Running'],
    queued: ['queued', 'Queued'],
    error: ['danger', 'Error'],
  }
  const [variant, label] = map[status] || ['default', status]
  return <Badge variant={variant}>{label}</Badge>
}

export function ScoreBadge({ score }) {
  if (score == null) return <span className="text-[#607a9f] text-base">--</span>
  const variant = score >= 90 ? 'success' : score >= 50 ? 'warning' : 'danger'
  return <Badge variant={variant}>{score}</Badge>
}

export function Skeleton({ className }) {
  return <div className={clsx('animate-pulse bg-[#243853] rounded-xl', className)} />
}

export function Button({ children, variant = 'primary', size = 'md', className, ...props }) {
  const variants = {
    primary: 'bg-gradient-to-r from-[#3f8ef6] to-[#38a6ef] hover:from-[#5ba2ff] hover:to-[#54b8fa] text-white shadow-[0_16px_28px_-16px_rgba(79,156,251,0.9)]',
    secondary: 'bg-[#223553]/88 hover:bg-[#2a4267] text-[#d8e6fb] border border-[#43628a]/45',
    danger: 'bg-rose-500/12 hover:bg-rose-500/20 text-rose-200 border border-rose-300/30',
    ghost: 'hover:bg-[#213352]/85 text-[#c0d2eb] hover:text-white',
  }
  const sizes = {
    sm: 'px-[1.125rem] py-2.5 text-base sm:px-5 sm:py-3',
    md: 'px-5 py-3 text-base sm:px-6 sm:py-3.5 sm:text-lg',
    lg: 'px-6 py-3.5 text-lg sm:px-7 sm:py-4 sm:text-xl',
  }

  return (
    <button
      className={clsx(
        'inline-flex items-center justify-center gap-2 rounded-xl font-medium tracking-wide transition-all duration-200 min-h-[48px] sm:min-h-[54px]',
        'focus:outline-none focus:ring-2 focus:ring-[#72b3ff]/60 focus:ring-offset-2 focus:ring-offset-[#0a1322]',
        'disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:translate-y-0 hover:-translate-y-0.5',
        variants[variant],
        sizes[size],
        className
      )}
      {...props}
    >
      {children}
    </button>
  )
}

export function Input({ className, ...props }) {
  return (
    <input
      className={clsx(
        'w-full bg-[#0d1a2e]/95 border border-[#53739b]/36 text-[#deebff] rounded-xl px-[1.125rem] sm:px-5 py-[0.85rem] sm:py-[0.95rem] text-base sm:text-lg leading-normal',
        'placeholder-[#97add0] focus:outline-none focus:border-[#72b3ff] focus:ring-2 focus:ring-[#72b3ff]/28 transition-all',
        className
      )}
      {...props}
    />
  )
}

export function Select({ className, children, ...props }) {
  return (
    <select
      className={clsx(
        'w-full bg-[#0d1a2e]/95 border border-[#53739b]/36 text-[#deebff] rounded-xl px-[1.125rem] sm:px-5 py-[0.85rem] sm:py-[0.95rem] text-base sm:text-lg leading-normal',
        'focus:outline-none focus:border-[#72b3ff] focus:ring-2 focus:ring-[#72b3ff]/28 transition-all',
        className
      )}
      {...props}
    >
      {children}
    </select>
  )
}

export function PageHeader({ title, subtitle, action }) {
  return (
    <div className="flex flex-col gap-5 sm:flex-row sm:items-start sm:justify-between mb-8 sm:mb-10 reveal-up">
      <div>
        <h1 className="text-3xl sm:text-4xl lg:text-[3rem] font-semibold text-white tracking-tight">{title}</h1>
        {subtitle && <p className="text-base sm:text-lg text-[#c2d6ef] mt-2">{subtitle}</p>}
        <div className="section-divider mt-4 max-w-72" />
      </div>
      {action && <div className="sm:self-start">{action}</div>}
    </div>
  )
}

export function StatCard({ label, value, sub, icon: Icon, color = 'blue' }) {
  const colors = {
    blue: 'text-sky-200 bg-sky-500/16 border border-sky-300/25',
    green: 'text-emerald-200 bg-emerald-500/16 border border-emerald-300/25',
    amber: 'text-amber-200 bg-amber-500/16 border border-amber-300/25',
    red: 'text-rose-200 bg-rose-500/16 border border-rose-300/25',
    slate: 'text-slate-200 bg-slate-500/16 border border-slate-300/25',
  }

  return (
    <Card className="p-5 sm:p-6 reveal-up">
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="text-sm text-[#bed1ea] uppercase tracking-[0.14em] mb-2">{label}</p>
          <p className="text-3xl sm:text-4xl font-semibold text-white">{value ?? '--'}</p>
          {sub && <p className="text-base text-[#b4c8e3] mt-2.5">{sub}</p>}
        </div>
        {Icon && (
          <div className={clsx('w-12 h-12 sm:w-16 sm:h-16 rounded-2xl flex items-center justify-center soft-float', colors[color])}>
            <Icon size={23} />
          </div>
        )}
      </div>
    </Card>
  )
}

export function Table({ headers, children, empty = 'No data' }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-base">
        <thead>
          <tr className="border-b border-[#3f5d84]/45">
            {headers.map(h => (
              <th
                key={h}
                className="text-left px-5 sm:px-7 py-[1.125rem] sm:py-6 text-xs sm:text-sm font-semibold text-[#b8cce8] uppercase tracking-[0.12em] whitespace-nowrap"
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {children || (
            <tr>
              <td colSpan={headers.length} className="px-5 sm:px-7 py-11 sm:py-14 text-center text-[#a7bfdd] text-base sm:text-xl">
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
    <div className="flex gap-2 border-b border-[#3f5d84]/45 mb-8 sm:mb-10 overflow-x-auto">
      {tabs.map(t => (
        <button
          key={t.id}
          onClick={() => onChange(t.id)}
          className={clsx(
            'px-[1.125rem] sm:px-6 py-3 sm:py-3.5 text-base sm:text-lg font-medium border-b-2 -mb-px transition-all whitespace-nowrap rounded-t-lg',
            active === t.id
              ? 'border-[#72b3ff] text-[#d9e8ff] bg-[#203351]/65 shadow-[inset_0_1px_0_rgba(182,212,255,0.25)]'
              : 'border-transparent text-[#aac0dc] hover:text-[#d8e8ff] hover:bg-[#1a2b45]/45'
          )}
        >
          {t.label}
          {t.count != null && (
            <span
              className={clsx(
                'ml-2 text-sm px-2 py-0.5 rounded-full',
                active === t.id ? 'bg-[#3a5f8b] text-[#d9e8ff]' : 'bg-[#263b58] text-[#a9bddb]'
              )}
            >
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
    <div className="flex flex-col items-center justify-center py-24 sm:py-28 text-center reveal-up">
      <div className="w-[72px] h-[72px] sm:w-[84px] sm:h-[84px] rounded-2xl border border-[#4e6f9a]/35 bg-[#1c2f4b]/75 flex items-center justify-center mb-7">
        {Icon && <Icon size={34} className="text-[#a8bedf]" />}
      </div>
      <p className="text-2xl sm:text-3xl text-white font-medium mb-2">{title}</p>
      <p className="text-base sm:text-xl text-[#b2c7e1] max-w-xl">{description}</p>
    </div>
  )
}

export function Toggle({ checked, onChange, label }) {
  return (
    <label className="flex items-center gap-4 cursor-pointer">
      <div className="relative">
        <input type="checkbox" className="sr-only" checked={checked} onChange={e => onChange(e.target.checked)} />
        <div className={clsx('w-14 h-7 rounded-full transition-all', checked ? 'bg-[#4f9cfb]' : 'bg-[#2a446a]')} />
        <div
          className={clsx(
            'absolute top-0.5 left-0.5 w-6 h-6 bg-white rounded-full shadow transition-transform',
            checked ? 'translate-x-7' : 'translate-x-0'
          )}
        />
      </div>
      {label && <span className="text-base sm:text-lg text-[#c0d4ec]">{label}</span>}
    </label>
  )
}

