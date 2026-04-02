import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getJobs, startAudit } from '../api/audits'
import { Card, StatusBadge, ScoreBadge, Button, Input, Select, Toggle, PageHeader, Skeleton, EmptyState, StatCard } from '../components/ui'
import { Plus, ExternalLink, ChevronRight, LayoutDashboard, Loader2, X, Activity, Clock3, Gauge } from 'lucide-react'

function NewAuditModal({ onClose }) {
  const [url, setUrl] = useState('https://')
  const [lead, setLead] = useState('')
  const [maxPages, setMaxPages] = useState(200)
  const [jsRender, setJsRender] = useState(true)
  const qc = useQueryClient()
  const navigate = useNavigate()

  const { mutate, isPending, error } = useMutation({
    mutationFn: () => startAudit({ url, lead_name: lead, max_pages: maxPages, js_render: jsRender }),
    onSuccess: data => { qc.invalidateQueries({ queryKey: ['jobs'] }); navigate(`/audit/${data.job_id}`); onClose() },
  })

  return (
    <div className="fixed inset-0 bg-black/65 backdrop-blur-sm flex items-center justify-center z-50 p-4">
      <div className="surface-blur panel-surface rounded-2xl p-6 sm:p-8 w-full max-w-2xl shadow-[0_30px_60px_-30px_rgba(2,10,20,0.95)] reveal-up">
        <div className="flex items-center justify-between mb-5">
          <h3 className="text-2xl font-semibold text-white">New Audit</h3>
          <button onClick={onClose} className="text-[#8fa7c8] hover:text-white transition-colors"><X size={18} /></button>
        </div>

        {error && (
          <div className="bg-rose-500/10 border border-rose-400/30 text-rose-200 text-sm rounded-xl px-4 py-3 mb-4">
            {error.response?.data?.detail || 'Something went wrong'}
          </div>
        )}

        <div className="space-y-5">
          <div>
            <label className="text-base font-medium text-[#afc4e2] block mb-2">Website URL</label>
            <Input value={url} onChange={e => setUrl(e.target.value)} placeholder="https://example.com" />
          </div>
          <div>
            <label className="text-base font-medium text-[#afc4e2] block mb-2">Lead / Client Name</label>
            <Input value={lead} onChange={e => setLead(e.target.value)} placeholder="Acme Corp" />
          </div>
          <div>
            <label className="text-base font-medium text-[#afc4e2] block mb-2">Max Pages</label>
            <Select value={maxPages} onChange={e => setMaxPages(Number(e.target.value))}>
              {[50, 100, 200, 500].map(n => <option key={n} value={n}>{n} pages</option>)}
            </Select>
          </div>
          <Toggle checked={jsRender} onChange={setJsRender} label="JS rendering (Playwright)" />
        </div>

        <div className="flex flex-col sm:flex-row gap-3 mt-6">
          <Button variant="secondary" className="flex-1" onClick={onClose}>Cancel</Button>
          <Button className="flex-1" onClick={() => mutate()} disabled={isPending || !url}>
            {isPending ? <><Loader2 size={14} className="animate-spin" /> Starting...</> : 'Start Audit'}
          </Button>
        </div>
      </div>
    </div>
  )
}

function avgScore(result) {
  if (!result?.pagespeed_sample?.length) return null
  const scores = result.pagespeed_sample.map(p => p.performance_score).filter(s => s != null)
  if (!scores.length) return null
  return Math.round(scores.reduce((a, b) => a + b, 0) / scores.length)
}

export default function Dashboard() {
  const [showModal, setShowModal] = useState(false)
  const navigate = useNavigate()
  const { data: jobs, isLoading } = useQuery({ queryKey: ['jobs'], queryFn: () => getJobs(100), refetchInterval: 5000 })
  const completed = jobs?.filter(j => j.status === 'completed') || []
  const running = jobs?.filter(j => j.status === 'running' || j.status === 'queued') || []
  const scoredCompleted = completed.map(j => avgScore(j.result)).filter(s => s != null)
  const completedAvg = scoredCompleted.length
    ? Math.round(scoredCompleted.reduce((a, b) => a + b, 0) / scoredCompleted.length)
    : null

  return (
    <div className="px-4 py-5 sm:px-7 sm:py-8 lg:px-10 lg:py-10 max-w-[90rem] mx-auto">
      <PageHeader
        title="Dashboard"
        subtitle={`${jobs?.length ?? 0} audits`}
        action={(
          <Button onClick={() => setShowModal(true)} className="w-full sm:w-auto">
            <Plus size={16} /> New Audit
          </Button>
        )}
      />

      {isLoading ? (
        <div className="space-y-3">
          {[...Array(5)].map((_, i) => <Skeleton key={i} className="h-14 w-full" />)}
        </div>
      ) : !jobs?.length ? (
        <EmptyState icon={LayoutDashboard} title="No audits yet" description="Start your first audit to analyze a website's SEO health." />
      ) : (
        <div className="space-y-6">
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-5">
            <StatCard label="Total Audits" value={jobs.length} icon={Activity} color="blue" />
            <StatCard label="Running Now" value={running.length} icon={Clock3} color="amber" />
            <StatCard label="Avg Score" value={completedAvg ?? '--'} icon={Gauge} color="green" />
          </div>

          <Card className="reveal-up">
            <div className="px-6 sm:px-8 py-5 sm:py-6 border-b border-[var(--border-soft)]">
              <h3 className="text-xl sm:text-2xl font-semibold text-white">Recent Audits</h3>
              <p className="text-base text-[#c1d5ee] mt-1.5">Latest crawls and current run status</p>
            </div>

            <div className="hidden md:block overflow-x-auto">
              <table className="w-full text-[1.05rem]">
                <thead>
                  <tr className="border-b border-[#3f5d84]/45">
                    {['URL', 'Lead', 'Status', 'Score', 'Pages', 'Date', ''].map(h => (
                      <th key={h} className="text-left px-5 sm:px-7 py-4 sm:py-5 text-xs sm:text-sm font-semibold text-[#91a9ca] uppercase tracking-[0.12em] whitespace-nowrap">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {jobs.map(job => {
                    const score = avgScore(job.result)
                    const pages = job.result?.crawl_summary?.total_pages
                    return (
                      <tr
                        key={job.job_id}
                        className="border-b border-[#3f5d84]/30 hover:bg-[#1d304c]/75 cursor-pointer transition-colors"
                        onClick={() => navigate(`/audit/${job.job_id}`)}
                      >
                        <td className="px-5 sm:px-7 py-5">
                          <div className="flex items-center gap-2">
                            <span className="font-mono text-base text-[#a8d1ff] break-words whitespace-normal">{job.url}</span>
                            <ExternalLink size={12} className="text-[#7189ab] shrink-0" />
                          </div>
                        </td>
                        <td className="px-5 sm:px-7 py-5 text-[#c0d2eb]">{job.lead_name || '--'}</td>
                        <td className="px-5 sm:px-7 py-5"><StatusBadge status={job.status} /></td>
                        <td className="px-5 sm:px-7 py-5"><ScoreBadge score={score} /></td>
                        <td className="px-5 sm:px-7 py-5 text-[#c0d2eb]">{pages ?? '--'}</td>
                        <td className="px-5 sm:px-7 py-5 text-[#9ab1d1] whitespace-nowrap">{job.created_at?.slice(0, 10)}</td>
                        <td className="px-5 sm:px-7 py-5"><ChevronRight size={19} className="text-[#7e95b8]" /></td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>

            <div className="md:hidden p-5 space-y-4">
              {jobs.map(job => {
                const score = avgScore(job.result)
                const pages = job.result?.crawl_summary?.total_pages
                return (
                  <button
                    key={job.job_id}
                    onClick={() => navigate(`/audit/${job.job_id}`)}
                    className="w-full text-left rounded-2xl border border-[var(--border-soft)] bg-[#17283f]/55 p-5 space-y-3.5 hover:border-[var(--border-soft-2)] transition-colors"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <p className="font-mono text-base text-[#8fc0ff] break-words">{job.url}</p>
                      <ChevronRight size={17} className="text-[#8aa2c5] shrink-0" />
                    </div>
                    <div className="flex items-center justify-between text-base">
                      <span className="text-[#9bb3d4]">{job.lead_name || '--'}</span>
                      <StatusBadge status={job.status} />
                    </div>
                    <div className="flex items-center justify-between text-base">
                      <span className="text-[#9bb3d4]">Pages: {pages ?? '--'}</span>
                      <ScoreBadge score={score} />
                    </div>
                    <p className="text-sm text-[#839cc0]">{job.created_at?.slice(0, 10)}</p>
                  </button>
                )
              })}
            </div>
          </Card>
        </div>
      )}

      {showModal && <NewAuditModal onClose={() => setShowModal(false)} />}
    </div>
  )
}






