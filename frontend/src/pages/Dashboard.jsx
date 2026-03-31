import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getJobs, startAudit } from '../api/audits'
import { Card, StatusBadge, ScoreBadge, Button, Input, Select, Toggle, PageHeader, Skeleton, EmptyState } from '../components/ui'
import { Plus, ExternalLink, ChevronRight, LayoutDashboard, Loader2, X } from 'lucide-react'
import { useAuth } from '../context/AuthContext'

function NewAuditModal({ onClose }) {
  const [url, setUrl] = useState('https://')
  const [lead, setLead] = useState('')
  const [maxPages, setMaxPages] = useState(200)
  const [jsRender, setJsRender] = useState(true)
  const qc = useQueryClient()
  const navigate = useNavigate()

  const { mutate, isPending, error } = useMutation({
    mutationFn: () => startAudit({ url, lead_name: lead, max_pages: maxPages, js_render: jsRender }),
    onSuccess: data => { qc.invalidateQueries(['jobs']); navigate(`/audit/${data.job_id}`); onClose() },
  })

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50 p-4">
      <div className="bg-[#1E293B] border border-[#334155] rounded-2xl p-6 w-full max-w-md shadow-2xl">
        <div className="flex items-center justify-between mb-5">
          <h3 className="text-base font-semibold text-white">New Audit</h3>
          <button onClick={onClose} className="text-slate-500 hover:text-white transition-colors"><X size={18} /></button>
        </div>

        {error && (
          <div className="bg-red-500/10 border border-red-500/20 text-red-400 text-sm rounded-lg px-4 py-3 mb-4">
            {error.response?.data?.detail || 'Something went wrong'}
          </div>
        )}

        <div className="space-y-4">
          <div>
            <label className="text-xs font-medium text-slate-400 block mb-1.5">Website URL</label>
            <Input value={url} onChange={e => setUrl(e.target.value)} placeholder="https://example.com" />
          </div>
          <div>
            <label className="text-xs font-medium text-slate-400 block mb-1.5">Lead / Client Name</label>
            <Input value={lead} onChange={e => setLead(e.target.value)} placeholder="Acme Corp" />
          </div>
          <div>
            <label className="text-xs font-medium text-slate-400 block mb-1.5">Max Pages</label>
            <Select value={maxPages} onChange={e => setMaxPages(Number(e.target.value))}>
              {[50, 100, 200, 500].map(n => <option key={n} value={n}>{n} pages</option>)}
            </Select>
          </div>
          <Toggle checked={jsRender} onChange={setJsRender} label="JS rendering (Playwright)" />
        </div>

        <div className="flex gap-3 mt-6">
          <Button variant="secondary" className="flex-1" onClick={onClose}>Cancel</Button>
          <Button className="flex-1" onClick={() => mutate()} disabled={isPending || !url}>
            {isPending ? <><Loader2 size={14} className="animate-spin" /> Starting…</> : 'Start Audit'}
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
  const { user } = useAuth()
  const navigate = useNavigate()
  const { data: jobs, isLoading } = useQuery({ queryKey: ['jobs'], queryFn: () => getJobs(100), refetchInterval: 5000 })

  return (
    <div className="p-6 max-w-6xl mx-auto">
      <PageHeader
        title="Dashboard"
        subtitle={`${jobs?.length ?? 0} audits`}
        action={
          <Button onClick={() => setShowModal(true)}>
            <Plus size={16} /> New Audit
          </Button>
        }
      />

      {isLoading ? (
        <div className="space-y-3">
          {[...Array(5)].map((_, i) => <Skeleton key={i} className="h-14 w-full" />)}
        </div>
      ) : !jobs?.length ? (
        <EmptyState icon={LayoutDashboard} title="No audits yet" description="Start your first audit to analyse a website's SEO health." />
      ) : (
        <Card>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-[#334155]">
                  {['URL', 'Lead', 'Status', 'Score', 'Pages', 'Date', ''].map(h => (
                    <th key={h} className="text-left px-4 py-3 text-xs font-medium text-slate-500 uppercase tracking-wider whitespace-nowrap">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {jobs.map(job => {
                  const score = avgScore(job.result)
                  const pages = job.result?.crawl_summary?.total_pages
                  return (
                    <tr key={job.job_id}
                      className="border-b border-[#334155]/50 hover:bg-[#263348] cursor-pointer transition-colors"
                      onClick={() => navigate(`/audit/${job.job_id}`)}>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2">
                          <span className="font-mono text-xs text-blue-400 max-w-[220px] truncate">{job.url}</span>
                          <ExternalLink size={12} className="text-slate-600 shrink-0" />
                        </div>
                      </td>
                      <td className="px-4 py-3 text-slate-400">{job.lead_name || '—'}</td>
                      <td className="px-4 py-3"><StatusBadge status={job.status} /></td>
                      <td className="px-4 py-3"><ScoreBadge score={score} /></td>
                      <td className="px-4 py-3 text-slate-400">{pages ?? '—'}</td>
                      <td className="px-4 py-3 text-slate-500 text-xs whitespace-nowrap">{job.created_at?.slice(0, 10)}</td>
                      <td className="px-4 py-3"><ChevronRight size={16} className="text-slate-600" /></td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </Card>
      )}

      {showModal && <NewAuditModal onClose={() => setShowModal(false)} />}
    </div>
  )
}
