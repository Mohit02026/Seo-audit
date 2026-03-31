import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getJobs, getDiff } from '../api/audits'
import { Card, Button, Select, StatCard, Table, PageHeader, EmptyState } from '../components/ui'
import { GitCompare, TrendingUp, TrendingDown } from 'lucide-react'
import clsx from 'clsx'

function Delta({ value }) {
  if (!value) return <span className="text-slate-600">0</span>
  const up = value > 0
  return (
    <span className={clsx('flex items-center gap-1 font-medium', up ? 'text-red-400' : 'text-green-400')}>
      {up ? <TrendingUp size={13} /> : <TrendingDown size={13} />}
      {up ? '+' : ''}{value}
    </span>
  )
}

const INTERESTING_KEYS = ['total_pages', 'status_4xx', 'status_5xx', 'missing_titles', 'no_meta_desc', 'broken_external_links_count', 'thin_content_pages', 'canonical_issues_count', 'orphan_pages_count']

export default function Diff() {
  const [jobA, setJobA] = useState('')
  const [jobB, setJobB] = useState('')
  const { data: jobs } = useQuery({ queryKey: ['jobs'], queryFn: () => getJobs(100) })
  const completed = jobs?.filter(j => j.status === 'completed') || []

  const { data: diff, isFetching, refetch } = useQuery({
    queryKey: ['diff', jobA, jobB],
    queryFn: () => getDiff(jobA, jobB),
    enabled: false,
  })

  const run = () => refetch()

  return (
    <div className="p-6 max-w-6xl mx-auto">
      <PageHeader title="Audit Diff" subtitle="Compare two completed audits to surface changes" />

      <Card className="p-5 mb-6">
        <div className="flex gap-4 items-end">
          <div className="flex-1">
            <label className="text-xs font-medium text-slate-400 block mb-1.5">Audit A (baseline)</label>
            <Select value={jobA} onChange={e => setJobA(e.target.value)}>
              <option value="">Select audit…</option>
              {completed.map(j => <option key={j.job_id} value={j.job_id}>{j.url} — {j.created_at?.slice(0, 10)}</option>)}
            </Select>
          </div>
          <div className="text-slate-600"><GitCompare size={18} /></div>
          <div className="flex-1">
            <label className="text-xs font-medium text-slate-400 block mb-1.5">Audit B (current)</label>
            <Select value={jobB} onChange={e => setJobB(e.target.value)}>
              <option value="">Select audit…</option>
              {completed.map(j => <option key={j.job_id} value={j.job_id}>{j.url} — {j.created_at?.slice(0, 10)}</option>)}
            </Select>
          </div>
          <Button onClick={run} disabled={!jobA || !jobB || isFetching}>
            {isFetching ? 'Comparing…' : 'Compare'}
          </Button>
        </div>
      </Card>

      {!diff ? (
        <EmptyState icon={GitCompare} title="Select two audits to compare" description="Choose a baseline and a current audit to see what changed between them." />
      ) : diff.error ? (
        <Card className="p-5 text-red-400 text-sm">{diff.error}</Card>
      ) : (
        <div className="space-y-6">
          {/* Summary delta cards */}
          <div className="grid grid-cols-3 md:grid-cols-5 gap-3">
            {INTERESTING_KEYS.filter(k => diff.summary_delta?.[k] != null).slice(0, 5).map(k => (
              <Card key={k} className="p-4">
                <p className="text-xs text-slate-500 uppercase tracking-wider mb-1">{k.replace(/_/g, ' ')}</p>
                <Delta value={diff.summary_delta[k]} />
              </Card>
            ))}
          </div>

          <div className="grid md:grid-cols-2 gap-4">
            {/* New pages */}
            {diff.new_pages?.length > 0 && (
              <Card className="p-5">
                <h3 className="text-sm font-medium text-green-400 mb-3">+ {diff.new_pages.length} New Pages</h3>
                <div className="space-y-1 max-h-48 overflow-y-auto">
                  {diff.new_pages.map(u => <p key={u} className="font-mono text-xs text-slate-400 truncate">{u}</p>)}
                </div>
              </Card>
            )}
            {/* Removed pages */}
            {diff.removed_pages?.length > 0 && (
              <Card className="p-5">
                <h3 className="text-sm font-medium text-red-400 mb-3">− {diff.removed_pages.length} Removed Pages</h3>
                <div className="space-y-1 max-h-48 overflow-y-auto">
                  {diff.removed_pages.map(u => <p key={u} className="font-mono text-xs text-slate-400 truncate">{u}</p>)}
                </div>
              </Card>
            )}
          </div>

          {/* Status changes */}
          {diff.status_changes?.length > 0 && (
            <Card>
              <div className="p-4 border-b border-[#334155]">
                <h3 className="text-sm font-medium text-white">Status Changes ({diff.status_changes.length})</h3>
              </div>
              <Table headers={['URL', 'Before', 'After']}>
                {diff.status_changes.map(r => (
                  <tr key={r.url} className="border-b border-[#334155]/50 hover:bg-[#263348]">
                    <td className="px-4 py-2.5 font-mono text-xs text-blue-400 max-w-[300px] truncate">{r.url}</td>
                    <td className="px-4 py-2.5 text-slate-400 font-mono">{r.before}</td>
                    <td className="px-4 py-2.5 font-mono font-medium" style={{ color: r.after >= 400 ? '#EF4444' : '#22C55E' }}>{r.after}</td>
                  </tr>
                ))}
              </Table>
            </Card>
          )}

          {/* Title changes */}
          {diff.title_changes?.length > 0 && (
            <Card>
              <div className="p-4 border-b border-[#334155]">
                <h3 className="text-sm font-medium text-white">Title Changes ({diff.title_changes.length})</h3>
              </div>
              <Table headers={['URL', 'Before', 'After']}>
                {diff.title_changes.map(r => (
                  <tr key={r.url} className="border-b border-[#334155]/50 hover:bg-[#263348]">
                    <td className="px-4 py-2.5 font-mono text-xs text-blue-400 max-w-[200px] truncate">{r.url}</td>
                    <td className="px-4 py-2.5 text-slate-500 text-xs max-w-[200px] truncate">{r.before || <em>empty</em>}</td>
                    <td className="px-4 py-2.5 text-slate-300 text-xs max-w-[200px] truncate">{r.after || <em>empty</em>}</td>
                  </tr>
                ))}
              </Table>
            </Card>
          )}

          {/* Score delta */}
          {diff.score_delta?.length > 0 && (
            <Card>
              <div className="p-4 border-b border-[#334155]">
                <h3 className="text-sm font-medium text-white">PageSpeed Score Changes</h3>
              </div>
              <Table headers={['URL', 'Before', 'After', 'Change']}>
                {diff.score_delta.map(r => (
                  <tr key={r.url} className="border-b border-[#334155]/50 hover:bg-[#263348]">
                    <td className="px-4 py-2.5 font-mono text-xs text-blue-400 max-w-[300px] truncate">{r.url}</td>
                    <td className="px-4 py-2.5 text-slate-400 font-mono">{r.before}</td>
                    <td className="px-4 py-2.5 font-mono font-medium" style={{ color: r.after >= r.before ? '#22C55E' : '#EF4444' }}>{r.after}</td>
                    <td className="px-4 py-2.5"><Delta value={r.delta} /></td>
                  </tr>
                ))}
              </Table>
            </Card>
          )}
        </div>
      )}
    </div>
  )
}
