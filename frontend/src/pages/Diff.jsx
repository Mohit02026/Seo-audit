import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getJobs, getDiff } from '../api/audits'
import { Card, Button, Select, Table, PageHeader, EmptyState } from '../components/ui'
import { GitCompare, TrendingUp, TrendingDown } from 'lucide-react'
import clsx from 'clsx'

function Delta({ value }) {
  if (!value) return <span className="text-[#7f95b8]">0</span>
  const up = value > 0
  return (
    <span className={clsx('flex items-center gap-1 font-medium', up ? 'text-rose-300' : 'text-emerald-300')}>
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
    <div className="px-4 py-5 sm:px-7 sm:py-8 lg:px-10 lg:py-10 max-w-[90rem] mx-auto">
      <PageHeader title="Audit Diff" subtitle="Compare two completed audits to surface changes" />

      <Card className="p-6 sm:p-8 mb-8 reveal-up">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end">
          <div className="flex-1">
            <label className="text-base font-medium text-[#afc4e2] block mb-2">Audit A (baseline)</label>
            <Select value={jobA} onChange={e => setJobA(e.target.value)}>
              <option value="">Select audit...</option>
              {completed.map(j => <option key={j.job_id} value={j.job_id}>{j.url} - {j.created_at?.slice(0, 10)}</option>)}
            </Select>
          </div>
          <div className="text-[#7f95b8] self-center"><GitCompare size={20} /></div>
          <div className="flex-1">
            <label className="text-base font-medium text-[#afc4e2] block mb-2">Audit B (current)</label>
            <Select value={jobB} onChange={e => setJobB(e.target.value)}>
              <option value="">Select audit...</option>
              {completed.map(j => <option key={j.job_id} value={j.job_id}>{j.url} - {j.created_at?.slice(0, 10)}</option>)}
            </Select>
          </div>
          <Button onClick={run} disabled={!jobA || !jobB || isFetching} className="w-full lg:w-auto">
            {isFetching ? 'Comparing...' : 'Compare'}
          </Button>
        </div>
      </Card>

      {!diff ? (
        <EmptyState icon={GitCompare} title="Select two audits to compare" description="Choose a baseline and a current audit to see what changed between them." />
      ) : diff.error ? (
        <Card className="p-5 text-rose-300 text-sm">{diff.error}</Card>
      ) : (
        <div className="space-y-6">
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4 reveal-up">
            {INTERESTING_KEYS.filter(k => diff.summary_delta?.[k] != null).slice(0, 5).map(k => (
              <Card key={k} className="p-5 sm:p-6">
                <p className="text-xs sm:text-sm text-[#91a9ca] uppercase tracking-[0.12em] mb-2">{k.replace(/_/g, ' ')}</p>
                <Delta value={diff.summary_delta[k]} />
              </Card>
            ))}
          </div>

          <div className="grid md:grid-cols-2 gap-5 reveal-up">
            {diff.new_pages?.length > 0 && (
              <Card className="p-6 sm:p-7">
                <h3 className="text-lg font-medium text-emerald-300 mb-3.5">+ {diff.new_pages.length} New Pages</h3>
                <div className="space-y-1.5 max-h-56 overflow-y-auto">
                  {diff.new_pages.map(u => <p key={u} className="font-mono text-base text-[#c0d2eb] break-words">{u}</p>)}
                </div>
              </Card>
            )}
            {diff.removed_pages?.length > 0 && (
              <Card className="p-6 sm:p-7">
                <h3 className="text-lg font-medium text-rose-300 mb-3.5">- {diff.removed_pages.length} Removed Pages</h3>
                <div className="space-y-1.5 max-h-56 overflow-y-auto">
                  {diff.removed_pages.map(u => <p key={u} className="font-mono text-base text-[#c0d2eb] break-words">{u}</p>)}
                </div>
              </Card>
            )}
          </div>

          {diff.status_changes?.length > 0 && (
            <Card className="reveal-up">
              <div className="p-5 sm:p-6 border-b border-[var(--border-soft)]">
                <h3 className="text-xl font-medium text-white">Status Changes ({diff.status_changes.length})</h3>
              </div>
              <div className="hidden md:block">
                <Table headers={['URL', 'Before', 'After']}>
                  {diff.status_changes.map(r => (
                    <tr key={r.url} className="border-b border-[#3f5d84]/30 hover:bg-[#1d304c]/75">
                      <td className="px-5 sm:px-7 py-[1.125rem] font-mono text-base text-[#a8d1ff] break-words whitespace-normal">{r.url}</td>
                      <td className="px-5 sm:px-7 py-[1.125rem] text-[#b2c5e1] font-mono">{r.before}</td>
                      <td className="px-5 sm:px-7 py-[1.125rem] font-mono font-medium" style={{ color: r.after >= 400 ? '#fb6f7a' : '#31d49f' }}>{r.after}</td>
                    </tr>
                  ))}
                </Table>
              </div>
              <div className="md:hidden p-5 space-y-4">
                {diff.status_changes.map(r => (
                  <div key={r.url} className="rounded-2xl border border-[var(--border-soft)] bg-[#17283f]/55 p-5 space-y-2.5">
                    <p className="font-mono text-base text-[#8fc0ff] break-words">{r.url}</p>
                    <div className="text-base text-[#b2c5e1]">Before: <span className="font-mono">{r.before}</span></div>
                    <div className="text-base">After: <span className="font-mono font-medium" style={{ color: r.after >= 400 ? '#fb6f7a' : '#31d49f' }}>{r.after}</span></div>
                  </div>
                ))}
              </div>
            </Card>
          )}

          {diff.title_changes?.length > 0 && (
            <Card className="reveal-up">
              <div className="p-5 sm:p-6 border-b border-[var(--border-soft)]">
                <h3 className="text-xl font-medium text-white">Title Changes ({diff.title_changes.length})</h3>
              </div>
              <div className="hidden md:block">
                <Table headers={['URL', 'Before', 'After']}>
                  {diff.title_changes.map(r => (
                    <tr key={r.url} className="border-b border-[#3f5d84]/30 hover:bg-[#1d304c]/75">
                      <td className="px-5 sm:px-7 py-[1.125rem] font-mono text-base text-[#a8d1ff] break-words whitespace-normal">{r.url}</td>
                      <td className="px-5 sm:px-7 py-[1.125rem] text-[#c2d6ef] break-words whitespace-normal">{r.before || <em>empty</em>}</td>
                      <td className="px-5 sm:px-7 py-[1.125rem] text-[#e2ecfb] break-words whitespace-normal">{r.after || <em>empty</em>}</td>
                    </tr>
                  ))}
                </Table>
              </div>
              <div className="md:hidden p-5 space-y-4">
                {diff.title_changes.map(r => (
                  <div key={r.url} className="rounded-2xl border border-[var(--border-soft)] bg-[#17283f]/55 p-5 space-y-2.5">
                    <p className="font-mono text-base text-[#8fc0ff] break-words">{r.url}</p>
                    <p className="text-base text-[#9eb3d2]">Before: {r.before || <em>empty</em>}</p>
                    <p className="text-base text-[#d6e2f5]">After: {r.after || <em>empty</em>}</p>
                  </div>
                ))}
              </div>
            </Card>
          )}

          {diff.score_delta?.length > 0 && (
            <Card className="reveal-up">
              <div className="p-5 sm:p-6 border-b border-[var(--border-soft)]">
                <h3 className="text-xl font-medium text-white">PageSpeed Score Changes</h3>
              </div>
              <div className="hidden md:block">
                <Table headers={['URL', 'Before', 'After', 'Change']}>
                  {diff.score_delta.map(r => (
                    <tr key={r.url} className="border-b border-[#3f5d84]/30 hover:bg-[#1d304c]/75">
                      <td className="px-5 sm:px-7 py-[1.125rem] font-mono text-base text-[#a8d1ff] break-words whitespace-normal">{r.url}</td>
                      <td className="px-5 sm:px-7 py-[1.125rem] text-[#b2c5e1] font-mono">{r.before}</td>
                      <td className="px-5 sm:px-7 py-[1.125rem] font-mono font-medium" style={{ color: r.after >= r.before ? '#31d49f' : '#fb6f7a' }}>{r.after}</td>
                      <td className="px-5 sm:px-7 py-[1.125rem]"><Delta value={r.delta} /></td>
                    </tr>
                  ))}
                </Table>
              </div>
              <div className="md:hidden p-5 space-y-4">
                {diff.score_delta.map(r => (
                  <div key={r.url} className="rounded-2xl border border-[var(--border-soft)] bg-[#17283f]/55 p-5 space-y-2.5">
                    <p className="font-mono text-base text-[#8fc0ff] break-words">{r.url}</p>
                    <div className="flex items-center justify-between text-base text-[#b2c5e1]"><span>Before</span><span className="font-mono">{r.before}</span></div>
                    <div className="flex items-center justify-between text-base"><span className="text-[#b2c5e1]">After</span><span className="font-mono font-medium" style={{ color: r.after >= r.before ? '#31d49f' : '#fb6f7a' }}>{r.after}</span></div>
                    <div className="pt-1"><Delta value={r.delta} /></div>
                  </div>
                ))}
              </div>
            </Card>
          )}
        </div>
      )}
    </div>
  )
}







