import { useState, useEffect } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { getJob, getGscData } from '../api/audits'
import api from '../api/client'
import { Card, StatusBadge, Tabs, Skeleton, StatCard, Table, EmptyState } from '../components/ui'
import { RadialBarChart, RadialBar, ResponsiveContainer } from 'recharts'
import { ArrowLeft, Download, FileText, Table2, Gauge, Search, AlertTriangle, CheckCircle2 } from 'lucide-react'
import clsx from 'clsx'

async function downloadFile(url, filename) {
  const res = await api.get(url, { responseType: 'blob' })
  const href = URL.createObjectURL(res.data)
  const a = document.createElement('a')
  a.href = href
  a.download = filename
  a.click()
  URL.revokeObjectURL(href)
}

const SCORE_COLOR = s => s >= 90 ? '#31d49f' : s >= 50 ? '#f7b443' : '#fb6f7a'

function ScoreRing({ score, size = 80 }) {
  if (score == null) return <div className="text-[#7f95b8] text-sm">N/A</div>
  const color = SCORE_COLOR(score)
  return (
    <div className="relative" style={{ width: size, height: size }}>
      <ResponsiveContainer width="100%" height="100%">
        <RadialBarChart innerRadius="70%" outerRadius="100%" data={[{ value: score }]} startAngle={90} endAngle={-270}>
          <RadialBar background={{ fill: '#36557d' }} dataKey="value" fill={color} cornerRadius={5} />
        </RadialBarChart>
      </ResponsiveContainer>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-xl font-bold text-white">{score}</span>
      </div>
    </div>
  )
}

function IssueRow({ label, value, threshold = 0, invert = false }) {
  const isIssue = invert ? value < threshold : value > threshold
  return (
    <div className="flex items-center justify-between py-3.5 border-b border-[var(--border-soft)] last:border-0">
      <span className="text-base text-[#b2c5e1]">{label}</span>
      <span className={clsx('text-base font-medium', isIssue ? 'text-rose-300' : 'text-[#d6e2f5]')}>{value ?? 0}</span>
    </div>
  )
}

function formatElapsed(seconds) {
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = seconds % 60
  if (h > 0) return `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`
  return `${m}:${String(s).padStart(2, '0')}`
}

function RunningBanner({ status, url, createdAt }) {
  const [elapsed, setElapsed] = useState(0)

  useEffect(() => {
    const start = createdAt ? new Date(createdAt).getTime() : Date.now()
    setElapsed(Math.floor((Date.now() - start) / 1000))
    const id = setInterval(() => setElapsed(Math.floor((Date.now() - start) / 1000)), 1000)
    return () => clearInterval(id)
  }, [createdAt])

  const isQueued = status === 'queued'

  return (
    <div className="mb-6 reveal-up">
      <div className="surface-blur panel-surface rounded-2xl p-5 sm:p-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2.5">
            <div className="w-2 h-2 rounded-full bg-[#4f9cfb] pulse-glow" />
            <span className="text-base font-medium text-[#d6e8ff]">
              {isQueued ? 'Queued…' : `Crawling ${url}`}
            </span>
          </div>
          <span className="font-mono text-base text-[#7f9ec5] tabular-nums">{formatElapsed(elapsed)}</span>
        </div>
        <div className="relative h-2 rounded-full bg-[#1a2e47] overflow-hidden">
          <div
            className="absolute inset-y-0 left-0 rounded-full bg-gradient-to-r from-[#3a7fd4] to-[#4f9cfb] progress-fill"
            style={{ animationPlayState: isQueued ? 'paused' : 'running' }}
          />
          <div className="absolute inset-y-0 left-0 w-1/3 progress-shimmer" />
        </div>
        <p className="text-sm text-[#6a87ab] mt-3">
          {isQueued ? 'Waiting for a worker to pick up this job…' : 'Analysis will appear automatically when the crawl finishes.'}
        </p>
      </div>
    </div>
  )
}

export default function AuditDetail() {
  const { id } = useParams()
  const navigate = useNavigate()
  const [tab, setTab] = useState('overview')

  const { data: job, isLoading } = useQuery({
    queryKey: ['job', id],
    queryFn: () => getJob(id),
    refetchInterval: query => {
      const d = query.state.data
      return d?.status === 'running' || d?.status === 'queued' ? 3000 : false
    },
  })

  const { data: gscData } = useQuery({
    queryKey: ['gsc', id],
    queryFn: () => getGscData(id),
    enabled: job?.status === 'completed',
  })

  if (isLoading) return (
    <div className="px-4 py-5 sm:px-7 sm:py-8 lg:px-10 lg:py-10 max-w-[90rem] mx-auto space-y-5">
      <Skeleton className="h-8 w-64" />
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-5">{[...Array(4)].map((_, i) => <Skeleton key={i} className="h-32" />)}</div>
      <Skeleton className="h-64 w-full" />
    </div>
  )

  const result = job?.result || {}
  const summary = result.crawl_summary || {}
  const pages = job?.pages_data || []
  const ps = result.pagespeed_sample || []
  const avgScore = ps.length ? Math.round(ps.filter(p => p.performance_score != null).reduce((a, b) => a + (b.performance_score || 0), 0) / ps.filter(p => p.performance_score != null).length) : null

  const exportUrl = type => `/api/export/${id}/${type}`

  const tabs = [
    { id: 'overview', label: 'Overview' },
    { id: 'issues', label: 'Issues', count: (summary.missing_titles || 0) + (summary.status_4xx || 0) + (summary.status_5xx || 0) },
    { id: 'pages', label: 'Pages', count: summary.total_pages },
    { id: 'pagespeed', label: 'PageSpeed', count: ps.length },
    { id: 'gsc', label: 'GSC', count: gscData?.length },
    { id: 'reports', label: 'Reports' },
  ]

  return (
    <div className="px-4 py-5 sm:px-7 sm:py-8 lg:px-10 lg:py-10 max-w-[90rem] mx-auto">
      <div className="flex items-start sm:items-center gap-3 mb-7 reveal-up">
        <button onClick={() => navigate('/')} className="text-[#8fa7c8] hover:text-white transition-colors">
          <ArrowLeft size={20} />
        </button>
        <div className="flex-1 min-w-0">
          <div className="flex flex-wrap items-center gap-3">
            <h1 className="text-2xl sm:text-3xl font-semibold text-white break-words">{job?.url}</h1>
            <StatusBadge status={job?.status} />
          </div>
          <p className="text-base text-[#c2d6ef] mt-1.5">{job?.lead_name} - {job?.created_at?.slice(0, 10)}</p>
        </div>
      </div>

      {(job?.status === 'running' || job?.status === 'queued') && (
        <RunningBanner status={job.status} url={job.url} createdAt={job.created_at} />
      )}

      <Tabs tabs={tabs} active={tab} onChange={setTab} />

      {tab === 'overview' && (
        <div className="space-y-6">
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-5">
            <StatCard label="Total Pages" value={summary.total_pages} icon={Table2} color="blue" />
            <StatCard label="Avg Perf Score" value={avgScore ?? '--'} icon={Gauge} color={avgScore >= 90 ? 'green' : avgScore >= 50 ? 'amber' : 'red'} />
            <StatCard label="4xx Errors" value={summary.status_4xx} icon={AlertTriangle} color="red" />
            <StatCard label="With Schema" value={summary.pages_with_schema} icon={CheckCircle2} color="green" />
          </div>

          <div className="grid md:grid-cols-2 gap-5">
            <Card className="p-5 sm:p-7 reveal-up">
              <h3 className="text-xl font-medium text-white mb-4">On-Page SEO</h3>
              <IssueRow label="Missing titles" value={summary.missing_titles} />
              <IssueRow label="Missing meta description" value={summary.no_meta_desc} />
              <IssueRow label="Missing H1" value={summary.no_h1} />
              <IssueRow label="Multiple H1" value={summary.multi_h1} />
              <IssueRow label="Noindex pages" value={summary.noindex_pages} />
              <IssueRow label="Thin content (<300 words)" value={summary.thin_content_pages} />
            </Card>
            <Card className="p-5 sm:p-7 reveal-up">
              <h3 className="text-xl font-medium text-white mb-4">Technical Health</h3>
              <IssueRow label="Broken internal links (4xx)" value={summary.status_4xx} />
              <IssueRow label="Broken external links" value={summary.broken_external_links_count} />
              <IssueRow label="Canonical issues" value={summary.canonical_issues_count} />
              <IssueRow label="Orphan pages" value={summary.orphan_pages_count} />
              <IssueRow label="Mixed content" value={summary.pages_with_mixed_content} />
              <IssueRow label="Avg response time" value={summary.avg_response_time_ms ? `${summary.avg_response_time_ms}ms` : '--'} threshold={0} />
            </Card>
          </div>

          {result.top_pagerank_pages?.length > 0 && (
            <Card className="p-5 sm:p-7 reveal-up">
              <h3 className="text-xl font-medium text-white mb-5">Top Pages by Internal PageRank</h3>
              <div className="space-y-2.5">
                {result.top_pagerank_pages.slice(0, 10).map((p, i) => (
                  <div key={p.url} className="flex items-center gap-3">
                    <span className="text-base text-[#7e95b8] w-6">{i + 1}</span>
                    <div className="flex-1 min-w-0">
                      <p className="text-base font-mono text-[#a8d1ff] break-words">{p.url}</p>
                      {p.title && <p className="text-base text-[#c2d6ef] break-words">{p.title}</p>}
                    </div>
                    <div className="w-24 bg-[#2d4568] rounded-full h-2">
                      <div className="bg-[#4f9cfb] h-2 rounded-full" style={{ width: `${p.pagerank_score * 100}%` }} />
                    </div>
                    <span className="text-base text-[#b2c5e1] w-12 text-right">{(p.pagerank_score * 100).toFixed(0)}%</span>
                  </div>
                ))}
              </div>
            </Card>
          )}
        </div>
      )}

      {tab === 'issues' && (
        <div className="space-y-4">
          {[
            {
              label: 'Critical',
              items: [
                summary.status_4xx && `${summary.status_4xx} pages returning 4xx errors`,
                summary.status_5xx && `${summary.status_5xx} pages returning 5xx errors`,
                summary.missing_titles && `${summary.missing_titles} pages missing title tags`,
                summary.pages_not_https && `${summary.pages_not_https} pages not on HTTPS`,
              ].filter(Boolean),
              color: 'border-rose-400/35 bg-rose-500/8',
            },
            {
              label: 'Warnings',
              items: [
                summary.no_meta_desc && `${summary.no_meta_desc} pages missing meta descriptions`,
                summary.no_h1 && `${summary.no_h1} pages missing H1`,
                summary.thin_content_pages && `${summary.thin_content_pages} thin content pages (<300 words)`,
                summary.broken_external_links_count && `${summary.broken_external_links_count} broken external links`,
                summary.canonical_issues_count && `${summary.canonical_issues_count} canonical conflicts`,
                summary.orphan_pages_count && `${summary.orphan_pages_count} orphan pages`,
                summary.pages_with_mixed_content && `${summary.pages_with_mixed_content} pages with mixed content`,
              ].filter(Boolean),
              color: 'border-amber-400/35 bg-amber-500/8',
            },
            {
              label: 'Good',
              items: [
                summary.pages_with_schema && `${summary.pages_with_schema} pages with structured data`,
                !summary.status_4xx && 'No 4xx errors found',
                summary.status_2xx && `${summary.status_2xx} pages returning 200 OK`,
              ].filter(Boolean),
              color: 'border-emerald-400/35 bg-emerald-500/8',
            },
          ].map(group => group.items.length ? (
            <Card key={group.label} className={clsx('p-5 sm:p-7 border reveal-up', group.color)}>
              <h3 className="text-xl font-semibold text-white mb-3.5">{group.label}</h3>
              <ul className="space-y-1.5">
                {group.items.map(item => (
                  <li key={item} className="text-base text-[#d6e2f5] flex items-start gap-2">
                    <span className="mt-1 text-[#8ea4c6]">-</span> {item}
                  </li>
                ))}
              </ul>
            </Card>
          ) : null)}
        </div>
      )}

      {tab === 'pages' && (
        <Card className="reveal-up">
          <div className="px-6 sm:px-8 py-5 sm:py-6 border-b border-[var(--border-soft)]">
            <h3 className="text-xl sm:text-2xl font-semibold text-white">Crawled Pages</h3>
            <p className="text-base text-[#c1d5ee] mt-1.5">Status, metadata quality, and technical checks for each discovered URL</p>
          </div>

          <div className="hidden md:block overflow-x-auto">
            <table className="w-full text-base">
              <thead>
                <tr className="border-b border-[#3f5d84]/45">
                  {['URL', 'Status', 'Depth', 'PageRank', 'Title', 'Words', 'H1', 'Schema', 'OG', 'Response'].map(h => (
                    <th key={h} className="text-left px-4 sm:px-6 py-[1.125rem] sm:py-5 text-xs sm:text-sm text-[#91a9ca] font-semibold uppercase tracking-[0.12em] whitespace-nowrap">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {pages.map(p => (
                  <tr key={p.url} className="border-b border-[#3f5d84]/28 hover:bg-[#1d304c]/75 transition-colors">
                    <td className="px-4 sm:px-6 py-[1.125rem]">
                      <a href={p.url} target="_blank" rel="noreferrer" className="font-mono text-base text-[#a8d1ff] break-words whitespace-normal block hover:underline">{p.url}</a>
                    </td>
                    <td className="px-4 sm:px-6 py-[1.125rem]">
                      <span className={clsx('font-mono font-medium', p.status_code < 300 ? 'text-emerald-300' : p.status_code < 400 ? 'text-amber-300' : 'text-rose-300')}>
                        {p.status_code}
                      </span>
                    </td>
                    <td className="px-4 sm:px-6 py-[1.125rem] text-[#c0d2eb]">{p.url_depth}</td>
                    <td className="px-4 sm:px-6 py-[1.125rem]">
                      {p.pagerank_score != null ? (
                        <div className="flex items-center gap-1.5">
                          <div className="w-14 bg-[#2d4568] rounded-full h-1.5">
                            <div className="bg-[#4f9cfb] h-1.5 rounded-full" style={{ width: `${p.pagerank_score * 100}%` }} />
                          </div>
                          <span className="text-[#c0d2eb]">{(p.pagerank_score * 100).toFixed(0)}%</span>
                        </div>
                      ) : '--'}
                    </td>
                    <td className="px-4 sm:px-6 py-[1.125rem]">
                      <span className={clsx('break-words whitespace-normal block', !p.title ? 'text-rose-300 italic' : 'text-[#e2ecfb]')}>
                        {p.title || 'Missing'}
                      </span>
                    </td>
                    <td className="px-4 sm:px-6 py-[1.125rem] text-[#c0d2eb]">{p.word_count}</td>
                    <td className="px-4 sm:px-6 py-[1.125rem]">
                      <span className={p.h1_count === 0 ? 'text-rose-300' : p.h1_count > 1 ? 'text-amber-300' : 'text-emerald-300'}>
                        {p.h1_count}
                      </span>
                    </td>
                    <td className="px-4 sm:px-6 py-[1.125rem]">{p.has_schema ? <span className="text-emerald-300">yes</span> : <span className="text-[#8ea4c6]">--</span>}</td>
                    <td className="px-4 sm:px-6 py-[1.125rem]">{p.has_og ? <span className="text-emerald-300">yes</span> : <span className="text-[#8ea4c6]">--</span>}</td>
                    <td className="px-4 sm:px-6 py-[1.125rem] text-[#c0d2eb] font-mono">{p.response_time_ms}ms</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="md:hidden p-5 space-y-4">
            {pages.map(p => (
              <div key={p.url} className="rounded-2xl border border-[var(--border-soft)] bg-[#17283f]/55 p-5 space-y-3">
                <a href={p.url} target="_blank" rel="noreferrer" className="font-mono text-base text-[#8fc0ff] break-words hover:underline">{p.url}</a>
                <div className="flex items-center justify-between text-base">
                  <span className={clsx('font-mono font-medium', p.status_code < 300 ? 'text-emerald-300' : p.status_code < 400 ? 'text-amber-300' : 'text-rose-300')}>{p.status_code}</span>
                  <span className="text-[#a9bddb]">Depth: {p.url_depth}</span>
                </div>
                <div className="text-base text-[#bdd1ed]">{p.title || 'Missing title'}</div>
                <div className="text-base text-[#9bb3d4]">Words: {p.word_count} | H1: {p.h1_count}</div>
                <div className="flex items-center justify-between text-base">
                  <span className="text-[#9bb3d4]">Schema: {p.has_schema ? 'yes' : '--'} | OG: {p.has_og ? 'yes' : '--'}</span>
                  <span className="font-mono text-[#9bb3d4]">{p.response_time_ms}ms</span>
                </div>
              </div>
            ))}
          </div>
        </Card>
      )}

      {tab === 'pagespeed' && (
        <div className="space-y-4">
          {ps.length === 0 ? (
            <EmptyState icon={Gauge} title="No PageSpeed data" description="PageSpeed is sampled during audit if an API key is configured." />
          ) : ps.map(p => (
            <Card key={p.url} className="p-5 sm:p-7 reveal-up">
              <div className="flex flex-col sm:flex-row items-start gap-5">
                <ScoreRing score={p.performance_score} size={80} />
                <div className="flex-1 min-w-0">
                  <a href={p.url} target="_blank" rel="noreferrer" className="font-mono text-lg text-[#8fc0ff] hover:underline break-words block mb-4">{p.url}</a>
                  <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-6 gap-3">
                    {[
                      { label: 'LCP', value: p.lcp_ms ? `${(p.lcp_ms / 1000).toFixed(1)}s` : '--', good: p.lcp_ms < 2500 },
                      { label: 'INP', value: p.inp_ms ? `${p.inp_ms}ms` : '--', good: p.inp_ms < 200 },
                      { label: 'CLS', value: p.cls != null ? p.cls.toFixed(3) : '--', good: p.cls < 0.1 },
                      { label: 'FCP', value: p.fcp_ms ? `${(p.fcp_ms / 1000).toFixed(1)}s` : '--', good: p.fcp_ms < 1800 },
                      { label: 'TBT', value: p.tbt_ms ? `${p.tbt_ms}ms` : '--', good: p.tbt_ms < 200 },
                      { label: 'Speed Index', value: p.speed_index_ms ? `${(p.speed_index_ms / 1000).toFixed(1)}s` : '--', good: p.speed_index_ms < 3400 },
                    ].map(m => (
                      <div key={m.label} className="bg-[#0d1a2e]/90 border border-[#314d74] rounded-xl p-4">
                        <p className="text-base text-[#9eb3d2] mb-1">{m.label}</p>
                        <p className={clsx('text-lg font-semibold font-mono', m.good ? 'text-emerald-300' : 'text-amber-300')}>{m.value}</p>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </Card>
          ))}
        </div>
      )}

      {tab === 'gsc' && (
        <Card className="reveal-up">
          {!gscData?.length ? (
            <EmptyState icon={Search} title="No GSC data" description="Connect Google Search Console in Settings to see clicks, impressions and CTR per page." />
          ) : (
            <>
              <div className="hidden md:block">
                <Table headers={['Page', 'Clicks', 'Impressions', 'CTR', 'Position']}>
                  {gscData.map(row => (
                    <tr key={row.page} className="border-b border-[#3f5d84]/30 hover:bg-[#1d304c]/75 transition-colors">
                      <td className="px-5 sm:px-7 py-[1.125rem] font-mono text-base text-[#a8d1ff] break-words whitespace-normal">{row.page}</td>
                      <td className="px-5 sm:px-7 py-[1.125rem] text-white font-medium">{row.clicks?.toLocaleString()}</td>
                      <td className="px-5 sm:px-7 py-[1.125rem] text-[#c0d2eb]">{row.impressions?.toLocaleString()}</td>
                      <td className="px-5 sm:px-7 py-[1.125rem] text-[#c0d2eb]">{row.ctr}%</td>
                      <td className="px-5 sm:px-7 py-[1.125rem] text-[#c0d2eb]">{row.position}</td>
                    </tr>
                  ))}
                </Table>
              </div>
              <div className="md:hidden p-5 space-y-4">
                {gscData.map(row => (
                  <div key={row.page} className="rounded-2xl border border-[var(--border-soft)] bg-[#17283f]/55 p-5 space-y-3">
                    <p className="font-mono text-base text-[#8fc0ff] break-words">{row.page}</p>
                    <div className="grid grid-cols-2 gap-2.5 text-base text-[#c0d2eb]">
                      <p>Clicks: {row.clicks?.toLocaleString()}</p>
                      <p>Impr.: {row.impressions?.toLocaleString()}</p>
                      <p>CTR: {row.ctr}%</p>
                      <p>Pos.: {row.position}</p>
                    </div>
                  </div>
                ))}
              </div>
            </>
          )}
        </Card>
      )}

      {tab === 'reports' && job?.status === 'completed' && (
        <div className="grid md:grid-cols-2 gap-5 reveal-up">
          {[
            { label: 'Client Report (PDF)', desc: 'Branded PDF with critical, warning and good sections for delivery', url: exportUrl('pdf'), filename: `audit-${id}-client.pdf`, icon: FileText, primary: true },
            { label: 'Client Report (Markdown)', desc: 'Plain Markdown report for editing before sending', url: `/api/report/${id}/client`, filename: `audit-${id}-client.md`, icon: FileText },
            { label: 'Internal Report (Markdown)', desc: 'Full technical audit with per-page tables', url: `/api/report/${id}/internal`, filename: `audit-${id}-internal.md`, icon: FileText },
            { label: 'Raw Data (CSV)', desc: 'All page metrics as a spreadsheet', url: exportUrl('csv'), filename: `audit-${id}.csv`, icon: Download },
            { label: 'Raw Data (JSON)', desc: 'Complete audit JSON for n8n or other automations', url: exportUrl('json'), filename: `audit-${id}.json`, icon: Download },
          ].map(r => (
            <button key={r.label} onClick={() => downloadFile(r.url, r.filename)} className="block text-left w-full">
              <Card className={clsx('p-7 transition-all cursor-pointer hover:border-[#4f9cfb]/55 hover:bg-[#1d304c]/85', r.primary && 'border-[#4f9cfb]/45 bg-[#4f9cfb]/10')}>
                <div className="flex items-start gap-4">
                  <div className={clsx('w-12 h-12 rounded-xl flex items-center justify-center', r.primary ? 'bg-[#4f9cfb]/25 text-[#9ac8ff]' : 'bg-[#2d4568] text-[#9db3d2]')}>
                    <r.icon size={22} />
                  </div>
                  <div>
                    <p className="text-lg font-medium text-white">{r.label}</p>
                    <p className="text-base text-[#9eb3d2] mt-0.5">{r.desc}</p>
                  </div>
                </div>
              </Card>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}







