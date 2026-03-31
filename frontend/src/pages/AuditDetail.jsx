import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { getJob, getGscData } from '../api/audits'
import { Card, StatusBadge, ScoreBadge, Badge, Button, Tabs, Skeleton, StatCard, Table, EmptyState, PageHeader } from '../components/ui'
import { RadialBarChart, RadialBar, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts'
import { ArrowLeft, Download, FileText, Table2, Gauge, Search, AlertTriangle, CheckCircle2 } from 'lucide-react'
import clsx from 'clsx'

const SCORE_COLOR = s => s >= 90 ? '#22C55E' : s >= 50 ? '#F59E0B' : '#EF4444'

function ScoreRing({ score, size = 80 }) {
  if (score == null) return <div className="text-slate-600 text-sm">N/A</div>
  const color = SCORE_COLOR(score)
  return (
    <div className="relative" style={{ width: size, height: size }}>
      <ResponsiveContainer width="100%" height="100%">
        <RadialBarChart innerRadius="70%" outerRadius="100%" data={[{ value: score }]} startAngle={90} endAngle={-270}>
          <RadialBar background={{ fill: '#334155' }} dataKey="value" fill={color} cornerRadius={4} />
        </RadialBarChart>
      </ResponsiveContainer>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-lg font-bold text-white">{score}</span>
      </div>
    </div>
  )
}

function IssueRow({ label, value, threshold = 0, invert = false }) {
  const isIssue = invert ? value < threshold : value > threshold
  return (
    <div className="flex items-center justify-between py-2.5 border-b border-[#334155]/50 last:border-0">
      <span className="text-sm text-slate-400">{label}</span>
      <span className={clsx('text-sm font-medium', isIssue ? 'text-red-400' : 'text-slate-300')}>{value ?? 0}</span>
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
    refetchInterval: d => d?.status === 'running' || d?.status === 'queued' ? 3000 : false,
  })

  const { data: gscData } = useQuery({
    queryKey: ['gsc', id],
    queryFn: () => getGscData(id),
    enabled: job?.status === 'completed',
  })

  if (isLoading) return (
    <div className="p-6 max-w-6xl mx-auto space-y-4">
      <Skeleton className="h-8 w-64" />
      <div className="grid grid-cols-4 gap-4">{[...Array(4)].map((_, i) => <Skeleton key={i} className="h-28" />)}</div>
      <Skeleton className="h-64 w-full" />
    </div>
  )

  const result = job?.result || {}
  const summary = result.crawl_summary || {}
  const pages = job?.pages_data || []
  const ps = result.pagespeed_sample || []
  const avgScore = ps.length ? Math.round(ps.filter(p => p.performance_score != null).reduce((a, b) => a + (b.performance_score || 0), 0) / ps.filter(p => p.performance_score != null).length) : null

  const exportUrl = (type) => `/api/export/${id}/${type}`

  const tabs = [
    { id: 'overview', label: 'Overview' },
    { id: 'issues', label: 'Issues', count: (summary.missing_titles || 0) + (summary.status_4xx || 0) + (summary.status_5xx || 0) },
    { id: 'pages', label: 'Pages', count: summary.total_pages },
    { id: 'pagespeed', label: 'PageSpeed', count: ps.length },
    { id: 'gsc', label: 'GSC', count: gscData?.length },
    { id: 'reports', label: 'Reports' },
  ]

  return (
    <div className="p-6 max-w-6xl mx-auto">
      <div className="flex items-center gap-3 mb-6">
        <button onClick={() => navigate('/')} className="text-slate-500 hover:text-white transition-colors">
          <ArrowLeft size={18} />
        </button>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-3">
            <h1 className="text-lg font-semibold text-white truncate">{job?.url}</h1>
            <StatusBadge status={job?.status} />
          </div>
          <p className="text-xs text-slate-500 mt-0.5">{job?.lead_name} · {job?.created_at?.slice(0, 10)}</p>
        </div>
      </div>

      <Tabs tabs={tabs} active={tab} onChange={setTab} />

      {/* OVERVIEW */}
      {tab === 'overview' && (
        <div className="space-y-6">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <StatCard label="Total Pages" value={summary.total_pages} icon={Table2} color="blue" />
            <StatCard label="Avg Perf Score" value={avgScore ?? '—'} icon={Gauge} color={avgScore >= 90 ? 'green' : avgScore >= 50 ? 'amber' : 'red'} />
            <StatCard label="4xx Errors" value={summary.status_4xx} icon={AlertTriangle} color="red" />
            <StatCard label="With Schema" value={summary.pages_with_schema} icon={CheckCircle2} color="green" />
          </div>

          <div className="grid md:grid-cols-2 gap-4">
            <Card className="p-5">
              <h3 className="text-sm font-medium text-white mb-4">On-Page SEO</h3>
              <IssueRow label="Missing titles" value={summary.missing_titles} />
              <IssueRow label="Missing meta description" value={summary.no_meta_desc} />
              <IssueRow label="Missing H1" value={summary.no_h1} />
              <IssueRow label="Multiple H1" value={summary.multi_h1} />
              <IssueRow label="Noindex pages" value={summary.noindex_pages} />
              <IssueRow label="Thin content (<300 words)" value={summary.thin_content_pages} />
            </Card>
            <Card className="p-5">
              <h3 className="text-sm font-medium text-white mb-4">Technical Health</h3>
              <IssueRow label="Broken internal links (4xx)" value={summary.status_4xx} />
              <IssueRow label="Broken external links" value={summary.broken_external_links_count} />
              <IssueRow label="Canonical issues" value={summary.canonical_issues_count} />
              <IssueRow label="Orphan pages" value={summary.orphan_pages_count} />
              <IssueRow label="Mixed content" value={summary.pages_with_mixed_content} />
              <IssueRow label="Avg response time" value={summary.avg_response_time_ms ? `${summary.avg_response_time_ms}ms` : '—'} threshold={0} />
            </Card>
          </div>

          {result.top_pagerank_pages?.length > 0 && (
            <Card className="p-5">
              <h3 className="text-sm font-medium text-white mb-4">Top Pages by Internal PageRank</h3>
              <div className="space-y-2">
                {result.top_pagerank_pages.slice(0, 10).map((p, i) => (
                  <div key={p.url} className="flex items-center gap-3">
                    <span className="text-xs text-slate-600 w-4">{i + 1}</span>
                    <div className="flex-1 min-w-0">
                      <p className="text-xs font-mono text-blue-400 truncate">{p.url}</p>
                      {p.title && <p className="text-xs text-slate-500 truncate">{p.title}</p>}
                    </div>
                    <div className="w-20 bg-[#334155] rounded-full h-1.5">
                      <div className="bg-blue-500 h-1.5 rounded-full" style={{ width: `${p.pagerank_score * 100}%` }} />
                    </div>
                    <span className="text-xs text-slate-400 w-8 text-right">{(p.pagerank_score * 100).toFixed(0)}%</span>
                  </div>
                ))}
              </div>
            </Card>
          )}
        </div>
      )}

      {/* ISSUES */}
      {tab === 'issues' && (
        <div className="space-y-4">
          {[
            { label: '🔴 Critical', items: [
              summary.status_4xx && `${summary.status_4xx} pages returning 4xx errors`,
              summary.status_5xx && `${summary.status_5xx} pages returning 5xx errors`,
              summary.missing_titles && `${summary.missing_titles} pages missing title tags`,
              summary.pages_not_https && `${summary.pages_not_https} pages not on HTTPS`,
            ].filter(Boolean), color: 'border-red-500/20 bg-red-500/5' },
            { label: '🟡 Warnings', items: [
              summary.no_meta_desc && `${summary.no_meta_desc} pages missing meta descriptions`,
              summary.no_h1 && `${summary.no_h1} pages missing H1`,
              summary.thin_content_pages && `${summary.thin_content_pages} thin content pages (<300 words)`,
              summary.broken_external_links_count && `${summary.broken_external_links_count} broken external links`,
              summary.canonical_issues_count && `${summary.canonical_issues_count} canonical conflicts`,
              summary.orphan_pages_count && `${summary.orphan_pages_count} orphan pages`,
              summary.pages_with_mixed_content && `${summary.pages_with_mixed_content} pages with mixed content`,
            ].filter(Boolean), color: 'border-amber-500/20 bg-amber-500/5' },
            { label: '✅ Good', items: [
              summary.pages_with_schema && `${summary.pages_with_schema} pages with structured data`,
              !summary.status_4xx && 'No 4xx errors found',
              summary.status_2xx && `${summary.status_2xx} pages returning 200 OK`,
            ].filter(Boolean), color: 'border-green-500/20 bg-green-500/5' },
          ].map(group => group.items.length ? (
            <Card key={group.label} className={clsx('p-5 border', group.color)}>
              <h3 className="text-sm font-semibold text-white mb-3">{group.label}</h3>
              <ul className="space-y-1.5">
                {group.items.map(item => (
                  <li key={item} className="text-sm text-slate-300 flex items-start gap-2">
                    <span className="mt-1 text-slate-500">·</span> {item}
                  </li>
                ))}
              </ul>
            </Card>
          ) : null)}
        </div>
      )}

      {/* PAGES */}
      {tab === 'pages' && (
        <Card>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-[#334155]">
                  {['URL', 'Status', 'Depth', 'PageRank', 'Title', 'Words', 'H1', 'Schema', 'OG', 'Response'].map(h => (
                    <th key={h} className="text-left px-3 py-3 text-slate-500 font-medium uppercase tracking-wider whitespace-nowrap">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {pages.map(p => (
                  <tr key={p.url} className="border-b border-[#334155]/50 hover:bg-[#263348] transition-colors">
                    <td className="px-3 py-2.5 max-w-[200px]">
                      <a href={p.url} target="_blank" rel="noreferrer" className="font-mono text-blue-400 truncate block hover:underline">{p.url}</a>
                    </td>
                    <td className="px-3 py-2.5">
                      <span className={clsx('font-mono font-medium', p.status_code < 300 ? 'text-green-400' : p.status_code < 400 ? 'text-amber-400' : 'text-red-400')}>
                        {p.status_code}
                      </span>
                    </td>
                    <td className="px-3 py-2.5 text-slate-400">{p.url_depth}</td>
                    <td className="px-3 py-2.5">
                      {p.pagerank_score != null ? (
                        <div className="flex items-center gap-1.5">
                          <div className="w-12 bg-[#334155] rounded-full h-1">
                            <div className="bg-blue-500 h-1 rounded-full" style={{ width: `${p.pagerank_score * 100}%` }} />
                          </div>
                          <span className="text-slate-400">{(p.pagerank_score * 100).toFixed(0)}%</span>
                        </div>
                      ) : '—'}
                    </td>
                    <td className="px-3 py-2.5 max-w-[160px]">
                      <span className={clsx('truncate block', !p.title ? 'text-red-400 italic' : 'text-slate-300')}>
                        {p.title || 'Missing'}
                      </span>
                    </td>
                    <td className="px-3 py-2.5 text-slate-400">{p.word_count}</td>
                    <td className="px-3 py-2.5">
                      <span className={p.h1_count === 0 ? 'text-red-400' : p.h1_count > 1 ? 'text-amber-400' : 'text-green-400'}>
                        {p.h1_count}
                      </span>
                    </td>
                    <td className="px-3 py-2.5">{p.has_schema ? <span className="text-green-400">✓</span> : <span className="text-slate-600">—</span>}</td>
                    <td className="px-3 py-2.5">{p.has_og ? <span className="text-green-400">✓</span> : <span className="text-slate-600">—</span>}</td>
                    <td className="px-3 py-2.5 text-slate-400 font-mono">{p.response_time_ms}ms</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}

      {/* PAGESPEED */}
      {tab === 'pagespeed' && (
        <div className="space-y-4">
          {ps.length === 0 ? (
            <EmptyState icon={Gauge} title="No PageSpeed data" description="PageSpeed is sampled during audit if an API key is configured." />
          ) : ps.map(p => (
            <Card key={p.url} className="p-5">
              <div className="flex items-start gap-5">
                <ScoreRing score={p.performance_score} size={80} />
                <div className="flex-1 min-w-0">
                  <a href={p.url} target="_blank" rel="noreferrer" className="font-mono text-sm text-blue-400 hover:underline truncate block mb-3">{p.url}</a>
                  <div className="grid grid-cols-3 md:grid-cols-6 gap-3">
                    {[
                      { label: 'LCP', value: p.lcp_ms ? `${(p.lcp_ms / 1000).toFixed(1)}s` : '—', good: p.lcp_ms < 2500 },
                      { label: 'INP', value: p.inp_ms ? `${p.inp_ms}ms` : '—', good: p.inp_ms < 200 },
                      { label: 'CLS', value: p.cls != null ? p.cls.toFixed(3) : '—', good: p.cls < 0.1 },
                      { label: 'FCP', value: p.fcp_ms ? `${(p.fcp_ms / 1000).toFixed(1)}s` : '—', good: p.fcp_ms < 1800 },
                      { label: 'TBT', value: p.tbt_ms ? `${p.tbt_ms}ms` : '—', good: p.tbt_ms < 200 },
                      { label: 'Speed Index', value: p.speed_index_ms ? `${(p.speed_index_ms / 1000).toFixed(1)}s` : '—', good: p.speed_index_ms < 3400 },
                    ].map(m => (
                      <div key={m.label} className="bg-[#0F172A] rounded-lg p-3">
                        <p className="text-xs text-slate-500 mb-1">{m.label}</p>
                        <p className={clsx('text-sm font-semibold font-mono', m.good ? 'text-green-400' : 'text-amber-400')}>{m.value}</p>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </Card>
          ))}
        </div>
      )}

      {/* GSC */}
      {tab === 'gsc' && (
        <Card>
          {!gscData?.length ? (
            <EmptyState icon={Search} title="No GSC data" description="Connect Google Search Console in Settings to see clicks, impressions and CTR per page." />
          ) : (
            <Table headers={['Page', 'Clicks', 'Impressions', 'CTR', 'Position']}>
              {gscData.map(row => (
                <tr key={row.page} className="border-b border-[#334155]/50 hover:bg-[#263348] transition-colors">
                  <td className="px-4 py-2.5 font-mono text-xs text-blue-400 max-w-[300px] truncate">{row.page}</td>
                  <td className="px-4 py-2.5 text-white font-medium">{row.clicks?.toLocaleString()}</td>
                  <td className="px-4 py-2.5 text-slate-400">{row.impressions?.toLocaleString()}</td>
                  <td className="px-4 py-2.5 text-slate-400">{row.ctr}%</td>
                  <td className="px-4 py-2.5 text-slate-400">{row.position}</td>
                </tr>
              ))}
            </Table>
          )}
        </Card>
      )}

      {/* REPORTS */}
      {tab === 'reports' && job?.status === 'completed' && (
        <div className="grid md:grid-cols-2 gap-4">
          {[
            { label: 'Client Report (PDF)', desc: 'Branded PDF with 🔴/🟡/✅ issues for client delivery', href: exportUrl('pdf'), icon: FileText, primary: true },
            { label: 'Client Report (Markdown)', desc: 'Plain Markdown report for editing before sending', href: `/api/report/${id}/client`, icon: FileText },
            { label: 'Internal Report (Markdown)', desc: 'Full technical audit with per-page tables', href: `/api/report/${id}/internal`, icon: FileText },
            { label: 'Raw Data (CSV)', desc: 'All page metrics as a spreadsheet', href: exportUrl('csv'), icon: Download },
            { label: 'Raw Data (JSON)', desc: 'Complete audit JSON for n8n or other automations', href: exportUrl('json'), icon: Download },
          ].map(r => (
            <a key={r.label} href={r.href} target="_blank" rel="noreferrer" download>
              <Card className={clsx('p-5 hover:border-blue-500/40 hover:bg-[#263348] transition-all cursor-pointer', r.primary && 'border-blue-500/30 bg-blue-500/5')}>
                <div className="flex items-start gap-4">
                  <div className={clsx('w-9 h-9 rounded-lg flex items-center justify-center', r.primary ? 'bg-blue-500/20 text-blue-400' : 'bg-[#334155] text-slate-400')}>
                    <r.icon size={18} />
                  </div>
                  <div>
                    <p className="text-sm font-medium text-white">{r.label}</p>
                    <p className="text-xs text-slate-500 mt-0.5">{r.desc}</p>
                  </div>
                </div>
              </Card>
            </a>
          ))}
        </div>
      )}
    </div>
  )
}
