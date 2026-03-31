import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getGscStatus, revokeGsc, saveGscDomain } from '../api/audits'
import { Card, Button, Input, Badge, PageHeader, EmptyState, Table } from '../components/ui'
import { Link2, Link2Off, Plus, CheckCircle2, Settings } from 'lucide-react'

export default function GscSettings() {
  const [pendingDomain, setPendingDomain] = useState('')
  const qc = useQueryClient()
  const { data: domains, isLoading } = useQuery({ queryKey: ['gsc-status'], queryFn: getGscStatus })
  const hasPending = domains?.some(d => d.domain === 'pending')

  const revoke = useMutation({
    mutationFn: d => revokeGsc(d),
    onSuccess: () => qc.invalidateQueries(['gsc-status']),
  })

  const save = useMutation({
    mutationFn: () => saveGscDomain(pendingDomain),
    onSuccess: () => { qc.invalidateQueries(['gsc-status']); setPendingDomain('') },
  })

  const connected = (domains || []).filter(d => d.domain !== 'pending')

  return (
    <div className="p-6 max-w-3xl mx-auto">
      <PageHeader title="Google Search Console" subtitle="Connect GSC properties to see clicks, impressions and CTR in your audits" />

      {/* Connect new */}
      <Card className="p-5 mb-6">
        <h3 className="text-sm font-semibold text-white mb-1">Connect a property</h3>
        <p className="text-xs text-slate-500 mb-4">You'll be redirected to Google to authorise access. Ensure the Google account has access to the GSC property.</p>
        <a href="/api/gsc/auth">
          <Button><Link2 size={16} /> Connect with Google</Button>
        </a>
      </Card>

      {/* Pending token confirmation */}
      {hasPending && (
        <Card className="p-5 mb-6 border-amber-500/30 bg-amber-500/5">
          <div className="flex items-start gap-3">
            <CheckCircle2 size={18} className="text-amber-400 mt-0.5 shrink-0" />
            <div className="flex-1">
              <p className="text-sm font-medium text-white mb-1">Google authorised — confirm your domain</p>
              <p className="text-xs text-slate-400 mb-3">Enter the domain exactly as it appears in Search Console (e.g. <code className="text-slate-300 bg-[#334155] px-1 rounded">example.com</code>)</p>
              <div className="flex gap-3">
                <Input value={pendingDomain} onChange={e => setPendingDomain(e.target.value)} placeholder="example.com" className="max-w-xs" />
                <Button onClick={() => save.mutate()} disabled={!pendingDomain || save.isPending}>
                  {save.isPending ? 'Saving…' : 'Save'}
                </Button>
              </div>
            </div>
          </div>
        </Card>
      )}

      {/* Connected domains */}
      <Card>
        <div className="p-4 border-b border-[#334155]">
          <h3 className="text-sm font-semibold text-white">Connected Domains</h3>
        </div>
        {isLoading ? null : connected.length === 0 ? (
          <EmptyState icon={Settings} title="No domains connected" description="Connect a Google Search Console property above to get started." />
        ) : (
          <Table headers={['Domain', 'Last Updated', '']}>
            {connected.map(d => (
              <tr key={d.domain} className="border-b border-[#334155]/50 hover:bg-[#263348] transition-colors">
                <td className="px-4 py-3">
                  <div className="flex items-center gap-2">
                    <CheckCircle2 size={14} className="text-green-400 shrink-0" />
                    <span className="font-mono text-sm text-slate-200">{d.domain}</span>
                  </div>
                </td>
                <td className="px-4 py-3 text-xs text-slate-500">{d.updated_at?.slice(0, 16).replace('T', ' ')}</td>
                <td className="px-4 py-3">
                  <button onClick={() => { if (confirm(`Disconnect ${d.domain}?`)) revoke.mutate(d.domain) }}
                    className="flex items-center gap-1.5 text-xs text-slate-500 hover:text-red-400 transition-colors">
                    <Link2Off size={13} /> Disconnect
                  </button>
                </td>
              </tr>
            ))}
          </Table>
        )}
      </Card>
    </div>
  )
}
