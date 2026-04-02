import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getGscStatus, revokeGsc, saveGscDomain } from '../api/audits'
import api from '../api/client'
import { Card, Button, Input, PageHeader, EmptyState, Table } from '../components/ui'
import { Link2, Link2Off, CheckCircle2, Settings } from 'lucide-react'

export default function GscSettings() {
  const [pendingDomain, setPendingDomain] = useState('')
  const qc = useQueryClient()
  const { data: domains, isLoading } = useQuery({ queryKey: ['gsc-status'], queryFn: getGscStatus })
  const hasPending = domains?.some(d => d.domain === 'pending')

  const revoke = useMutation({
    mutationFn: d => revokeGsc(d),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['gsc-status'] }),
  })

  const save = useMutation({
    mutationFn: () => saveGscDomain(pendingDomain),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['gsc-status'] }); setPendingDomain('') },
  })

  const connected = (domains || []).filter(d => d.domain !== 'pending')

  return (
    <div className="px-4 py-5 sm:px-7 sm:py-8 lg:px-10 lg:py-10 max-w-[90rem] mx-auto">
      <PageHeader title="Google Search Console" subtitle="Connect GSC properties to see clicks, impressions and CTR in your audits" />

      <Card className="p-6 sm:p-8 mb-8 reveal-up">
        <h3 className="text-2xl font-semibold text-white mb-1">Connect a property</h3>
        <p className="text-base text-[#9eb3d2] mb-5">You will be redirected to Google to authorize access. Ensure the Google account has access to the GSC property.</p>
        <Button onClick={() => api.get('/api/gsc/auth-url').then(r => { window.location.href = r.data.url })}>
          <Link2 size={16} /> Connect with Google
        </Button>
      </Card>

      {hasPending && (
        <Card className="p-6 sm:p-8 mb-8 border-amber-300/35 bg-amber-500/7 reveal-up">
          <div className="flex items-start gap-3">
            <CheckCircle2 size={22} className="text-amber-300 mt-0.5 shrink-0" />
            <div className="flex-1">
              <p className="text-xl font-medium text-white mb-1">Google authorized - confirm your domain</p>
              <p className="text-base text-[#a9bedb] mb-4">Enter the domain exactly as it appears in Search Console (for example <code className="text-[#d7e8ff] bg-[#2c4468] px-1 rounded">example.com</code>)</p>
              <div className="flex flex-col sm:flex-row gap-3">
                <Input value={pendingDomain} onChange={e => setPendingDomain(e.target.value)} placeholder="example.com" className="max-w-sm" />
                <Button onClick={() => save.mutate()} disabled={!pendingDomain || save.isPending} className="w-full sm:w-auto">
                  {save.isPending ? 'Saving...' : 'Save'}
                </Button>
              </div>
            </div>
          </div>
        </Card>
      )}

      <Card className="reveal-up">
        <div className="p-5 sm:p-6 border-b border-[var(--border-soft)]">
          <h3 className="text-xl sm:text-2xl font-semibold text-white">Connected Domains</h3>
          <p className="text-base text-[#9bb3d4] mt-1.5">Domains currently synced with Search Console access</p>
        </div>
        {isLoading ? null : connected.length === 0 ? (
          <EmptyState icon={Settings} title="No domains connected" description="Connect a Google Search Console property above to get started." />
        ) : (
          <>
            <div className="hidden md:block">
              <Table headers={['Domain', 'Last Updated', '']}>
                {connected.map(d => (
                  <tr key={d.domain} className="border-b border-[#3f5d84]/30 hover:bg-[#1d304c]/75 transition-colors">
                    <td className="px-5 sm:px-7 py-[1.125rem] sm:py-5">
                      <div className="flex items-center gap-2">
                        <CheckCircle2 size={16} className="text-emerald-300 shrink-0" />
                        <span className="font-mono text-base text-[#dce9ff]">{d.domain}</span>
                      </div>
                    </td>
                    <td className="px-5 sm:px-7 py-[1.125rem] sm:py-5 text-base text-[#9eb3d2]">{d.updated_at?.slice(0, 16).replace('T', ' ')}</td>
                    <td className="px-5 sm:px-7 py-[1.125rem] sm:py-5">
                      <button
                        onClick={() => { if (confirm(`Disconnect ${d.domain}?`)) revoke.mutate(d.domain) }}
                        className="flex items-center gap-1.5 text-base text-[#9eb3d2] hover:text-rose-200 transition-colors"
                      >
                        <Link2Off size={14} /> Disconnect
                      </button>
                    </td>
                  </tr>
                ))}
              </Table>
            </div>

            <div className="md:hidden p-5 space-y-4">
              {connected.map(d => (
                <div key={d.domain} className="rounded-2xl border border-[var(--border-soft)] bg-[#17283f]/55 p-5 space-y-3.5">
                  <div className="flex items-center gap-2">
                    <CheckCircle2 size={16} className="text-emerald-300 shrink-0" />
                    <span className="font-mono text-base text-[#dce9ff] break-words">{d.domain}</span>
                  </div>
                  <p className="text-base text-[#9eb3d2]">{d.updated_at?.slice(0, 16).replace('T', ' ')}</p>
                  <button
                    onClick={() => { if (confirm(`Disconnect ${d.domain}?`)) revoke.mutate(d.domain) }}
                    className="inline-flex items-center gap-1.5 px-3.5 py-2.5 rounded-lg text-base text-rose-200 bg-rose-500/14 border border-rose-300/30"
                  >
                    <Link2Off size={14} /> Disconnect
                  </button>
                </div>
              ))}
            </div>
          </>
        )}
      </Card>
    </div>
  )
}





