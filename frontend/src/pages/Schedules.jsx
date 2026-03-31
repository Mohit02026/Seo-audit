import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getSchedules, createSchedule, updateSchedule, deleteSchedule } from '../api/audits'
import { Card, Button, Input, Select, Toggle, PageHeader, Badge, EmptyState, Table } from '../components/ui'
import { Plus, Pause, Play, Trash2, CalendarClock, X } from 'lucide-react'

function NewScheduleModal({ onClose }) {
  const [url, setUrl] = useState('https://')
  const [lead, setLead] = useState('')
  const [freq, setFreq] = useState('weekly')
  const [maxPages, setMaxPages] = useState(200)
  const [jsRender, setJsRender] = useState(true)
  const qc = useQueryClient()

  const { mutate, isPending } = useMutation({
    mutationFn: () => createSchedule({ url, lead_name: lead, frequency: freq, max_pages: maxPages, js_render: jsRender }),
    onSuccess: () => { qc.invalidateQueries(['schedules']); onClose() },
  })

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50 p-4">
      <div className="bg-[#1E293B] border border-[#334155] rounded-2xl p-6 w-full max-w-md shadow-2xl">
        <div className="flex items-center justify-between mb-5">
          <h3 className="text-base font-semibold text-white">New Schedule</h3>
          <button onClick={onClose} className="text-slate-500 hover:text-white"><X size={18} /></button>
        </div>
        <div className="space-y-4">
          <div>
            <label className="text-xs font-medium text-slate-400 block mb-1.5">Website URL</label>
            <Input value={url} onChange={e => setUrl(e.target.value)} placeholder="https://example.com" />
          </div>
          <div>
            <label className="text-xs font-medium text-slate-400 block mb-1.5">Lead / Client</label>
            <Input value={lead} onChange={e => setLead(e.target.value)} placeholder="Acme Corp" />
          </div>
          <div>
            <label className="text-xs font-medium text-slate-400 block mb-1.5">Frequency</label>
            <Select value={freq} onChange={e => setFreq(e.target.value)}>
              <option value="weekly">Weekly</option>
              <option value="monthly">Monthly</option>
            </Select>
          </div>
          <div>
            <label className="text-xs font-medium text-slate-400 block mb-1.5">Max Pages</label>
            <Select value={maxPages} onChange={e => setMaxPages(Number(e.target.value))}>
              {[50, 100, 200, 500].map(n => <option key={n} value={n}>{n} pages</option>)}
            </Select>
          </div>
          <Toggle checked={jsRender} onChange={setJsRender} label="JS rendering" />
        </div>
        <div className="flex gap-3 mt-6">
          <Button variant="secondary" className="flex-1" onClick={onClose}>Cancel</Button>
          <Button className="flex-1" onClick={() => mutate()} disabled={isPending || !url}>
            {isPending ? 'Creating…' : 'Create Schedule'}
          </Button>
        </div>
      </div>
    </div>
  )
}

export default function Schedules() {
  const [showModal, setShowModal] = useState(false)
  const qc = useQueryClient()
  const { data: schedules, isLoading } = useQuery({ queryKey: ['schedules'], queryFn: getSchedules, refetchInterval: 60000 })

  const toggle = useMutation({
    mutationFn: ({ id, active }) => updateSchedule(id, { active: active ? 1 : 0 }),
    onSuccess: () => qc.invalidateQueries(['schedules']),
  })

  const remove = useMutation({
    mutationFn: id => deleteSchedule(id),
    onSuccess: () => qc.invalidateQueries(['schedules']),
  })

  return (
    <div className="p-6 max-w-6xl mx-auto">
      <PageHeader
        title="Schedules"
        subtitle="Recurring audits run automatically"
        action={<Button onClick={() => setShowModal(true)}><Plus size={16} /> New Schedule</Button>}
      />

      {isLoading ? null : !schedules?.length ? (
        <EmptyState icon={CalendarClock} title="No schedules yet" description="Set up a recurring audit to track a site's SEO health over time." />
      ) : (
        <Card>
          <Table headers={['URL', 'Lead', 'Frequency', 'Status', 'Last Run', 'Next Run', '']}>
            {schedules.map(s => (
              <tr key={s.schedule_id} className="border-b border-[#334155]/50 hover:bg-[#263348] transition-colors">
                <td className="px-4 py-3 font-mono text-xs text-blue-400 max-w-[200px] truncate">{s.url}</td>
                <td className="px-4 py-3 text-slate-400 text-sm">{s.lead_name || '—'}</td>
                <td className="px-4 py-3">
                  <Badge variant="info">{s.frequency}</Badge>
                </td>
                <td className="px-4 py-3">
                  <Badge variant={s.active ? 'success' : 'default'}>{s.active ? 'Active' : 'Paused'}</Badge>
                </td>
                <td className="px-4 py-3 text-xs text-slate-500">{s.last_run_at?.slice(0, 16).replace('T', ' ') || '—'}</td>
                <td className="px-4 py-3 text-xs text-slate-500">{s.next_run_at?.slice(0, 16).replace('T', ' ') || '—'}</td>
                <td className="px-4 py-3">
                  <div className="flex items-center gap-2">
                    <button onClick={() => toggle.mutate({ id: s.schedule_id, active: !s.active })}
                      className="text-slate-500 hover:text-white transition-colors p-1 rounded hover:bg-[#334155]">
                      {s.active ? <Pause size={14} /> : <Play size={14} />}
                    </button>
                    <button onClick={() => { if (confirm('Delete this schedule?')) remove.mutate(s.schedule_id) }}
                      className="text-slate-500 hover:text-red-400 transition-colors p-1 rounded hover:bg-red-500/10">
                      <Trash2 size={14} />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </Table>
        </Card>
      )}

      {showModal && <NewScheduleModal onClose={() => setShowModal(false)} />}
    </div>
  )
}
