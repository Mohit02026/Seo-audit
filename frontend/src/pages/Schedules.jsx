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
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['schedules'] }); onClose() },
  })

  return (
    <div className="fixed inset-0 bg-black/65 backdrop-blur-sm flex items-center justify-center z-50 p-4">
      <div className="surface-blur panel-surface rounded-2xl p-6 sm:p-8 w-full max-w-2xl shadow-[0_30px_60px_-30px_rgba(2,10,20,0.95)] reveal-up">
        <div className="flex items-center justify-between mb-5">
          <h3 className="text-2xl font-semibold text-white">New Schedule</h3>
          <button onClick={onClose} className="text-[#8fa7c8] hover:text-white transition-colors"><X size={18} /></button>
        </div>
        <div className="space-y-5">
          <div>
            <label className="text-base font-medium text-[#afc4e2] block mb-2">Website URL</label>
            <Input value={url} onChange={e => setUrl(e.target.value)} placeholder="https://example.com" />
          </div>
          <div>
            <label className="text-base font-medium text-[#afc4e2] block mb-2">Lead / Client</label>
            <Input value={lead} onChange={e => setLead(e.target.value)} placeholder="Acme Corp" />
          </div>
          <div>
            <label className="text-base font-medium text-[#afc4e2] block mb-2">Frequency</label>
            <Select value={freq} onChange={e => setFreq(e.target.value)}>
              <option value="weekly">Weekly</option>
              <option value="monthly">Monthly</option>
            </Select>
          </div>
          <div>
            <label className="text-base font-medium text-[#afc4e2] block mb-2">Max Pages</label>
            <Select value={maxPages} onChange={e => setMaxPages(Number(e.target.value))}>
              {[50, 100, 200, 500].map(n => <option key={n} value={n}>{n} pages</option>)}
            </Select>
          </div>
          <Toggle checked={jsRender} onChange={setJsRender} label="JS rendering" />
        </div>
        <div className="flex flex-col sm:flex-row gap-3 mt-6">
          <Button variant="secondary" className="flex-1" onClick={onClose}>Cancel</Button>
          <Button className="flex-1" onClick={() => mutate()} disabled={isPending || !url}>
            {isPending ? 'Creating...' : 'Create Schedule'}
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
    onSuccess: () => qc.invalidateQueries({ queryKey: ['schedules'] }),
  })

  const remove = useMutation({
    mutationFn: id => deleteSchedule(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['schedules'] }),
  })

  return (
    <div className="px-4 py-5 sm:px-7 sm:py-8 lg:px-10 lg:py-10 max-w-[90rem] mx-auto">
      <PageHeader
        title="Schedules"
        subtitle="Recurring audits run automatically"
        action={<Button onClick={() => setShowModal(true)} className="w-full sm:w-auto"><Plus size={16} /> New Schedule</Button>}
      />

      {isLoading ? null : !schedules?.length ? (
        <EmptyState icon={CalendarClock} title="No schedules yet" description="Set up a recurring audit to track a site's SEO health over time." />
      ) : (
        <Card className="reveal-up">
          <div className="px-6 sm:px-8 py-5 sm:py-6 border-b border-[var(--border-soft)]">
            <h3 className="text-xl sm:text-2xl font-semibold text-white">Active Schedules</h3>
            <p className="text-base text-[#c1d5ee] mt-1.5">Recurring crawl plans with next execution times</p>
          </div>

          <div className="hidden md:block">
            <Table headers={['URL', 'Lead', 'Frequency', 'Status', 'Last Run', 'Next Run', '']}>
              {schedules.map(s => (
                <tr key={s.schedule_id} className="border-b border-[#3f5d84]/30 hover:bg-[#1d304c]/75 transition-colors">
                  <td className="px-5 sm:px-7 py-[1.125rem] sm:py-5 font-mono text-base text-[#a8d1ff] break-words whitespace-normal">{s.url}</td>
                  <td className="px-5 sm:px-7 py-[1.125rem] sm:py-5 text-[#c0d2eb]">{s.lead_name || '--'}</td>
                  <td className="px-5 sm:px-7 py-[1.125rem] sm:py-5">
                    <Badge variant="info">{s.frequency}</Badge>
                  </td>
                  <td className="px-5 sm:px-7 py-[1.125rem] sm:py-5">
                    <Badge variant={s.active ? 'success' : 'default'}>{s.active ? 'Active' : 'Paused'}</Badge>
                  </td>
                  <td className="px-5 sm:px-7 py-[1.125rem] sm:py-5 text-[#9ab1d1] whitespace-nowrap">{s.last_run_at?.slice(0, 16).replace('T', ' ') || '--'}</td>
                  <td className="px-5 sm:px-7 py-[1.125rem] sm:py-5 text-[#9ab1d1] whitespace-nowrap">{s.next_run_at?.slice(0, 16).replace('T', ' ') || '--'}</td>
                  <td className="px-5 sm:px-7 py-[1.125rem] sm:py-5">
                    <div className="flex items-center gap-2">
                      <button
                        onClick={() => toggle.mutate({ id: s.schedule_id, active: !s.active })}
                        className="text-[#93abd0] hover:text-white transition-colors p-1 rounded-md hover:bg-[#274062]"
                      >
                        {s.active ? <Pause size={14} /> : <Play size={14} />}
                      </button>
                      <button
                        onClick={() => { if (confirm('Delete this schedule?')) remove.mutate(s.schedule_id) }}
                        className="text-[#93abd0] hover:text-rose-200 transition-colors p-1 rounded-md hover:bg-rose-500/15"
                      >
                        <Trash2 size={14} />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </Table>
          </div>

            <div className="md:hidden p-5 space-y-4">
              {schedules.map(s => (
              <div key={s.schedule_id} className="rounded-2xl border border-[var(--border-soft)] bg-[#17283f]/55 p-5 space-y-3.5">
                <p className="font-mono text-base text-[#8fc0ff] break-words">{s.url}</p>
                <div className="flex items-center justify-between">
                  <Badge variant="info">{s.frequency}</Badge>
                  <Badge variant={s.active ? 'success' : 'default'}>{s.active ? 'Active' : 'Paused'}</Badge>
                </div>
                <div className="text-base text-[#a2b8d8] space-y-1.5">
                  <p>Lead: {s.lead_name || '--'}</p>
                  <p>Last: {s.last_run_at?.slice(0, 16).replace('T', ' ') || '--'}</p>
                  <p>Next: {s.next_run_at?.slice(0, 16).replace('T', ' ') || '--'}</p>
                </div>
                <div className="flex items-center gap-2 pt-1">
                  <button
                    onClick={() => toggle.mutate({ id: s.schedule_id, active: !s.active })}
                    className="inline-flex items-center gap-1.5 px-3.5 py-2.5 rounded-lg text-base text-[#d6e4fb] bg-[#244063]/70 border border-[#4b668d]/45"
                  >
                    {s.active ? <Pause size={14} /> : <Play size={14} />}
                    {s.active ? 'Pause' : 'Resume'}
                  </button>
                  <button
                    onClick={() => { if (confirm('Delete this schedule?')) remove.mutate(s.schedule_id) }}
                    className="inline-flex items-center gap-1.5 px-3.5 py-2.5 rounded-lg text-base text-rose-200 bg-rose-500/14 border border-rose-300/30"
                  >
                    <Trash2 size={14} />
                    Delete
                  </button>
                </div>
              </div>
            ))}
          </div>
        </Card>
      )}

      {showModal && <NewScheduleModal onClose={() => setShowModal(false)} />}
    </div>
  )
}







