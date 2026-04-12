import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useAuth } from '../context/AuthContext'
import { Activity, Loader2 } from 'lucide-react'

export default function Login() {
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const { login } = useAuth()
  const navigate = useNavigate()

  const submit = async e => {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      await login(email, password)
      navigate('/')
    } catch {
      setError('Invalid email or password')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="relative min-h-screen flex items-center justify-center p-6 sm:p-8 overflow-hidden">
      <div className="pointer-events-none absolute -left-24 -top-20 h-72 w-72 rounded-full bg-sky-500/18 blur-3xl" />
      <div className="pointer-events-none absolute -right-20 bottom-0 h-80 w-80 rounded-full bg-cyan-400/12 blur-3xl" />

      <div className="w-full max-w-2xl reveal-up relative z-10">
        <div className="flex items-center justify-center gap-5 mb-14">
          <div className="w-16 h-16 rounded-xl bg-gradient-to-br from-[#4f9cfb] to-[#24b5e7] flex items-center justify-center shadow-[0_16px_28px_-16px_rgba(79,156,251,0.95)]">
            <Activity size={28} className="text-white" />
          </div>
          <span className="text-4xl sm:text-5xl font-semibold tracking-tight text-white">SEO Crawler</span>
        </div>

        <div className="surface-blur panel-surface rounded-2xl p-8 sm:p-12 shadow-[0_30px_56px_-30px_rgba(2,10,20,0.92)]">
          <h2 className="text-3xl sm:text-4xl font-semibold text-white mb-2">Welcome back</h2>
          <p className="text-lg sm:text-xl text-[#9db3d2] mb-9">Sign in to continue to your audits</p>

          {error && (
            <div className="bg-rose-500/10 border border-rose-400/30 text-rose-200 text-base rounded-xl px-4 py-3 mb-4">
              {error}
            </div>
          )}

          <form onSubmit={submit} className="space-y-6">
            <div>
              <label className="text-lg font-medium text-[#afc4e2] mb-2.5 block">Email</label>
              <input
                type="email"
                value={email}
                onChange={e => setEmail(e.target.value)}
                placeholder="admin@example.com"
                required
                className="w-full bg-[#0d1a2e]/95 border border-[#53739b]/36 text-[#deebff] rounded-xl px-6 py-[1.05rem] text-lg placeholder-[#7288aa] leading-normal focus:outline-none focus:border-[#72b3ff] focus:ring-2 focus:ring-[#72b3ff]/28 transition-all"
              />
            </div>
            <div>
              <label className="text-lg font-medium text-[#afc4e2] mb-2.5 block">Password</label>
              <input
                type="password"
                value={password}
                onChange={e => setPassword(e.target.value)}
                placeholder="********"
                required
                className="w-full bg-[#0d1a2e]/95 border border-[#53739b]/36 text-[#deebff] rounded-xl px-6 py-[1.05rem] text-lg placeholder-[#7288aa] leading-normal focus:outline-none focus:border-[#72b3ff] focus:ring-2 focus:ring-[#72b3ff]/28 transition-all"
              />
            </div>
            <button
              type="submit"
              disabled={loading}
              className="w-full bg-gradient-to-r from-[#3f8ef6] to-[#3ba3f3] hover:from-[#5ba2ff] hover:to-[#51b3ff] disabled:opacity-60 text-white font-semibold py-[1.15rem] rounded-xl text-lg sm:text-xl transition-all flex items-center justify-center gap-2 mt-3 shadow-[0_16px_30px_-18px_rgba(79,156,251,0.95)]"
            >
              {loading ? <><Loader2 size={16} className="animate-spin" /> Signing in...</> : 'Sign in'}
            </button>
          </form>
        </div>

        <p className="text-center text-base text-[#7d96ba] mt-6">Default: admin@example.com / changeme123</p>
      </div>
    </div>
  )
}
