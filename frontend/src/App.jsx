import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { AuthProvider, useAuth } from './context/AuthContext'
import Layout from './components/Layout'
import Login from './pages/Login'
import Dashboard from './pages/Dashboard'
import AuditDetail from './pages/AuditDetail'
import Diff from './pages/Diff'
import Schedules from './pages/Schedules'
import GscSettings from './pages/GscSettings'

function RequireAuth({ children }) {
  const { user, loading } = useAuth()
  if (loading) return <div className="min-h-screen" />
  if (!user) return <Navigate to="/login" replace />
  return <Layout>{children}</Layout>
}

export default function App() {
  return (
    <AuthProvider>
      <BrowserRouter basename="/app">
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route path="/" element={<RequireAuth><Dashboard /></RequireAuth>} />
          <Route path="/audit/:id" element={<RequireAuth><AuditDetail /></RequireAuth>} />
          <Route path="/diff" element={<RequireAuth><Diff /></RequireAuth>} />
          <Route path="/schedules" element={<RequireAuth><Schedules /></RequireAuth>} />
          <Route path="/settings/gsc" element={<RequireAuth><GscSettings /></RequireAuth>} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </BrowserRouter>
    </AuthProvider>
  )
}
