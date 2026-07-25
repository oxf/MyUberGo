import { useEffect, useState, type FormEvent } from 'react';
import { NavLink, Outlet } from 'react-router-dom';
import { getAccessToken, login } from './api/auth';
import { UNAUTHORIZED_EVENT } from './api/client';

// Gates the dashboard behind a login form: the list endpoints sit behind
// Kong's jwt plugin now, so there's no more anonymous access (see
// gateway/kong.yml). Auth state is just "do we have an access token" —
// tracked here, not in a separate store, since this is the only place that
// needs to react to it.
export function App() {
  const [authed, setAuthed] = useState(() => !!getAccessToken());
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    const onUnauthorized = () => setAuthed(false);
    window.addEventListener(UNAUTHORIZED_EVENT, onUnauthorized);
    return () => window.removeEventListener(UNAUTHORIZED_EVENT, onUnauthorized);
  }, []);

  if (!authed) {
    const handleSubmit = async (e: FormEvent) => {
      e.preventDefault();
      setSubmitting(true);
      setError(null);
      try {
        await login(email, password);
        setAuthed(true);
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        setSubmitting(false);
      }
    };

    return (
      <div className="login-screen">
        <form className="login-form" onSubmit={handleSubmit}>
          <h1>MyUberGo admin</h1>
          <label>
            Email
            <input value={email} onChange={(e) => setEmail(e.target.value)} type="email" required autoFocus />
          </label>
          <label>
            Password
            <input value={password} onChange={(e) => setPassword(e.target.value)} type="password" required />
          </label>
          {error && <p className="error">{error}</p>}
          <button type="submit" disabled={submitting}>
            {submitting ? 'Signing in…' : 'Sign in'}
          </button>
        </form>
      </div>
    );
  }

  return (
    <div className="layout">
      <nav className="topnav">
        <span className="brand">MyUberGo admin</span>
        <NavLink to="/users">Users</NavLink>
        <NavLink to="/drivers">Drivers</NavLink>
        <NavLink to="/shifts">Shifts</NavLink>
        <NavLink to="/rides">Rides</NavLink>
      </nav>
      <main>
        <Outlet />
      </main>
    </div>
  );
}

export default App;
