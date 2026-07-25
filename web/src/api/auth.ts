// The list endpoints now sit behind Kong's jwt plugin (see gateway/kong.yml),
// so the dashboard needs to log in and attach a bearer token like any other
// client. Only the access token is kept — this is a read-only admin
// dashboard, not a full session manager, so there's no refresh-token
// rotation; when the 15-minute access token expires the user just signs in
// again (see the 401 handling in ./client.ts).

const TOKEN_KEY = 'myubergo_access_token';

export function getAccessToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}

export function clearAccessToken(): void {
  localStorage.removeItem(TOKEN_KEY);
}

export async function login(email: string, password: string): Promise<void> {
  const res = await fetch('/api/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${body.slice(0, 200)}`);
  }
  const data = (await res.json()) as { accessToken: string };
  localStorage.setItem(TOKEN_KEY, data.accessToken);
}
