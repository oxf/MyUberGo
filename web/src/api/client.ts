import type { PagedResponse, PageParams } from './types';
import { getAccessToken, clearAccessToken } from './auth';

// Dispatched whenever a request comes back 401, so the login gate in App.tsx
// can drop back to the sign-in form (e.g. once the 15-minute access token
// issued by auth-service expires).
export const UNAUTHORIZED_EVENT = 'myubergo:unauthorized';

// Dispatched on 403: every list endpoint this dashboard uses is now
// Admin-only at the Kong gateway (see gateway/kong.yml), so a successful
// login with a non-Admin token still can't see any data. App.tsx treats
// this the same as UNAUTHORIZED_EVENT but with a role-specific message.
export const FORBIDDEN_EVENT = 'myubergo:forbidden';

function withAuthHeaders(): HeadersInit | undefined {
  const token = getAccessToken();
  return token ? { Authorization: `Bearer ${token}` } : undefined;
}

function handleAuthStatus(res: Response): void {
  if (res.status === 401) {
    clearAccessToken();
    window.dispatchEvent(new Event(UNAUTHORIZED_EVENT));
  }
  if (res.status === 403) {
    clearAccessToken();
    window.dispatchEvent(new Event(FORBIDDEN_EVENT));
  }
}

export async function fetchPaged<T>(
  path: string,
  params: PageParams,
  signal?: AbortSignal,
): Promise<PagedResponse<T>> {
  const qs = new URLSearchParams({
    page: String(params.page),
    pageSize: String(params.pageSize),
    sortBy: params.sortBy,
    sortDir: params.sortDir,
  });

  const res = await fetch(`${path}?${qs}`, { signal, headers: withAuthHeaders() });
  handleAuthStatus(res);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${body.slice(0, 200)}`);
  }
  return res.json() as Promise<PagedResponse<T>>;
}

// apiGet is for small, non-paged snapshots (e.g. the admin live-positions
// map) — fetchPaged's querystring/PagedResponse shape doesn't apply.
export async function apiGet<T>(path: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(path, { signal, headers: withAuthHeaders() });
  handleAuthStatus(res);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${body.slice(0, 200)}`);
  }
  return res.json() as Promise<T>;
}
