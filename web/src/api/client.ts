import type { PagedResponse, PageParams } from './types';
import { getAccessToken, clearAccessToken } from './auth';

// Dispatched whenever a request comes back 401, so the login gate in App.tsx
// can drop back to the sign-in form (e.g. once the 15-minute access token
// issued by auth-service expires).
export const UNAUTHORIZED_EVENT = 'myubergo:unauthorized';

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

  const token = getAccessToken();
  const res = await fetch(`${path}?${qs}`, {
    signal,
    headers: token ? { Authorization: `Bearer ${token}` } : undefined,
  });
  if (res.status === 401) {
    clearAccessToken();
    window.dispatchEvent(new Event(UNAUTHORIZED_EVENT));
  }
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${body.slice(0, 200)}`);
  }
  return res.json() as Promise<PagedResponse<T>>;
}
