import type { PagedResponse, PageParams } from './types';

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

  const res = await fetch(`${path}?${qs}`, { signal });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${body.slice(0, 200)}`);
  }
  return res.json() as Promise<PagedResponse<T>>;
}
