import { useEffect, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { fetchPaged } from '../api/client';
import type { PagedResponse, PageParams, SortDir } from '../api/types';

export interface UsePagedQueryResult<T> {
  data: PagedResponse<T> | null;
  loading: boolean;
  error: string | null;
  params: PageParams;
  setPage: (page: number) => void;
  toggleSort: (sortBy: string) => void;
}

// Server-side paged + sorted fetch whose page/sort state lives in the URL
// query string, so every table view is bookmarkable and survives refresh.
export function usePagedQuery<T>(
  path: string,
  defaults: { sortBy: string; sortDir?: SortDir; pageSize?: number },
): UsePagedQueryResult<T> {
  const [searchParams, setSearchParams] = useSearchParams();

  const params: PageParams = {
    page: Math.max(1, Number(searchParams.get('page') ?? '1') || 1),
    pageSize: defaults.pageSize ?? 20,
    sortBy: searchParams.get('sortBy') ?? defaults.sortBy,
    sortDir: (searchParams.get('sortDir') as SortDir | null) ?? defaults.sortDir ?? 'desc',
  };

  const [data, setData] = useState<PagedResponse<T> | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    fetchPaged<T>(path, params, controller.signal)
      .then(setData)
      .catch((err: unknown) => {
        if (!controller.signal.aborted) {
          setError(err instanceof Error ? err.message : String(err));
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [path, params.page, params.pageSize, params.sortBy, params.sortDir]);

  const setPage = (page: number) => {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      next.set('page', String(page));
      return next;
    });
  };

  // Same column: flip direction. New column: sort by it descending. Either
  // way jump back to page 1 — the old offset is meaningless under a new order.
  const toggleSort = (sortBy: string) => {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (params.sortBy === sortBy) {
        next.set('sortDir', params.sortDir === 'desc' ? 'asc' : 'desc');
      } else {
        next.set('sortBy', sortBy);
        next.set('sortDir', 'desc');
      }
      next.set('page', '1');
      return next;
    });
  };

  return { data, loading, error, params, setPage, toggleSort };
}
