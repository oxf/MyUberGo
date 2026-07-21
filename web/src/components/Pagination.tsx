export interface PaginationProps {
  page: number;
  pageSize: number;
  totalCount: number;
  onPageChange: (page: number) => void;
}

export function Pagination({ page, pageSize, totalCount, onPageChange }: PaginationProps) {
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  const from = Math.max(1, page - 2);
  const to = Math.min(totalPages, page + 2);
  const pages: number[] = [];
  for (let p = from; p <= to; p++) pages.push(p);

  return (
    <nav className="pagination">
      <button type="button" disabled={page <= 1} onClick={() => onPageChange(page - 1)}>
        ‹ Prev
      </button>
      {from > 1 && (
        <>
          <button type="button" onClick={() => onPageChange(1)}>1</button>
          {from > 2 && <span>…</span>}
        </>
      )}
      {pages.map((p) => (
        <button key={p} type="button" disabled={p === page} className={p === page ? 'current' : ''} onClick={() => onPageChange(p)}>
          {p}
        </button>
      ))}
      {to < totalPages && (
        <>
          {to < totalPages - 1 && <span>…</span>}
          <button type="button" onClick={() => onPageChange(totalPages)}>{totalPages}</button>
        </>
      )}
      <button type="button" disabled={page >= totalPages} onClick={() => onPageChange(page + 1)}>
        Next ›
      </button>
      <span className="total">{totalCount} rows</span>
    </nav>
  );
}
