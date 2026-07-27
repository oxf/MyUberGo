import { usePagedQuery } from '../hooks/usePagedQuery';
import { DataTable, type Column } from '../components/DataTable';
import { Pagination } from '../components/Pagination';
import type { ShiftDto } from '../api/types';
import { formatMoney } from '../lib/money';

const columns: Column<ShiftDto>[] = [
  { key: 'id', header: 'ID', render: (s) => <code title={s.id}>{s.id.slice(0, 8)}</code> },
  { key: 'driverId', header: 'Driver', render: (s) => <code title={s.driverId}>{s.driverId.slice(0, 8)}</code> },
  { key: 'startedAt', header: 'Started', sortable: true, render: (s) => new Date(s.startedAt).toLocaleString() },
  { key: 'endedAt', header: 'Ended', sortable: true, render: (s) => (s.endedAt ? new Date(s.endedAt).toLocaleString() : '—') },
  { key: 'totalRides', header: 'Rides', sortable: true, render: (s) => s.totalRides },
  { key: 'totalEarningsMinor', header: 'Earnings', sortable: true, render: (s) => formatMoney(s.totalEarningsMinor, s.currency) },
];

export function ShiftsPage() {
  const q = usePagedQuery<ShiftDto>('/api/driver/driver-shift', { sortBy: 'startedAt' });
  return (
    <section>
      <h1>Shifts</h1>
      {q.error && <p className="error">{q.error}</p>}
      <DataTable columns={columns} rows={q.data?.items ?? []} rowKey={(s) => s.id} sortBy={q.params.sortBy} sortDir={q.params.sortDir} onSort={q.toggleSort} loading={q.loading} />
      <Pagination page={q.params.page} pageSize={q.params.pageSize} totalCount={q.data?.totalCount ?? 0} onPageChange={q.setPage} />
    </section>
  );
}
