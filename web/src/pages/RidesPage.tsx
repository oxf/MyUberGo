import { usePagedQuery } from '../hooks/usePagedQuery';
import { DataTable, type Column } from '../components/DataTable';
import { Pagination } from '../components/Pagination';
import type { RideDto } from '../api/types';
import { formatMoney } from '../lib/money';

const columns: Column<RideDto>[] = [
  { key: 'id', header: 'ID', render: (r) => <code title={r.id}>{r.id.slice(0, 8)}</code> },
  { key: 'clientId', header: 'Client', render: (r) => <code title={r.clientId}>{r.clientId.slice(0, 8)}</code> },
  { key: 'driverId', header: 'Driver', render: (r) => (r.driverId ? <code title={r.driverId}>{r.driverId.slice(0, 8)}</code> : '—') },
  { key: 'status', header: 'Status', sortable: true, render: (r) => r.status },
  { key: 'pickup', header: 'Pickup', render: (r) => r.pickup.address },
  { key: 'destination', header: 'Destination', render: (r) => r.destination.address },
  { key: 'estimatedPriceMinor', header: 'Price', sortable: true, render: (r) => formatMoney(r.estimatedPriceMinor, r.currency) },
  { key: 'estimatedDistanceKm', header: 'Km', sortable: true, render: (r) => r.estimatedDistanceKm.toFixed(2) },
  { key: 'createdAt', header: 'Created', sortable: true, render: (r) => new Date(r.createdAt).toLocaleString() },
];

export function RidesPage() {
  const q = usePagedQuery<RideDto>('/api/ride/ride', { sortBy: 'createdAt' });
  return (
    <section>
      <h1>Rides</h1>
      {q.error && <p className="error">{q.error}</p>}
      <DataTable columns={columns} rows={q.data?.items ?? []} rowKey={(r) => r.id} sortBy={q.params.sortBy} sortDir={q.params.sortDir} onSort={q.toggleSort} loading={q.loading} />
      <Pagination page={q.params.page} pageSize={q.params.pageSize} totalCount={q.data?.totalCount ?? 0} onPageChange={q.setPage} />
    </section>
  );
}
