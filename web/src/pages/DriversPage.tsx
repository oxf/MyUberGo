import { usePagedQuery } from '../hooks/usePagedQuery';
import { DataTable, type Column } from '../components/DataTable';
import { Pagination } from '../components/Pagination';
import type { DriverDto } from '../api/types';

// driver.driver no longer stores name/phone (auth.user is the single
// source now — see the role-table refactor notes in CLAUDE.md/PLAN.md),
// so this table can only show the userId, not a name.
const columns: Column<DriverDto>[] = [
  { key: 'id', header: 'ID', render: (d) => <code title={d.id}>{d.id.slice(0, 8)}</code> },
  { key: 'userId', header: 'User ID', render: (d) => <code title={d.userId}>{d.userId.slice(0, 8)}</code> },
  { key: 'rating', header: 'Rating', sortable: true, render: (d) => d.rating.toFixed(1) },
  { key: 'vehicleType', header: 'Vehicle', sortable: true, render: (d) => d.vehicleType },
  { key: 'licencePlate', header: 'Plate', render: (d) => d.licencePlate },
  { key: 'status', header: 'Status', sortable: true, render: (d) => d.status },
  { key: 'totalRidesCompleted', header: 'Rides', sortable: true, render: (d) => d.totalRidesCompleted },
  { key: 'createdAt', header: 'Created', sortable: true, render: (d) => new Date(d.createdAt).toLocaleString() },
];

export function DriversPage() {
  const q = usePagedQuery<DriverDto>('/api/driver/driver', { sortBy: 'createdAt' });
  return (
    <section>
      <h1>Drivers</h1>
      {q.error && <p className="error">{q.error}</p>}
      <DataTable columns={columns} rows={q.data?.items ?? []} rowKey={(d) => d.id} sortBy={q.params.sortBy} sortDir={q.params.sortDir} onSort={q.toggleSort} loading={q.loading} />
      <Pagination page={q.params.page} pageSize={q.params.pageSize} totalCount={q.data?.totalCount ?? 0} onPageChange={q.setPage} />
    </section>
  );
}
