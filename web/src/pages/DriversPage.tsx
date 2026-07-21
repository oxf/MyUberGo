import { usePagedQuery } from '../hooks/usePagedQuery';
import { DataTable, type Column } from '../components/DataTable';
import { Pagination } from '../components/Pagination';
import type { DriverProfileDto } from '../api/types';

const columns: Column<DriverProfileDto>[] = [
  { key: 'id', header: 'ID', render: (d) => <code title={d.id}>{d.id.slice(0, 8)}</code> },
  { key: 'driverName', header: 'Name', sortable: true, render: (d) => d.driverName },
  { key: 'phone', header: 'Phone', render: (d) => d.phone },
  { key: 'rating', header: 'Rating', sortable: true, render: (d) => d.rating.toFixed(1) },
  { key: 'vehicleType', header: 'Vehicle', sortable: true, render: (d) => d.vehicleType },
  { key: 'licencePlate', header: 'Plate', render: (d) => d.licencePlate },
  { key: 'status', header: 'Status', sortable: true, render: (d) => d.status },
  { key: 'totalRidesCompleted', header: 'Rides', sortable: true, render: (d) => d.totalRidesCompleted },
  { key: 'createdAt', header: 'Created', sortable: true, render: (d) => new Date(d.createdAt).toLocaleString() },
];

export function DriversPage() {
  const q = usePagedQuery<DriverProfileDto>('/api/driver/driver-profile', { sortBy: 'createdAt' });
  return (
    <section>
      <h1>Driver Profiles</h1>
      {q.error && <p className="error">{q.error}</p>}
      <DataTable columns={columns} rows={q.data?.items ?? []} rowKey={(d) => d.id} sortBy={q.params.sortBy} sortDir={q.params.sortDir} onSort={q.toggleSort} loading={q.loading} />
      <Pagination page={q.params.page} pageSize={q.params.pageSize} totalCount={q.data?.totalCount ?? 0} onPageChange={q.setPage} />
    </section>
  );
}
