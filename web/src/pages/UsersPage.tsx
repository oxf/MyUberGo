import { usePagedQuery } from '../hooks/usePagedQuery';
import { DataTable, type Column } from '../components/DataTable';
import { Pagination } from '../components/Pagination';
import type { UserDto } from '../api/types';

const columns: Column<UserDto>[] = [
  { key: 'id', header: 'ID', render: (u) => <code title={u.id}>{u.id.slice(0, 8)}</code> },
  { key: 'email', header: 'Email', sortable: true, render: (u) => u.email },
  { key: 'name', header: 'Name', sortable: true, render: (u) => u.name },
  { key: 'phone', header: 'Phone', render: (u) => u.phone },
  { key: 'role', header: 'Role', sortable: true, render: (u) => u.role },
  { key: 'createdAt', header: 'Created', sortable: true, render: (u) => new Date(u.createdAt).toLocaleString() },
];

export function UsersPage() {
  const q = usePagedQuery<UserDto>('/api/auth/users', { sortBy: 'createdAt' });
  return (
    <section>
      <h1>Users</h1>
      {q.error && <p className="error">{q.error}</p>}
      <DataTable columns={columns} rows={q.data?.items ?? []} rowKey={(u) => u.id} sortBy={q.params.sortBy} sortDir={q.params.sortDir} onSort={q.toggleSort} loading={q.loading} />
      <Pagination page={q.params.page} pageSize={q.params.pageSize} totalCount={q.data?.totalCount ?? 0} onPageChange={q.setPage} />
    </section>
  );
}
