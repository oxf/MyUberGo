import { usePagedQuery } from '../hooks/usePagedQuery';
import { DataTable, type Column } from '../components/DataTable';
import { Pagination } from '../components/Pagination';
import type { InvoiceDto } from '../api/types';
import { formatMoney } from '../lib/money';

const columns: Column<InvoiceDto>[] = [
  { key: 'id', header: 'ID', render: (i) => <code title={i.id}>{i.id.slice(0, 8)}</code> },
  { key: 'rideId', header: 'Ride', render: (i) => <code title={i.rideId}>{i.rideId.slice(0, 8)}</code> },
  { key: 'clientId', header: 'Client', render: (i) => <code title={i.clientId}>{i.clientId.slice(0, 8)}</code> },
  { key: 'type', header: 'Type', render: (i) => i.type },
  { key: 'status', header: 'Status', sortable: true, render: (i) => i.status },
  { key: 'amountMinor', header: 'Amount', sortable: true, render: (i) => formatMoney(i.amountMinor, i.currency) },
  { key: 'attemptCount', header: 'Attempts', render: (i) => i.attemptCount },
  { key: 'createdAt', header: 'Created', sortable: true, render: (i) => new Date(i.createdAt).toLocaleString() },
];

export function InvoicesPage() {
  const q = usePagedQuery<InvoiceDto>('/api/billing/invoices', { sortBy: 'createdAt' });
  return (
    <section>
      <h1>Invoices</h1>
      {q.error && <p className="error">{q.error}</p>}
      <DataTable columns={columns} rows={q.data?.items ?? []} rowKey={(i) => i.id} sortBy={q.params.sortBy} sortDir={q.params.sortDir} onSort={q.toggleSort} loading={q.loading} />
      <Pagination page={q.params.page} pageSize={q.params.pageSize} totalCount={q.data?.totalCount ?? 0} onPageChange={q.setPage} />
    </section>
  );
}
