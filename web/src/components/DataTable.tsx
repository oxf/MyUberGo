import type { ReactNode } from 'react';
import type { SortDir } from '../api/types';

export interface Column<T> {
  /** API sort key — must match the endpoint's Go sort whitelist. */
  key: string;
  header: string;
  sortable?: boolean;
  render: (row: T) => ReactNode;
}

export interface DataTableProps<T> {
  columns: Column<T>[];
  rows: T[];
  rowKey: (row: T) => string;
  sortBy: string;
  sortDir: SortDir;
  onSort: (key: string) => void;
  loading?: boolean;
}

export function DataTable<T>({ columns, rows, rowKey, sortBy, sortDir, onSort, loading }: DataTableProps<T>) {
  return (
    <table className="data-table">
      <thead>
        <tr>
          {columns.map((col) => (
            <th key={col.key}>
              {col.sortable ? (
                <button type="button" className="sort-header" onClick={() => onSort(col.key)}>
                  {col.header}
                  {sortBy === col.key ? (sortDir === 'asc' ? ' ▲' : ' ▼') : ''}
                </button>
              ) : (
                col.header
              )}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.length === 0 ? (
          <tr>
            <td colSpan={columns.length} className="empty">
              {loading ? 'Loading…' : 'No data'}
            </td>
          </tr>
        ) : (
          rows.map((row) => (
            <tr key={rowKey(row)}>
              {columns.map((col) => (
                <td key={col.key}>{col.render(row)}</td>
              ))}
            </tr>
          ))
        )}
      </tbody>
    </table>
  );
}
