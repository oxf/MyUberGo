// Field names mirror the json tags in services/contracts/http — keep this
// file in sync whenever those contracts change.

export interface PagedResponse<T> {
  items: T[];
  page: number;
  pageSize: number;
  totalCount: number;
}

export type SortDir = 'asc' | 'desc';

export interface PageParams {
  page: number;
  pageSize: number;
  sortBy: string;
  sortDir: SortDir;
}

export interface UserDto {
  id: string;
  email: string;
  name: string;
  phone: string;
  role: 'Client' | 'Driver' | 'Admin';
  createdAt: string;
  // Only populated by GET /me for the caller's own profile.
  clientId?: string | null;
}

export interface DriverDto {
  id: string;
  userId: string;
  rating: number;
  vehicleType: string;
  licencePlate: string;
  status: string;
  totalRidesCompleted: number;
  createdAt: string;
}

export interface ShiftDto {
  id: string;
  driverId: string;
  startedAt: string;
  endedAt?: string | null;
  totalRides: number;
  totalEarnings: number;
}

export interface LocationDto {
  latitude: number;
  longitude: number;
  address: string;
}

export interface RideDto {
  id: string;
  clientId: string;
  driverId?: string | null;
  status: string;
  pickup: LocationDto;
  destination: LocationDto;
  estimatedPrice: number;
  estimatedDistanceKm: number;
  createdAt: string;
}
