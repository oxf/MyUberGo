import { useEffect, useRef, useState } from 'react';
import { CircleMarker, MapContainer, Popup, TileLayer } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import { apiGet } from '../api/client';
import type { LivePositionsResponse } from '../api/types';

const POLL_INTERVAL_MS = 5000;
// Prague — 50°05'15.71"N 14°25'15.08"E, the midpoint of the e2e-test
// simulator's actor bounding box (see services/e2e-test's coordinate bounds).
const DEFAULT_CENTER: [number, number] = [50.0877, 14.42086];

function relativeTime(iso: string): string {
  const seconds = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.round(seconds / 60);
  return `${minutes}m ago`;
}

// Fleet-wide live map for admins: every driver with a recent ping, plus
// every client currently inside an open ride tracking window (clients have
// no position outside that — see GET /api/location/positions). Polled, not
// pushed — location-service's WS hub is ride-scoped only, no observer role.
export function LivePositionsPage() {
  const [data, setData] = useState<LivePositionsResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const inFlight = useRef(false);

  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();

    async function poll() {
      if (inFlight.current) return;
      inFlight.current = true;
      try {
        const result = await apiGet<LivePositionsResponse>('/api/location/positions', controller.signal);
        if (!cancelled) {
          setData(result);
          setError(null);
        }
      } catch (err) {
        if (!cancelled && !(err instanceof DOMException && err.name === 'AbortError')) {
          setError(err instanceof Error ? err.message : String(err));
        }
      } finally {
        inFlight.current = false;
      }
    }

    poll();
    const intervalId = setInterval(poll, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(intervalId);
      controller.abort();
    };
  }, []);

  const drivers = data?.drivers ?? [];
  const clients = data?.clients ?? [];
  const empty = data !== null && drivers.length === 0 && clients.length === 0;

  return (
    <section>
      <h1>Live Map</h1>
      {error && <p className="error">{error}</p>}
      {empty && <p>No active drivers or clients right now.</p>}
      {/* center is the initial view only — react-leaflet's MapContainer doesn't
          re-center on prop changes after mount, so this stays fixed on Prague
          rather than jumping around as poll results reorder. */}
      <MapContainer center={DEFAULT_CENTER} zoom={12} style={{ height: '75vh', width: '100%' }}>
        <TileLayer
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          attribution="&copy; OpenStreetMap contributors"
        />
        {drivers.map((d) => (
          <CircleMarker key={`driver-${d.driverId}`} center={[d.lat, d.lon]} radius={7} pathOptions={{ color: '#2563eb', fillColor: '#2563eb', fillOpacity: 0.8 }}>
            <Popup>
              <div>
                <strong>Driver</strong>
                <br />
                {d.driverId}
                <br />
                Last seen {relativeTime(d.serverTs)}
              </div>
            </Popup>
          </CircleMarker>
        ))}
        {clients.map((c) => (
          <CircleMarker key={`client-${c.clientId}`} center={[c.lat, c.lon]} radius={7} pathOptions={{ color: '#ea580c', fillColor: '#ea580c', fillOpacity: 0.8 }}>
            <Popup>
              <div>
                <strong>Client</strong>
                <br />
                {c.clientId}
                <br />
                Ride {c.rideId}
                <br />
                Last seen {relativeTime(c.serverTs)}
              </div>
            </Popup>
          </CircleMarker>
        ))}
      </MapContainer>
    </section>
  );
}
