import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom';
import App from './App';
import { UsersPage } from './pages/UsersPage';
import { DriversPage } from './pages/DriversPage';
import { ShiftsPage } from './pages/ShiftsPage';
import { RidesPage } from './pages/RidesPage';
import './index.css';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <BrowserRouter>
      <Routes>
        <Route element={<App />}>
          <Route index element={<Navigate to="/rides" replace />} />
          <Route path="/users" element={<UsersPage />} />
          <Route path="/drivers" element={<DriversPage />} />
          <Route path="/shifts" element={<ShiftsPage />} />
          <Route path="/rides" element={<RidesPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  </StrictMode>,
);
