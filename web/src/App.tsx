import { NavLink, Outlet } from 'react-router-dom';

export function App() {
  return (
    <div className="layout">
      <nav className="topnav">
        <span className="brand">MyUberGo admin</span>
        <NavLink to="/users">Users</NavLink>
        <NavLink to="/drivers">Drivers</NavLink>
        <NavLink to="/shifts">Shifts</NavLink>
        <NavLink to="/rides">Rides</NavLink>
      </nav>
      <main>
        <Outlet />
      </main>
    </div>
  );
}

export default App;
