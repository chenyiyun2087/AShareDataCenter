import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { MainLayout } from '@/layouts/MainLayout';
import NetworkConsole from '@/pages/NetworkConsole';

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<MainLayout />}>
          <Route index element={<NetworkConsole />} />
          <Route path="console/network" element={<NetworkConsole />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default App;
