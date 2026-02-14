import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { MainLayout } from '@/layouts/MainLayout';
import { Dashboard } from '@/sections/Dashboard';
import { ScheduledTasks } from '@/sections/ScheduledTasks';
import { TaskHistory } from '@/sections/TaskHistory';
import { DataBrowser } from '@/sections/DataBrowser';

import Pipeline from '@/pages/jobs/Pipeline';
import Scheduling from '@/pages/jobs/Scheduling';
import Checks from '@/pages/jobs/Checks';

import Backfill from '@/pages/jobs/Backfill';

const MonitoringTasks = () => (
  <div className="space-y-6">
    <h1 className="text-3xl font-bold tracking-tight">任务状态</h1>
    <ScheduledTasks />
    <TaskHistory />
  </div>
);

const MonitoringFailures = () => (
  <div className="space-y-6">
    <h1 className="text-3xl font-bold tracking-tight">失败统计</h1>
    <div className="rounded-lg border bg-card text-card-foreground shadow-sm p-6">
      <p className="text-muted-foreground">失败统计看板开发中...</p>
    </div>
  </div>
);

const MonitoringSLO = () => (
  <div className="space-y-6">
    <h1 className="text-3xl font-bold tracking-tight">指标看板</h1>
    <div className="rounded-lg border bg-card text-card-foreground shadow-sm p-6">
      <p className="text-muted-foreground">SLO 指标看板开发中...</p>
    </div>
  </div>
);

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<MainLayout />}>
          <Route index element={
            <div className="space-y-6">
              <h1 className="text-3xl font-bold tracking-tight">平台首页</h1>
              <Dashboard />
              <DataBrowser />
            </div>
          } />

          <Route path="jobs">
            <Route index element={<Navigate to="/jobs/pipeline" replace />} />
            <Route path="pipeline" element={<Pipeline />} />
            <Route path="scheduling" element={<Scheduling />} />
            <Route path="checks" element={<Checks />} />
            <Route path="backfill" element={<Backfill />} />
          </Route>

          <Route path="monitoring">
            <Route index element={<Navigate to="/monitoring/tasks" replace />} />
            <Route path="tasks" element={<MonitoringTasks />} />
            <Route path="failures" element={<MonitoringFailures />} />
            <Route path="slo" element={<MonitoringSLO />} />
          </Route>
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default App;
