import { Header } from '@/sections/Header';
import { Dashboard } from '@/sections/Dashboard';
import { ODSStatus } from '@/sections/ODSStatus';
import { IndicatorSync } from '@/sections/IndicatorSync';
import { ManualTrigger } from '@/sections/ManualTrigger';
import { ScheduledTasks } from '@/sections/ScheduledTasks';
import { TaskHistory } from '@/sections/TaskHistory';
import { DataBrowser } from '@/sections/DataBrowser';

function App() {
  return (
    <div className="min-h-screen bg-background">
      <Header />
      <main className="pb-12">
        <Dashboard />
        <ODSStatus />
        <IndicatorSync />
        <ManualTrigger />
        <ScheduledTasks />
        <TaskHistory />
        <DataBrowser />
      </main>
    </div>
  );
}

export default App;
