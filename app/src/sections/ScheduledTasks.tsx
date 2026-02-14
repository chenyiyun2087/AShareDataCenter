import { useState, useEffect } from 'react';
import { Clock, Loader2 } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';

interface Job {
  id: string;
  name: string;
  next_run_time: string;
  trigger: string;
}

export function ScheduledTasks() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetchJobs();
  }, []);

  const fetchJobs = async () => {
    setLoading(true);
    try {
      const res = await fetch('/api/jobs');
      const json = await res.json();
      if (json.data) {
        setJobs(json.data);
      }
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Card className="border-border/50 bg-card/50 backdrop-blur">
      <CardHeader className="pb-4">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-purple-500/10">
            <Clock className="w-5 h-5 text-purple-400" />
          </div>
          <CardTitle className="text-lg font-semibold">定时任务</CardTitle>
        </div>
      </CardHeader>
      <CardContent>
        {loading ? (
          <div className="flex items-center justify-center p-4">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : (
          <div className="space-y-4">
            {jobs.map((job) => (
              <div key={job.id} className="flex items-center justify-between p-4 rounded-lg border border-border/50 bg-secondary/20">
                <div className="space-y-1">
                  <div className="font-medium">{job.name}</div>
                  <div className="text-sm text-muted-foreground">ID: {job.id}</div>
                </div>
                <div className="text-right space-y-1">
                  <Badge variant="outline" className="bg-purple-500/10 text-purple-400 border-purple-500/20">
                    Running
                  </Badge>
                  <div className="text-xs text-muted-foreground">下次: {job.next_run_time}</div>
                </div>
              </div>
            ))}
            {jobs.length === 0 && (
              <div className="text-center text-muted-foreground p-4">暂无定时任务</div>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
