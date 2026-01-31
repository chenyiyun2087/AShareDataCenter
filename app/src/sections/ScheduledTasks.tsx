import { useCallback, useEffect, useState } from 'react';
import { motion } from 'framer-motion';
import { Clock, Plus, Trash2 } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { GradientButton } from '@/components/GradientButton';
import { Button } from '@/components/ui/button';
import { deleteJson, getJson, postJson } from '@/lib/api';

interface ScheduledTask {
  id: string;
  next_run_time: string | null;
  trigger: string;
}

interface ScheduleForm {
  layer: string;
  mode: string;
  cron_hour: string;
  cron_minute: string;
  token: string;
  start_date: string;
  fina_start: string;
  fina_end: string;
  rate_limit: string;
}

export function ScheduledTasks() {
  const [tasks, setTasks] = useState<ScheduledTask[]>([]);
  const [form, setForm] = useState<ScheduleForm>({
    layer: 'base',
    mode: 'incremental',
    cron_hour: '2',
    cron_minute: '0',
    token: '',
    start_date: '20100101',
    fina_start: '',
    fina_end: '',
    rate_limit: '500',
  });
  const [status, setStatus] = useState<{ type: 'success' | 'error'; message: string } | null>(
    null,
  );
  const [loading, setLoading] = useState(false);

  const fetchTasks = useCallback(async () => {
    setLoading(true);
    setStatus(null);
    try {
      const response = await getJson<{ data: ScheduledTask[] }>('/api/tasks/schedule');
      setTasks(response.data);
    } catch (err) {
      const message = err instanceof Error ? err.message : '加载失败';
      setStatus({ type: 'error', message });
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchTasks();
  }, [fetchTasks]);

  const handleChange = (key: keyof ScheduleForm) => (value: string) => {
    setForm((prev) => ({ ...prev, [key]: value }));
  };

  const handleSubmit = async () => {
    setLoading(true);
    setStatus(null);
    try {
      await postJson('/api/tasks/schedule', form);
      setStatus({ type: 'success', message: '定时任务已添加。' });
      await fetchTasks();
    } catch (err) {
      const message = err instanceof Error ? err.message : '添加失败';
      setStatus({ type: 'error', message });
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = async (jobId: string) => {
    setLoading(true);
    setStatus(null);
    try {
      await deleteJson(`/api/tasks/schedule/${jobId}`);
      await fetchTasks();
    } catch (err) {
      const message = err instanceof Error ? err.message : '删除失败';
      setStatus({ type: 'error', message });
    } finally {
      setLoading(false);
    }
  };

  return (
    <motion.section
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.6 }}
      className="px-6 py-4"
    >
      <Card className="border-border/50 bg-card/50 backdrop-blur">
        <CardHeader className="pb-4">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-lg bg-amber-500/10">
              <Clock className="w-5 h-5 text-amber-400" />
            </div>
            <CardTitle className="text-lg font-semibold">定时任务</CardTitle>
          </div>
        </CardHeader>
        <CardContent className="space-y-6">
          {/* Cron Configuration */}
          <div className="p-4 rounded-xl bg-secondary/30 border border-border/30">
            <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-6 gap-4 mb-4">
              <div className="space-y-2">
                <Label className="text-sm text-muted-foreground">层级</Label>
                <Select value={form.layer} onValueChange={handleChange('layer')}>
                  <SelectTrigger className="bg-secondary/50 border-border/50">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent className="bg-card border-border/50">
                    <SelectItem value="base">base</SelectItem>
                    <SelectItem value="ods">ods</SelectItem>
                    <SelectItem value="dwd">dwd</SelectItem>
                    <SelectItem value="dws">dws</SelectItem>
                    <SelectItem value="ads">ads</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              
              <div className="space-y-2">
                <Label className="text-sm text-muted-foreground">模式</Label>
                <Select value={form.mode} onValueChange={handleChange('mode')}>
                  <SelectTrigger className="bg-secondary/50 border-border/50">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent className="bg-card border-border/50">
                    <SelectItem value="incremental">incremental</SelectItem>
                    <SelectItem value="full">full</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              
              <div className="space-y-2">
                <Label htmlFor="cron-hour" className="text-sm text-muted-foreground">
                  Cron Hour
                </Label>
                <Input
                  id="cron-hour"
                  type="number"
                  min={0}
                  max={23}
                  value={form.cron_hour}
                  onChange={(event) => handleChange('cron_hour')(event.target.value)}
                  className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20"
                />
              </div>
              
              <div className="space-y-2">
                <Label htmlFor="cron-minute" className="text-sm text-muted-foreground">
                  Cron Minute
                </Label>
                <Input
                  id="cron-minute"
                  type="number"
                  min={0}
                  max={59}
                  value={form.cron_minute}
                  onChange={(event) => handleChange('cron_minute')(event.target.value)}
                  className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20"
                />
              </div>
              
              <div className="space-y-2">
                <Label htmlFor="cron-token" className="text-sm text-muted-foreground">
                  Token
                </Label>
                <Input
                  id="cron-token"
                  placeholder="Token"
                  className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20"
                  value={form.token}
                  onChange={(event) => handleChange('token')(event.target.value)}
                />
              </div>
              
              <div className="space-y-2">
                <Label htmlFor="cron-rate" className="text-sm text-muted-foreground">
                  Rate Limit
                </Label>
                <Input
                  id="cron-rate"
                  type="number"
                  value={form.rate_limit}
                  onChange={(event) => handleChange('rate_limit')(event.target.value)}
                  className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20"
                />
              </div>
            </div>
            
            <div className="flex justify-end">
              <GradientButton variant="primary" size="sm" onClick={handleSubmit} disabled={loading}>
                <Plus className="w-4 h-4 mr-2" />
                {loading ? '提交中...' : '添加定时任务'}
              </GradientButton>
            </div>
          </div>
          
          {/* Scheduled Tasks Table */}
          <div>
            <h4 className="text-sm font-medium text-muted-foreground mb-3">已安排任务</h4>
            <div className="rounded-xl border border-border/50 overflow-hidden">
              <Table>
                <TableHeader>
                  <TableRow className="bg-secondary/50 hover:bg-secondary/50">
                    <TableHead className="text-muted-foreground">ID</TableHead>
                    <TableHead className="text-muted-foreground">Next Run</TableHead>
                    <TableHead className="text-muted-foreground">Trigger</TableHead>
                    <TableHead className="text-muted-foreground text-right">操作</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {tasks.length > 0 ? (
                    tasks.map((task) => (
                      <TableRow key={task.id} className="hover:bg-secondary/30">
                        <TableCell className="font-mono">{task.id}</TableCell>
                        <TableCell className="font-mono">
                          {task.next_run_time || '-'}
                        </TableCell>
                        <TableCell>{task.trigger}</TableCell>
                        <TableCell className="text-right">
                          <Button
                            variant="ghost"
                            size="sm"
                            className="text-red-400 hover:text-red-300 hover:bg-red-500/10"
                            onClick={() => handleDelete(task.id)}
                            disabled={loading}
                          >
                            <Trash2 className="w-4 h-4" />
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))
                  ) : (
                    <TableRow>
                      <TableCell colSpan={4} className="text-center text-muted-foreground py-8">
                        暂无任务
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>
            {status ? (
              <div
                className={`mt-4 text-sm ${
                  status.type === 'success' ? 'text-emerald-400' : 'text-red-400'
                }`}
              >
                {status.message}
              </div>
            ) : null}
          </div>
        </CardContent>
      </Card>
    </motion.section>
  );
}
