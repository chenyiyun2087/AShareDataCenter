import { useEffect, useState } from 'react';
import { motion } from 'framer-motion';
import { History, AlertCircle } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { StatusBadge } from '@/components/StatusBadge';
import { getJson } from '@/lib/api';

interface TaskRecord {
  id: number;
  api: string;
  type: string;
  startTime: string;
  endTime: string;
  status: 'success' | 'error' | 'running';
  error?: string;
}

export function TaskHistory() {
  const [records, setRecords] = useState<TaskRecord[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const load = async () => {
      try {
        const response = await getJson<{ data: Array<{
          id: number;
          api: string;
          type: string;
          start_time: string | null;
          end_time: string | null;
          status: string;
          error: string | null;
        }> }>('/api/tasks/history');
        setRecords(
          response.data.map((item) => ({
            id: item.id,
            api: item.api,
            type: item.type,
            startTime: item.start_time || '-',
            endTime: item.end_time || '-',
            status:
              item.status === 'SUCCESS'
                ? 'success'
                : item.status === 'FAILED'
                  ? 'error'
                  : 'running',
            error: item.error || undefined,
          })),
        );
      } catch (err) {
        const message = err instanceof Error ? err.message : '加载失败';
        setError(message);
      }
    };
    load();
  }, []);

  return (
    <motion.section
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.7 }}
      className="px-6 py-4"
    >
      <Card className="border-border/50 bg-card/50 backdrop-blur">
        <CardHeader className="pb-4">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-lg bg-purple-500/10">
              <History className="w-5 h-5 text-purple-400" />
            </div>
            <CardTitle className="text-lg font-semibold">最近任务执行情况</CardTitle>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          {error ? (
            <Alert variant="destructive" className="border-red-500/30 bg-red-500/10">
              <AlertCircle className="h-4 w-4 text-red-400" />
              <AlertTitle className="text-red-400">读取日志失败</AlertTitle>
              <AlertDescription className="text-red-300/80">{error}</AlertDescription>
            </Alert>
          ) : null}
          
          <div className="rounded-xl border border-border/50 overflow-hidden">
            <Table>
              <TableHeader>
                <TableRow className="bg-secondary/50 hover:bg-secondary/50">
                  <TableHead className="text-muted-foreground">ID</TableHead>
                  <TableHead className="text-muted-foreground">API</TableHead>
                  <TableHead className="text-muted-foreground">类型</TableHead>
                  <TableHead className="text-muted-foreground">开始时间</TableHead>
                  <TableHead className="text-muted-foreground">结束时间</TableHead>
                  <TableHead className="text-muted-foreground">状态</TableHead>
                  <TableHead className="text-muted-foreground">错误</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {records.length > 0 ? (
                  records.map((record) => (
                    <TableRow key={record.id} className="hover:bg-secondary/30">
                      <TableCell className="font-mono">{record.id}</TableCell>
                      <TableCell className="font-mono text-sky-400">{record.api}</TableCell>
                      <TableCell>
                        <span className="px-2 py-1 rounded-md bg-secondary text-xs">
                          {record.type}
                        </span>
                      </TableCell>
                      <TableCell className="font-mono text-sm">{record.startTime}</TableCell>
                      <TableCell className="font-mono text-sm">{record.endTime}</TableCell>
                      <TableCell>
                        <StatusBadge status={record.status}>
                          {record.status === 'success' && '成功'}
                          {record.status === 'error' && '失败'}
                          {record.status === 'running' && '运行中'}
                        </StatusBadge>
                      </TableCell>
                      <TableCell className="text-red-400 text-sm">
                        {record.error || '-'}
                      </TableCell>
                    </TableRow>
                  ))
                ) : (
                  <TableRow>
                    <TableCell colSpan={7} className="text-center text-muted-foreground py-8">
                      暂无记录
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>
    </motion.section>
  );
}
