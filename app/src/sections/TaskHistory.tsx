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

interface TaskRecord {
  id: number;
  api: string;
  type: string;
  startTime: string;
  endTime: string;
  status: 'success' | 'error' | 'running';
  error?: string;
}

const taskRecords: TaskRecord[] = [
  {
    id: 1,
    api: 'stk_factor',
    type: 'incremental',
    startTime: '2026-01-31 14:30:00',
    endTime: '2026-01-31 14:32:15',
    status: 'success',
  },
  {
    id: 2,
    api: 'stk_daily',
    type: 'full',
    startTime: '2026-01-31 14:25:00',
    endTime: '2026-01-31 14:28:30',
    status: 'success',
  },
  {
    id: 3,
    api: 'stk_factor',
    type: 'incremental',
    startTime: '2026-01-31 14:20:00',
    endTime: '-',
    status: 'running',
  },
  {
    id: 4,
    api: 'stk_limit',
    type: 'full',
    startTime: '2026-01-31 14:15:00',
    endTime: '2026-01-31 14:15:45',
    status: 'error',
    error: 'Connection timeout',
  },
];

export function TaskHistory() {
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
          <Alert variant="destructive" className="border-red-500/30 bg-red-500/10">
            <AlertCircle className="h-4 w-4 text-red-400" />
            <AlertTitle className="text-red-400">读取日志失败</AlertTitle>
            <AlertDescription className="text-red-300/80">
              Failed to connect to MySQL. Check MYSQL_HOST/MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD/MYSQL_DB and verify credentials.
            </AlertDescription>
          </Alert>
          
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
                {taskRecords.length > 0 ? (
                  taskRecords.map((record) => (
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
