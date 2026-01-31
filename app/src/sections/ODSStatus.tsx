import { useCallback, useEffect, useState } from 'react';
import { motion } from 'framer-motion';
import { AlertCircle, RefreshCw, Server } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { StatusBadge } from '@/components/StatusBadge';
import { GradientButton } from '@/components/GradientButton';
import { Progress } from '@/components/ui/progress';
import { getJson } from '@/lib/api';

interface OdsStatusResponse {
  data: {
    status: string;
    start_at: string | null;
    end_at: string | null;
    duration: number | null;
  } | null;
}

export function ODSStatus() {
  const [status, setStatus] = useState<OdsStatusResponse['data']>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [attempts, setAttempts] = useState(0);

  const fetchStatus = useCallback(async () => {
    setLoading(true);
    setError(null);
    setAttempts((prev) => prev + 1);
    try {
      const response = await getJson<OdsStatusResponse>('/api/ods/status');
      setStatus(response.data);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Unknown error';
      setError(message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchStatus();
  }, [fetchStatus]);

  const statusLabel = status?.status ?? '未知';
  const hasError = Boolean(error);
  const normalizedStatus = status?.status?.toUpperCase();
  const badgeStatus = hasError
    ? 'error'
    : normalizedStatus === 'SUCCESS'
      ? 'success'
      : normalizedStatus === 'FAILED'
        ? 'error'
        : 'running';

  return (
    <motion.section
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.3 }}
      className="px-6 py-4"
    >
      <Card className="border-border/50 bg-card/50 backdrop-blur">
        <CardHeader className="pb-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-sky-500/10">
                <Server className="w-5 h-5 text-sky-400" />
              </div>
              <CardTitle className="text-lg font-semibold">ODS 执行状态</CardTitle>
            </div>
            <StatusBadge status={badgeStatus}>{hasError ? '连接失败' : statusLabel}</StatusBadge>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          {hasError ? (
            <Alert variant="destructive" className="border-red-500/30 bg-red-500/10">
              <AlertCircle className="h-4 w-4 text-red-400" />
              <AlertTitle className="text-red-400">连接错误</AlertTitle>
              <AlertDescription className="text-red-300/80">
                读取 ODS 状态失败：{error}
              </AlertDescription>
            </Alert>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4 text-sm text-muted-foreground">
              <div>
                <div className="text-xs uppercase mb-1">开始时间</div>
                <div className="text-foreground">{status?.start_at || '暂无'}</div>
              </div>
              <div>
                <div className="text-xs uppercase mb-1">结束时间</div>
                <div className="text-foreground">{status?.end_at || '执行中'}</div>
              </div>
              <div>
                <div className="text-xs uppercase mb-1">耗时</div>
                <div className="text-foreground">{status?.duration ?? 0} 秒</div>
              </div>
              <div>
                <div className="text-xs uppercase mb-1">状态</div>
                <div className="text-foreground">{statusLabel}</div>
              </div>
            </div>
          )}
          
          <div className="space-y-2">
            <div className="flex justify-between text-sm">
              <span className="text-muted-foreground">连接重试</span>
              <span className="text-sky-400">{Math.min(attempts, 5)} / 5</span>
            </div>
            <Progress value={Math.min((attempts / 5) * 100, 100)} className="h-2 bg-secondary" />
          </div>
          
          <div className="flex justify-end">
            <GradientButton variant="primary" size="sm" onClick={fetchStatus} disabled={loading}>
              <RefreshCw className="w-4 h-4 mr-2" />
              {loading ? '加载中' : '重试连接'}
            </GradientButton>
          </div>
        </CardContent>
      </Card>
    </motion.section>
  );
}
