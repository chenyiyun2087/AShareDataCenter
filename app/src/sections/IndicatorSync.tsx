import { useState } from 'react';
import { motion } from 'framer-motion';
import { BarChart3, Info } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { GradientButton } from '@/components/GradientButton';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { postJson } from '@/lib/api';

interface SyncPayload {
  token: string;
  config: string;
  start_date: string;
  end_date: string;
  rate_limit: string;
  cyq_rate_limit: string;
  apis: string;
}

export function IndicatorSync() {
  const today = new Date();
  const todayString = `${today.getFullYear()}${String(today.getMonth() + 1).padStart(2, '0')}${String(
    today.getDate(),
  ).padStart(2, '0')}`;
  const [form, setForm] = useState<SyncPayload>({
    token: '',
    config: 'config/etl.ini',
    start_date: todayString,
    end_date: todayString,
    rate_limit: '500',
    cyq_rate_limit: '180',
    apis: 'stk_factor',
  });
  const [status, setStatus] = useState<{ type: 'success' | 'error'; message: string } | null>(
    null,
  );
  const [submitting, setSubmitting] = useState(false);

  const handleChange = (key: keyof SyncPayload) => (value: string) => {
    setForm((prev) => ({ ...prev, [key]: value }));
  };

  const handleSubmit = async () => {
    setSubmitting(true);
    setStatus(null);
    try {
      await postJson('/api/ods/features-run', {
        ...form,
      });
      setStatus({ type: 'success', message: '已提交指标同步任务。' });
    } catch (err) {
      const message = err instanceof Error ? err.message : '提交失败';
      setStatus({ type: 'error', message });
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <motion.section
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.4 }}
      className="px-6 py-4"
    >
      <Card className="border-border/50 bg-card/50 backdrop-blur">
        <CardHeader className="pb-4">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-lg bg-sky-500/10">
              <BarChart3 className="w-5 h-5 text-sky-400" />
            </div>
            <CardTitle className="text-lg font-semibold">ODS 技术指标同步</CardTitle>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-6">
            <div className="space-y-2">
              <Label htmlFor="token" className="text-sm text-muted-foreground">
                Token
              </Label>
              <Input
                id="token"
                placeholder="请输入访问令牌"
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20"
                value={form.token}
                onChange={(event) => handleChange('token')(event.target.value)}
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="config" className="text-sm text-muted-foreground">
                Config
              </Label>
              <Input
                id="config"
                value={form.config}
                onChange={(event) => handleChange('config')(event.target.value)}
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20 font-mono text-sm"
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="start-date" className="text-sm text-muted-foreground">
                Start Date
              </Label>
              <Input
                id="start-date"
                value={form.start_date}
                onChange={(event) => handleChange('start_date')(event.target.value)}
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20 font-mono"
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="end-date" className="text-sm text-muted-foreground">
                End Date
              </Label>
              <Input
                id="end-date"
                value={form.end_date}
                onChange={(event) => handleChange('end_date')(event.target.value)}
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20 font-mono"
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="rate-limit" className="text-sm text-muted-foreground">
                Rate Limit
              </Label>
              <Input
                id="rate-limit"
                type="number"
                value={form.rate_limit}
                onChange={(event) => handleChange('rate_limit')(event.target.value)}
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20"
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="cyq-rate-limit" className="text-sm text-muted-foreground">
                Cyq Rate Limit
              </Label>
              <Input
                id="cyq-rate-limit"
                type="number"
                value={form.cyq_rate_limit}
                onChange={(event) => handleChange('cyq_rate_limit')(event.target.value)}
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20"
              />
            </div>
            
            <div className="space-y-2 md:col-span-2 lg:col-span-3">
              <Label htmlFor="apis" className="text-sm text-muted-foreground">
                APIs
              </Label>
              <Input
                id="apis"
                value={form.apis}
                onChange={(event) => handleChange('apis')(event.target.value)}
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20 font-mono"
              />
            </div>
          </div>
          
          <div className="flex items-center justify-between">
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <div className="flex items-center gap-2 text-sm text-muted-foreground cursor-help">
                    <Info className="w-4 h-4" />
                    <span>默认仅同步 stk_factor，可按需调整 APIs</span>
                  </div>
                </TooltipTrigger>
                <TooltipContent>
                  <p>多个 API 请用逗号分隔</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
            
            <GradientButton variant="primary" onClick={handleSubmit} disabled={submitting}>
              {submitting ? '提交中...' : '同步技术指标'}
            </GradientButton>
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
        </CardContent>
      </Card>
    </motion.section>
  );
}
