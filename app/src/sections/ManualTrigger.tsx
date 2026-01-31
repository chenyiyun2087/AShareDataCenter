import { useState } from 'react';
import { motion } from 'framer-motion';
import { Play } from 'lucide-react';
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
import { GradientButton } from '@/components/GradientButton';
import { postJson } from '@/lib/api';

interface ManualForm {
  layer: string;
  mode: string;
  token: string;
  start_date: string;
  fina_start: string;
  fina_end: string;
  rate_limit: string;
}

export function ManualTrigger() {
  const [form, setForm] = useState<ManualForm>({
    layer: 'base',
    mode: 'incremental',
    token: '',
    start_date: '20100101',
    fina_start: '',
    fina_end: '',
    rate_limit: '500',
  });
  const [status, setStatus] = useState<{ type: 'success' | 'error'; message: string } | null>(
    null,
  );
  const [submitting, setSubmitting] = useState(false);

  const handleChange = (key: keyof ManualForm) => (value: string) => {
    setForm((prev) => ({ ...prev, [key]: value }));
  };

  const handleSubmit = async () => {
    setSubmitting(true);
    setStatus(null);
    try {
      await postJson('/api/tasks/run', form);
      setStatus({ type: 'success', message: '任务已触发。' });
    } catch (err) {
      const message = err instanceof Error ? err.message : '触发失败';
      setStatus({ type: 'error', message });
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <motion.section
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.5 }}
      className="px-6 py-4"
    >
      <Card className="border-border/50 bg-card/50 backdrop-blur">
        <CardHeader className="pb-4">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-lg bg-emerald-500/10">
              <Play className="w-5 h-5 text-emerald-400" />
            </div>
            <CardTitle className="text-lg font-semibold">手动触发</CardTitle>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
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
              <Label htmlFor="manual-token" className="text-sm text-muted-foreground">
                Token
              </Label>
              <Input
                id="manual-token"
                placeholder="请输入访问令牌"
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20"
                value={form.token}
                onChange={(event) => handleChange('token')(event.target.value)}
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="manual-start" className="text-sm text-muted-foreground">
                Start Date
              </Label>
              <Input
                id="manual-start"
                value={form.start_date}
                onChange={(event) => handleChange('start_date')(event.target.value)}
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20 font-mono"
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="fina-start" className="text-sm text-muted-foreground">
                Fina Start
              </Label>
              <Input
                id="fina-start"
                placeholder="YYYYMMDD"
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20 font-mono"
                value={form.fina_start}
                onChange={(event) => handleChange('fina_start')(event.target.value)}
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="fina-end" className="text-sm text-muted-foreground">
                Fina End
              </Label>
              <Input
                id="fina-end"
                placeholder="YYYYMMDD"
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20 font-mono"
                value={form.fina_end}
                onChange={(event) => handleChange('fina_end')(event.target.value)}
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="manual-rate" className="text-sm text-muted-foreground">
                Rate Limit
              </Label>
              <Input
                id="manual-rate"
                type="number"
                value={form.rate_limit}
                onChange={(event) => handleChange('rate_limit')(event.target.value)}
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20"
              />
            </div>
          </div>
          
          <div className="flex justify-end">
            <GradientButton variant="success" onClick={handleSubmit} disabled={submitting}>
              <Play className="w-4 h-4 mr-2" />
              {submitting ? '运行中...' : '运行任务'}
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
