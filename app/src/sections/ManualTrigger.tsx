import { useState } from 'react';
import { Play, Loader2 } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';

export function ManualTrigger() {
  const [loading, setLoading] = useState<string | null>(null);
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');

  const triggerTask = async (layer: string, mode: string, isDefault = false) => {
    const key = `${layer}-${mode}-${isDefault ? 'default' : 'custom'}`;
    setLoading(key);

    const payload: any = { layer, mode };
    if (isDefault) {
      payload.default_trigger = true;
    } else {
      if (startDate) payload.start_date = startDate;
      if (endDate) payload.end_date = endDate;
    }

    try {
      const res = await fetch('/api/tasks/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const json = await res.json();
      alert(json.message || '任务已触发');
    } catch (err) {
      alert('触发失败: ' + err);
    } finally {
      setLoading(null);
    }
  };

  const layers = [
    { id: 'ods', label: 'ODS 数据采集' },
    { id: 'dwd', label: 'DWD 数据清洗' },
    { id: 'dws', label: 'DWS 指标计算' },
    { id: 'ads', label: 'ADS 应用数据' },
  ];

  return (
    <Card className="border-border/50 bg-card/50 backdrop-blur">
      <CardHeader className="pb-4">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-emerald-500/10">
            <Play className="w-5 h-5 text-emerald-400" />
          </div>
          <CardTitle className="text-lg font-semibold">手动触发任务</CardTitle>
        </div>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="flex flex-col gap-4 p-4 border rounded-lg bg-secondary/20">
          <div className="flex items-center gap-4">
            <div className="grid gap-1.5 flex-1">
              <Label htmlFor="start-date">开始日期 (可选)</Label>
              <Input
                id="start-date"
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
              />
            </div>
            <div className="grid gap-1.5 flex-1">
              <Label htmlFor="end-date">结束日期 (可选)</Label>
              <Input
                id="end-date"
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
              />
            </div>
          </div>
          <p className="text-xs text-muted-foreground">
            * 不选择日期则执行增量更新 (默认逻辑)，与"默认触发"按钮效果类似。
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {layers.map((layer) => (
            <div key={layer.id} className="flex items-center justify-between p-3 border rounded-lg hover:bg-secondary/30 transition-colors">
              <div className="font-medium text-sm">{layer.label}</div>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => triggerTask(layer.id, 'incremental', true)}
                  disabled={loading !== null}
                >
                  {loading === `${layer.id}-incremental-default` && <Loader2 className="mr-2 h-3 w-3 animate-spin" />}
                  默认触发
                </Button>
                <Button
                  size="sm"
                  onClick={() => triggerTask(layer.id, 'incremental', false)}
                  disabled={loading !== null}
                >
                  {loading === `${layer.id}-incremental-custom` && <Loader2 className="mr-2 h-3 w-3 animate-spin" />}
                  按日期跑批
                </Button>
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
