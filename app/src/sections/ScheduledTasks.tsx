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

const scheduledTasks = [
  { id: 1, nextRun: '2026-01-31 15:00', trigger: '每日同步' },
  { id: 2, nextRun: '2026-01-31 18:00', trigger: '每日备份' },
];

export function ScheduledTasks() {
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
                <Select defaultValue="base">
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
                <Select defaultValue="incremental">
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
                  defaultValue="2"
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
                  defaultValue="0"
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
                />
              </div>
              
              <div className="space-y-2">
                <Label htmlFor="cron-rate" className="text-sm text-muted-foreground">
                  Rate Limit
                </Label>
                <Input
                  id="cron-rate"
                  type="number"
                  defaultValue="500"
                  className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20"
                />
              </div>
            </div>
            
            <div className="flex justify-end">
              <GradientButton variant="primary" size="sm">
                <Plus className="w-4 h-4 mr-2" />
                添加定时任务
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
                  {scheduledTasks.length > 0 ? (
                    scheduledTasks.map((task) => (
                      <TableRow key={task.id} className="hover:bg-secondary/30">
                        <TableCell className="font-mono">{task.id}</TableCell>
                        <TableCell className="font-mono">{task.nextRun}</TableCell>
                        <TableCell>{task.trigger}</TableCell>
                        <TableCell className="text-right">
                          <Button
                            variant="ghost"
                            size="sm"
                            className="text-red-400 hover:text-red-300 hover:bg-red-500/10"
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
          </div>
        </CardContent>
      </Card>
    </motion.section>
  );
}
