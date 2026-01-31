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

export function ManualTrigger() {
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
              <Label htmlFor="manual-token" className="text-sm text-muted-foreground">
                Token
              </Label>
              <Input
                id="manual-token"
                placeholder="请输入访问令牌"
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20"
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="manual-start" className="text-sm text-muted-foreground">
                Start Date
              </Label>
              <Input
                id="manual-start"
                defaultValue="20100101"
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
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="manual-rate" className="text-sm text-muted-foreground">
                Rate Limit
              </Label>
              <Input
                id="manual-rate"
                type="number"
                defaultValue="500"
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20"
              />
            </div>
          </div>
          
          <div className="flex justify-end">
            <GradientButton variant="success">
              <Play className="w-4 h-4 mr-2" />
              运行任务
            </GradientButton>
          </div>
        </CardContent>
      </Card>
    </motion.section>
  );
}
