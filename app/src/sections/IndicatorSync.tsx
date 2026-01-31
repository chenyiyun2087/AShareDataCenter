import { motion } from 'framer-motion';
import { BarChart3, Info } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { GradientButton } from '@/components/GradientButton';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';

export function IndicatorSync() {
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
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="config" className="text-sm text-muted-foreground">
                Config
              </Label>
              <Input
                id="config"
                defaultValue="config/etl.ini"
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20 font-mono text-sm"
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="start-date" className="text-sm text-muted-foreground">
                Start Date
              </Label>
              <Input
                id="start-date"
                defaultValue="20260131"
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20 font-mono"
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="end-date" className="text-sm text-muted-foreground">
                End Date
              </Label>
              <Input
                id="end-date"
                defaultValue="20260131"
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
                defaultValue="500"
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
                defaultValue="180"
                className="bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20"
              />
            </div>
            
            <div className="space-y-2 md:col-span-2 lg:col-span-3">
              <Label htmlFor="apis" className="text-sm text-muted-foreground">
                APIs
              </Label>
              <Input
                id="apis"
                defaultValue="stk_factor"
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
            
            <GradientButton variant="primary">
              同步技术指标
            </GradientButton>
          </div>
        </CardContent>
      </Card>
    </motion.section>
  );
}
