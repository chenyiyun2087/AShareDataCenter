import { motion } from 'framer-motion';
import { Search, Database, AlertCircle } from 'lucide-react';
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
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { GradientButton } from '@/components/GradientButton';

export function DataBrowser() {
  return (
    <motion.section
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.8 }}
      className="px-6 py-4"
    >
      <Card className="border-border/50 bg-card/50 backdrop-blur">
        <CardHeader className="pb-4">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-lg bg-indigo-500/10">
              <Database className="w-5 h-5 text-indigo-400" />
            </div>
            <CardTitle className="text-lg font-semibold">ODS 数据浏览</CardTitle>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-wrap items-end gap-4">
            <div className="space-y-2">
              <Label className="text-sm text-muted-foreground">表</Label>
              <Select defaultValue="ods_daily">
                <SelectTrigger className="w-[200px] bg-secondary/50 border-border/50">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent className="bg-card border-border/50">
                  <SelectItem value="ods_daily">ods_daily</SelectItem>
                  <SelectItem value="ods_weekly">ods_weekly</SelectItem>
                  <SelectItem value="ods_monthly">ods_monthly</SelectItem>
                  <SelectItem value="ods_factor">ods_factor</SelectItem>
                  <SelectItem value="ods_limit">ods_limit</SelectItem>
                </SelectContent>
              </Select>
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="ts-code" className="text-sm text-muted-foreground">
                ts_code
              </Label>
              <Input
                id="ts-code"
                placeholder="股票代码"
                className="w-[180px] bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20 font-mono"
              />
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="trade-date" className="text-sm text-muted-foreground">
                trade_date
              </Label>
              <Input
                id="trade-date"
                placeholder="YYYYMMDD"
                className="w-[150px] bg-secondary/50 border-border/50 focus:border-sky-500/50 focus:ring-sky-500/20 font-mono"
              />
            </div>
            
            <GradientButton variant="primary" size="sm">
              <Search className="w-4 h-4 mr-2" />
              搜索
            </GradientButton>
          </div>
          
          <Alert variant="destructive" className="border-red-500/30 bg-red-500/10">
            <AlertCircle className="h-4 w-4 text-red-400" />
            <AlertTitle className="text-red-400">读取 ODS 数据失败</AlertTitle>
            <AlertDescription className="text-red-300/80">
              Failed to connect to MySQL. Check MYSQL_HOST/MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD/MYSQL_DB and verify credentials.
            </AlertDescription>
          </Alert>
        </CardContent>
      </Card>
    </motion.section>
  );
}
