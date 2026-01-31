import { motion } from 'framer-motion';
import { AlertCircle, RefreshCw, Server } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { StatusBadge } from '@/components/StatusBadge';
import { GradientButton } from '@/components/GradientButton';
import { Progress } from '@/components/ui/progress';

export function ODSStatus() {
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
            <StatusBadge status="error">连接失败</StatusBadge>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <Alert variant="destructive" className="border-red-500/30 bg-red-500/10">
            <AlertCircle className="h-4 w-4 text-red-400" />
            <AlertTitle className="text-red-400">连接错误</AlertTitle>
            <AlertDescription className="text-red-300/80">
              读取 ODS 状态失败：Failed to connect to MySQL. Check MYSQL_HOST/MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD/MYSQL_DB and verify credentials.
            </AlertDescription>
          </Alert>
          
          <div className="space-y-2">
            <div className="flex justify-between text-sm">
              <span className="text-muted-foreground">连接重试</span>
              <span className="text-sky-400">3 / 5</span>
            </div>
            <Progress value={60} className="h-2 bg-secondary" />
          </div>
          
          <div className="flex justify-end">
            <GradientButton variant="primary" size="sm">
              <RefreshCw className="w-4 h-4 mr-2" />
              重试连接
            </GradientButton>
          </div>
        </CardContent>
      </Card>
    </motion.section>
  );
}
