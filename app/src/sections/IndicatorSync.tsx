import { motion } from 'framer-motion';
import { BarChart3, Info } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';

export function IndicatorSync() {
  const rateLimits = [
    ['120 积分', '约 20 次', '5000 条', '基础数据，流量限制'],
    ['2000 积分', '约 60 次', '5000 条', '常规数据，适度限制'],
    ['5000 积分', '500 次', '5000 条', '除分钟数据外无频次限制'],
    ['8000 积分', '不限', '5000 条', 'VIP 级别，全部数据无限制'],
  ];

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
            <CardTitle className="text-lg font-semibold">5000 积分请求频次说明</CardTitle>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="rounded-xl border border-border/50 overflow-hidden">
            <Table>
              <TableHeader>
                <TableRow className="bg-secondary/50 hover:bg-secondary/50">
                  <TableHead className="text-muted-foreground">积分等级</TableHead>
                  <TableHead className="text-muted-foreground">每分钟请求</TableHead>
                  <TableHead className="text-muted-foreground">单次数据量</TableHead>
                  <TableHead className="text-muted-foreground">说明</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rateLimits.map((row) => (
                  <TableRow key={row[0]} className="hover:bg-secondary/30">
                    {row.map((cell) => (
                      <TableCell key={`${row[0]}-${cell}`} className="text-sm">
                        {cell}
                      </TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
          <Alert className="border-sky-500/30 bg-sky-500/10">
            <Info className="h-4 w-4 text-sky-300" />
            <AlertTitle className="text-sky-200">重要提示</AlertTitle>
            <AlertDescription className="text-sky-100/80">
              5000 积分账号每分钟最多 500 次请求，单次可拉取 5000 条；历史日线 5000 条约
              23 年数据，分钟数据与部分特色数据可能仍有额外限制。
            </AlertDescription>
          </Alert>
        </CardContent>
      </Card>
    </motion.section>
  );
}
