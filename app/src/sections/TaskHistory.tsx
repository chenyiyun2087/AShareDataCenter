import { motion } from 'framer-motion';
import { History } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';

export function TaskHistory() {
  const risks = [
    ['高股权质押', '-3 分', '质押比例 > 50%', 'pledge_stat.pledge_ratio'],
    ['股东户数大增', '-2 分', '单季增长 > 30%', 'stk_holdernumber.holder_num'],
    ['ST 或退市风险', '-5 分', '被 ST 或即将退市', 'stock_basic.list_status'],
  ];

  return (
    <motion.section
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay: 0.7 }}
      className="px-6 py-4"
    >
      <Card className="border-border/50 bg-card/50 backdrop-blur">
        <CardHeader className="pb-4">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-lg bg-purple-500/10">
              <History className="w-5 h-5 text-purple-400" />
            </div>
            <CardTitle className="text-lg font-semibold">风险评估（扣分项）</CardTitle>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="rounded-xl border border-border/50 overflow-hidden">
            <Table>
              <TableHeader>
                <TableRow className="bg-secondary/50 hover:bg-secondary/50">
                  <TableHead className="text-muted-foreground">风险项</TableHead>
                  <TableHead className="text-muted-foreground">扣分</TableHead>
                  <TableHead className="text-muted-foreground">触发条件</TableHead>
                  <TableHead className="text-muted-foreground">数据接口</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {risks.map((row) => (
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
          <p className="text-xs text-muted-foreground">
            风险评估最多扣 5 分，用于识别质押风险、股东结构异动与退市风险。
          </p>
        </CardContent>
      </Card>
    </motion.section>
  );
}
