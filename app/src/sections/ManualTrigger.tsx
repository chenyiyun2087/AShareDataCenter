import { motion } from 'framer-motion';
import { Play } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';

export function ManualTrigger() {
  const dimensions = [
    ['基本面', '40%', 'ROE、净利润增长率、资产负债率'],
    ['成长性', '20%', '营收/利润 CAGR、毛利率趋势'],
    ['估值水平', '15%', 'PE/PB/PS 历史分位'],
    ['市场表现', '10%', '涨跌幅、量价配合、换手率'],
    ['资金面', '10%', '主力资金、北向资金、融资买入'],
    ['风险评估', '5%', '股权质押率、股东户数、ST 风险'],
  ];

  const fundamentals = [
    ['ROE', '15', '≥15%'],
    ['净利润同比增长率', '10', '≥30%'],
    ['营业收入增长率', '5', '≥20%'],
    ['资产负债率', '5', '30-60%'],
    ['经营现金流/净利润', '5', '≥1.2'],
  ];

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
            <CardTitle className="text-lg font-semibold">股票评分体系（总分 100）</CardTitle>
          </div>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="rounded-xl border border-border/50 overflow-hidden">
            <Table>
              <TableHeader>
                <TableRow className="bg-secondary/50 hover:bg-secondary/50">
                  <TableHead className="text-muted-foreground">评分维度</TableHead>
                  <TableHead className="text-muted-foreground">权重</TableHead>
                  <TableHead className="text-muted-foreground">关键指标</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {dimensions.map((row) => (
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

          <div>
            <h4 className="text-sm font-medium text-muted-foreground mb-3">基本面评分细则（40 分）</h4>
            <div className="rounded-xl border border-border/50 overflow-hidden">
              <Table>
                <TableHeader>
                  <TableRow className="bg-secondary/50 hover:bg-secondary/50">
                    <TableHead className="text-muted-foreground">指标</TableHead>
                    <TableHead className="text-muted-foreground">权重</TableHead>
                    <TableHead className="text-muted-foreground">优秀标准</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {fundamentals.map((row) => (
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
            <p className="mt-3 text-xs text-muted-foreground">
              示例：ROE 得分 = min(15, ROE × 15 / 15)；成长性得分 = min(10, 净利润增长率 ×
              10 / 30)。
            </p>
          </div>
        </CardContent>
      </Card>
    </motion.section>
  );
}
