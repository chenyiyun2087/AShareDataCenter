import { motion } from 'framer-motion';
import { Clock } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';

export function ScheduledTasks() {
  const acquisition = [
    '每日更新：日线行情、每日指标、资金流向（交易日收盘后）',
    '每周更新：财务指标、北向资金持股（每周末）',
    '每月更新：财务三表、财务指标详细数据（财报季）',
    '按需查询：股权质押、股东变动、龙虎榜数据',
  ];

  const storage = [
    '使用 MySQL/PostgreSQL 存储结构化数据',
    '建立股票代码 + 日期双索引提升查询效率',
    '财务数据按季度归档，避免重复查询',
    '行情数据保留最近 3 年明细，历史数据按周/月聚合',
    '每日增量更新，避免全量刷新浪费配额',
  ];

  const workflow = [
    '数据采集：从 Tushare 获取最新数据并入库（约 15 分钟）',
    '数据清洗：处理缺失值、异常值（约 5 分钟）',
    '指标计算：计算各维度评分指标（约 10 分钟）',
    '综合评分：加权汇总得出总分（约 2 分钟）',
    '排名输出：生成股票评分排行榜（约 3 分钟）',
  ];

  const notices = [
    '合理分配请求频次，优先获取核心数据。',
    '财务数据存在 1-2 个月滞后，可结合业绩预告。',
    '行业差异显著，建议分行业评分。',
    '牛熊市可动态调整评分权重。',
    '评分体系需回测验证有效性。',
  ];

  const renderList = (items: string[]) => (
    <ul className="space-y-2 text-sm text-muted-foreground">
      {items.map((item) => (
        <li key={item} className="flex gap-2">
          <span className="mt-1 h-2 w-2 rounded-full bg-sky-400" />
          <span>{item}</span>
        </li>
      ))}
    </ul>
  );

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
            <CardTitle className="text-lg font-semibold">评分系统实施建议</CardTitle>
          </div>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
            <div className="rounded-xl border border-border/50 p-4 bg-secondary/30">
              <h4 className="text-sm font-semibold text-foreground mb-3">数据获取策略</h4>
              {renderList(acquisition)}
            </div>
            <div className="rounded-xl border border-border/50 p-4 bg-secondary/30">
              <h4 className="text-sm font-semibold text-foreground mb-3">数据存储建议</h4>
              {renderList(storage)}
            </div>
            <div className="rounded-xl border border-border/50 p-4 bg-secondary/30">
              <h4 className="text-sm font-semibold text-foreground mb-3">评分计算流程</h4>
              {renderList(workflow)}
            </div>
          </div>

          <div className="rounded-xl border border-border/50 p-4">
            <h4 className="text-sm font-semibold text-foreground mb-3">注意事项</h4>
            {renderList(notices)}
          </div>
        </CardContent>
      </Card>
    </motion.section>
  );
}
