import { motion } from 'framer-motion';
import { Server } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';

export function ODSStatus() {
  const permissionGroups = [
    {
      title: '基础数据（120 积分起）',
      rows: [
        ['股票列表', 'stock_basic', '120', '每日更新', 'A 股代码、名称、行业等基础信息'],
        ['交易日历', 'trade_cal', '120', '定期更新', '交易所交易日历'],
        ['上市公司信息', 'stock_company', '120', '定期更新', '公司基本资料、主营业务等'],
        ['IPO 新股列表', 'new_share', '120', '每日 19 点', '新股上市时间、发行价等'],
        ['股票曾用名', 'namechange', '120', '定期更新', '历史曾用名信息'],
        ['ST 股票列表', 'stk_stateowned', '120', '不定期', 'ST、*ST 等特殊处理股票'],
      ],
    },
    {
      title: '行情数据（2000 积分起）',
      rows: [
        ['日线行情', 'daily', '2000', '交易日 15-17 点', '开高低收、成交量等'],
        ['周线行情', 'weekly', '2000', '每周五 15-17 点', '周 K 线数据'],
        ['月线行情', 'monthly', '2000', '每月更新', '月 K 线数据'],
        ['复权行情', 'pro_bar', '2000', '每月更新', '前/后复权行情'],
        ['每日指标', 'daily_basic', '2000', '交易日 15-17 点', 'PE、PB、PS 等'],
        ['复权因子', 'adj_factor', '2000', '交易日更新', '复权因子'],
        ['涨跌停价格', 'stk_limit', '2000', '交易日 9 点', '每日涨跌停价格'],
        ['停复牌信息', 'suspend_d', '2000', '交易日更新', '停复牌时间及原因'],
      ],
    },
    {
      title: '财务数据（2000 积分起）',
      rows: [
        ['利润表', 'income', '2000', '实时更新', '营业收入、净利润等'],
        ['资产负债表', 'balancesheet', '2000', '实时更新', '总资产、负债等'],
        ['现金流量表', 'cashflow', '2000', '实时更新', '经营/投资/筹资现金流'],
        ['财务指标', 'fina_indicator', '2000', '随财报更新', 'ROE、ROA 等'],
        ['业绩预告', 'forecast', '2000', '实时更新', '业绩预告数据'],
        ['业绩快报', 'express', '2000', '实时更新', '快报数据'],
        ['分红送股', 'dividend', '2000', '实时更新', '分红派息、送转股'],
        ['主营构成', 'fina_mainbz', '2000', '随财报更新', '主营业务构成明细'],
      ],
    },
    {
      title: '参考数据（2000 积分起）',
      rows: [
        ['前十大股东', 'top10_holders', '2000', '定期更新', '前十大股东持股情况'],
        ['前十大流通股东', 'top10_floatholders', '2000', '定期更新', '流通股东持股'],
        ['股东人数', 'stk_holdernumber', '2000', '不定期', '股东户数变化'],
        ['股权质押统计', 'pledge_stat', '2000', '每日晚 9 点', '股权质押统计'],
        ['股权质押明细', 'pledge_detail', '2000', '每日晚 9 点', '质押明细记录'],
        ['股东增减持', 'stk_holdertrade', '2000', '交易日 19 点', '重要股东变动'],
        ['大宗交易', 'block_trade', '2000', '每日晚 9 点', '大宗交易明细'],
        ['限售股解禁', 'share_float', '3000', '定期更新', '限售股解禁时间表'],
      ],
    },
    {
      title: '特色数据（2000-5000 积分）',
      rows: [
        ['沪深股通持股', 'hk_hold', '2000', '下个交易日 8 点', '北向资金持股明细'],
        ['龙虎榜统计', 'top_list', '2000', '每日晚 8 点', '龙虎榜每日明细'],
        ['龙虎榜机构', 'top_inst', '2000', '每日晚 8 点', '机构买卖席位数据'],
        ['券商盈利预测', 'forecast_vip', '5000', '定期更新', '券商一致预期'],
        ['每日筹码', 'cyq_perf', '5000', '交易日更新', '筹码分布及胜率'],
        ['技术面因子', 'stk_factor', '5000', '交易日更新', 'MACD、KDJ 等指标'],
      ],
    },
    {
      title: '两融及资金流向（2000-5000 积分）',
      rows: [
        ['融资融券汇总', 'margin', '2000', '每日 9 点', '融资融券交易汇总'],
        ['融资融券明细', 'margin_detail', '2000', '每日 9 点', '个股融资融券明细'],
        ['个股资金流向', 'moneyflow', '2000', '交易日 19 点', '主力/大单资金流向'],
        ['个股资金流向(THS)', 'moneyflow_hsgt', '5000', '交易日更新', '同花顺资金流向'],
        ['沪深港通资金', 'moneyflow_hsgt', '2000', '交易日更新', '北向南向资金流向'],
      ],
    },
  ];

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
              <CardTitle className="text-lg font-semibold">Tushare 5000 积分数据权限概览</CardTitle>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-6">
          {permissionGroups.map((group) => (
            <div key={group.title} className="space-y-3">
              <h3 className="text-sm font-semibold text-sky-300">{group.title}</h3>
              <div className="rounded-xl border border-border/50 overflow-hidden">
                <Table>
                  <TableHeader>
                    <TableRow className="bg-secondary/50 hover:bg-secondary/50">
                      <TableHead className="text-muted-foreground">数据类型</TableHead>
                      <TableHead className="text-muted-foreground">接口名称</TableHead>
                      <TableHead className="text-muted-foreground">积分要求</TableHead>
                      <TableHead className="text-muted-foreground">更新频率</TableHead>
                      <TableHead className="text-muted-foreground">数据说明</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {group.rows.map((row) => (
                      <TableRow key={`${group.title}-${row[1]}`} className="hover:bg-secondary/30">
                        {row.map((cell) => (
                          <TableCell key={`${group.title}-${row[1]}-${cell}`} className="text-sm">
                            {cell}
                          </TableCell>
                        ))}
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            </div>
          ))}
        </CardContent>
      </Card>
    </motion.section>
  );
}
