import { motion } from 'framer-motion';
import { Database } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';

export function DataBrowser() {
  const apiQuickRef = [
    ['获取股票列表', 'stock_basic', 'list_status=L', 'ts_code,name,industry'],
    ['日线行情', 'daily', 'ts_code,start_date', 'open,high,low,close,vol'],
    ['财务指标', 'fina_indicator', 'ts_code,period', 'roe,roa,debt_to_assets'],
    ['利润表', 'income', 'ts_code,period', 'revenue,n_income'],
    ['资金流向', 'moneyflow', 'ts_code,trade_date', 'buy_lg_amount,sell_lg_amount'],
    ['北向资金', 'hk_hold', 'ts_code,trade_date', 'hold_amount,hold_ratio'],
  ];

  const sampleCode = `import tushare as ts
import pandas as pd

# 初始化
pro = ts.pro_api('your_token')

# 1. 获取股票基本信息
df_basic = pro.stock_basic(exchange='', list_status='L',
                           fields='ts_code,symbol,name,area,industry')

# 2. 获取财务指标 (近4个季度)
df_indicator = pro.fina_indicator(ts_code='000001.SZ',
                                  fields='ts_code,end_date,roe,roa,gross_margin')

# 3. 获取日线行情 (近1年)
df_daily = pro.daily(ts_code='000001.SZ',
                     start_date='20250101', end_date='20260101')

# 4. 获取每日指标
df_basic_daily = pro.daily_basic(ts_code='000001.SZ',
                                 fields='ts_code,trade_date,pe,pb,ps,total_mv')

# 5. 获取资金流向
df_money = pro.moneyflow(ts_code='000001.SZ',
                         start_date='20260101', end_date='20260201')

# 6. 计算基本面评分示例
def calc_fundamental_score(indicator_df):
    score = 0
    latest = indicator_df.iloc[0]

    # ROE评分 (15分)
    roe = latest['roe']
    score += min(15, roe * 15 / 15)

    # 净利润增长率 (10分) - 需计算同比
    # ...

    return score

# 7. 综合评分
def calculate_stock_score(ts_code):
    scores = {
        'fundamental': calc_fundamental_score(...),  # 40分
        'growth': calc_growth_score(...),            # 20分
        'valuation': calc_valuation_score(...),      # 15分
        'market': calc_market_score(...),            # 10分
        'capital': calc_capital_score(...),          # 10分
        'risk': calc_risk_penalty(...)               # -5~0分
    }
    total = sum(scores.values())
    return total, scores`;

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
            <CardTitle className="text-lg font-semibold">Python 实现示例与接口速查</CardTitle>
          </div>
        </CardHeader>
        <CardContent className="space-y-6">
          <div>
            <h4 className="text-sm font-medium text-muted-foreground mb-3">实现示例</h4>
            <pre className="rounded-xl border border-border/50 bg-secondary/30 p-4 text-xs text-foreground overflow-auto">
              <code>{sampleCode}</code>
            </pre>
          </div>

          <div>
            <h4 className="text-sm font-medium text-muted-foreground mb-3">常用接口快速查询表</h4>
            <div className="rounded-xl border border-border/50 overflow-hidden">
              <Table>
                <TableHeader>
                  <TableRow className="bg-secondary/50 hover:bg-secondary/50">
                    <TableHead className="text-muted-foreground">数据需求</TableHead>
                    <TableHead className="text-muted-foreground">接口名称</TableHead>
                    <TableHead className="text-muted-foreground">关键参数</TableHead>
                    <TableHead className="text-muted-foreground">返回字段示例</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {apiQuickRef.map((row) => (
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
          </div>
        </CardContent>
      </Card>
    </motion.section>
  );
}
