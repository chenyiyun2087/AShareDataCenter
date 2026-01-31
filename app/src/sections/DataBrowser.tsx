import { useCallback, useEffect, useState } from 'react';
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { getJson } from '@/lib/api';

export function DataBrowser() {
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState('ods_daily');
  const [tsCode, setTsCode] = useState('');
  const [tradeDate, setTradeDate] = useState('');
  const [data, setData] = useState<{
    columns: string[];
    rows: Array<Array<string | number | null>>;
  } | null>(null);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const fetchTables = useCallback(async () => {
    try {
      const response = await getJson<{ data: string[] }>('/api/ods/tables');
      setTables(response.data);
      if (response.data.length > 0 && !response.data.includes(selectedTable)) {
        setSelectedTable(response.data[0]);
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : '加载表列表失败';
      setError(message);
    }
  }, [selectedTable]);

  const fetchRows = useCallback(
    async (currentPage: number) => {
      setLoading(true);
      setError(null);
      try {
        const response = await getJson<{
          data: {
            columns: string[];
            rows: Array<Array<string | number | null>>;
            total: number;
            page: number;
          };
        }>('/api/ods/rows', {
          table: selectedTable,
          page: currentPage,
          search_ts_code: tsCode || undefined,
          search_trade_date: tradeDate || undefined,
        });
        setData({ columns: response.data.columns, rows: response.data.rows });
        setTotal(response.data.total);
        setPage(response.data.page);
      } catch (err) {
        const message = err instanceof Error ? err.message : '读取数据失败';
        setError(message);
      } finally {
        setLoading(false);
      }
    },
    [selectedTable, tsCode, tradeDate],
  );

  useEffect(() => {
    fetchTables();
  }, [fetchTables]);

  useEffect(() => {
    fetchRows(1);
  }, [selectedTable, fetchRows]);

  const totalPages = Math.max(1, Math.ceil(total / 50));

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
              <Select
                value={selectedTable}
                onValueChange={(value) => {
                  setSelectedTable(value);
                  setPage(1);
                }}
              >
                <SelectTrigger className="w-[200px] bg-secondary/50 border-border/50">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent className="bg-card border-border/50">
                  {tables.map((table) => (
                    <SelectItem key={table} value={table}>
                      {table}
                    </SelectItem>
                  ))}
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
                value={tsCode}
                onChange={(event) => setTsCode(event.target.value)}
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
                value={tradeDate}
                onChange={(event) => setTradeDate(event.target.value)}
              />
            </div>
            
            <GradientButton
              variant="primary"
              size="sm"
              onClick={() => fetchRows(1)}
              disabled={loading}
            >
              <Search className="w-4 h-4 mr-2" />
              {loading ? '加载中...' : '搜索'}
            </GradientButton>
          </div>
          
          {error ? (
            <Alert variant="destructive" className="border-red-500/30 bg-red-500/10">
              <AlertCircle className="h-4 w-4 text-red-400" />
              <AlertTitle className="text-red-400">读取 ODS 数据失败</AlertTitle>
              <AlertDescription className="text-red-300/80">{error}</AlertDescription>
            </Alert>
          ) : null}

          {data ? (
            <div className="rounded-xl border border-border/50 overflow-hidden">
              <Table>
                <TableHeader>
                  <TableRow className="bg-secondary/50 hover:bg-secondary/50">
                    {data.columns.map((column) => (
                      <TableHead key={column} className="text-muted-foreground">
                        {column}
                      </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.rows.length > 0 ? (
                    data.rows.map((row, rowIndex) => (
                      <TableRow key={rowIndex} className="hover:bg-secondary/30">
                        {row.map((value, cellIndex) => (
                          <TableCell key={`${rowIndex}-${cellIndex}`} className="font-mono text-xs">
                            {value === null || value === undefined ? '-' : String(value)}
                          </TableCell>
                        ))}
                      </TableRow>
                    ))
                  ) : (
                    <TableRow>
                      <TableCell
                        colSpan={Math.max(data.columns.length, 1)}
                        className="text-center text-muted-foreground py-8"
                      >
                        暂无数据
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>
          ) : null}

          <div className="flex items-center justify-between text-sm text-muted-foreground">
            <span>共 {total} 条</span>
            <div className="flex items-center gap-3">
              <button
                className="px-2 py-1 rounded-md border border-border/50"
                onClick={() => fetchRows(Math.max(page - 1, 1))}
                disabled={page <= 1 || loading}
              >
                上一页
              </button>
              <span>
                第 {page} / {totalPages} 页
              </span>
              <button
                className="px-2 py-1 rounded-md border border-border/50"
                onClick={() => fetchRows(Math.min(page + 1, totalPages))}
                disabled={page >= totalPages || loading}
              >
                下一页
              </button>
            </div>
          </div>
        </CardContent>
      </Card>
    </motion.section>
  );
}
