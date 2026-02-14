import { useState, useEffect } from 'react';
import { Server, RefreshCw } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';

interface DataStatusItem {
  table: string;
  name: string;
  latest_date: string;
  date_count: number;
  row_count: number;
  ready: boolean;
}

interface DataStatusResponse {
  ods: DataStatusItem[];
  dwd: DataStatusItem[];
  dws: DataStatusItem[];
  ads: DataStatusItem[];
  latest_trade_date: string;
}

export function ODSStatus() {
  const [data, setData] = useState<DataStatusResponse | null>(null);
  const [loading, setLoading] = useState(false);

  const fetchData = async () => {
    setLoading(true);
    try {
      const res = await fetch('/api/data/status');
      const json = await res.json();
      if (json.data) {
        setData(json.data);
      }
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const renderTable = (items: DataStatusItem[] = []) => (
    <div className="rounded-xl border border-border/50 overflow-hidden mt-4">
      <Table>
        <TableHeader>
          <TableRow className="bg-secondary/50">
            <TableHead>数据表</TableHead>
            <TableHead>最新日期</TableHead>
            <TableHead>交易日天数</TableHead>
            <TableHead>总行数</TableHead>
            <TableHead>状态</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {items.map((row) => (
            <TableRow key={row.table} className="hover:bg-secondary/30">
              <TableCell className="font-medium">{row.name}</TableCell>
              <TableCell>{row.latest_date || '-'}</TableCell>
              <TableCell>{row.date_count}</TableCell>
              <TableCell>{row.row_count.toLocaleString()}</TableCell>
              <TableCell>
                <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${row.ready ? 'bg-green-500/10 text-green-500' : 'bg-yellow-500/10 text-yellow-500'
                  }`}>
                  {row.ready ? '就绪' : '未就绪'}
                </span>
              </TableCell>
            </TableRow>
          ))}
          {items.length === 0 && !loading && (
            <TableRow>
              <TableCell colSpan={5} className="text-center py-4 text-muted-foreground">
                暂无数据
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </div>
  );

  return (
    <Card className="border-border/50 bg-card/50 backdrop-blur">
      <CardHeader className="pb-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-lg bg-sky-500/10">
              <Server className="w-5 h-5 text-sky-400" />
            </div>
            <CardTitle className="text-lg font-semibold">数据状态概览</CardTitle>
          </div>
          <button
            onClick={fetchData}
            disabled={loading}
            className="p-2 rounded-full hover:bg-secondary/80 transition-colors disabled:opacity-50"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </CardHeader>
      <CardContent>
        <Tabs defaultValue="ods" className="w-full">
          <TabsList className="grid w-full grid-cols-4">
            <TabsTrigger value="ods">ODS 层</TabsTrigger>
            <TabsTrigger value="dwd">DWD 层</TabsTrigger>
            <TabsTrigger value="dws">DWS 层</TabsTrigger>
            <TabsTrigger value="ads">ADS 层</TabsTrigger>
          </TabsList>
          <TabsContent value="ods">{renderTable(data?.ods)}</TabsContent>
          <TabsContent value="dwd">{renderTable(data?.dwd)}</TabsContent>
          <TabsContent value="dws">{renderTable(data?.dws)}</TabsContent>
          <TabsContent value="ads">{renderTable(data?.ads)}</TabsContent>
        </Tabs>
      </CardContent>
    </Card>
  );
}
