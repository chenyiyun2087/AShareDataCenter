import { StatusCard } from '@/components/StatusCard';
import { BadgeCheck, Calendar, Gauge, Layers } from 'lucide-react';

export function Dashboard() {
  return (
    <section className="px-6 py-6">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatusCard
          title="账户等级"
          value="Tushare 会员"
          subtitle="5000 积分权限"
          icon={BadgeCheck}
          status="success"
          delay={0.1}
        />
        <StatusCard
          title="请求频次"
          value="500 次/分钟"
          subtitle="单次 5000 条"
          icon={Gauge}
          status="info"
          delay={0.2}
        />
        <StatusCard
          title="评分体系"
          value="100 分模型"
          subtitle="6 大维度权重"
          icon={Layers}
          status="warning"
          delay={0.3}
        />
        <StatusCard
          title="编制日期"
          value="2026-02-06"
          subtitle="方案版本 V1.0"
          icon={Calendar}
          status="info"
          delay={0.4}
        />
      </div>
    </section>
  );
}
