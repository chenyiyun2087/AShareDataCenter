import { StatusCard } from '@/components/StatusCard';
import { CheckCircle2, Clock, TrendingUp, Zap } from 'lucide-react';

export function Dashboard() {
  return (
    <section className="px-6 py-6">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatusCard
          title="今日任务"
          value="24"
          subtitle="已完成 / 总计 32"
          icon={CheckCircle2}
          status="success"
          delay={0.1}
        />
        <StatusCard
          title="成功率"
          value="96.5%"
          subtitle="过去 7 天平均"
          icon={TrendingUp}
          status="info"
          delay={0.2}
        />
        <StatusCard
          title="正在运行"
          value="3"
          subtitle="活跃任务数"
          icon={Zap}
          status="warning"
          delay={0.3}
        />
        <StatusCard
          title="最后同步"
          value="2分钟前"
          subtitle="2026-01-31 14:32"
          icon={Clock}
          status="info"
          delay={0.4}
        />
      </div>
    </section>
  );
}
