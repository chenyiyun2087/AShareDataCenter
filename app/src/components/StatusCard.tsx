import { motion } from 'framer-motion';
import type { LucideIcon } from 'lucide-react';

interface StatusCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  icon: LucideIcon;
  status?: 'success' | 'warning' | 'error' | 'info';
  delay?: number;
}

const statusColors = {
  success: 'text-emerald-400 bg-emerald-400/10',
  warning: 'text-amber-400 bg-amber-400/10',
  error: 'text-red-400 bg-red-400/10',
  info: 'text-sky-400 bg-sky-400/10',
};

const glowColors = {
  success: 'group-hover:shadow-[0_0_20px_rgba(34,197,94,0.3)]',
  warning: 'group-hover:shadow-[0_0_20px_rgba(245,158,11,0.3)]',
  error: 'group-hover:shadow-[0_0_20px_rgba(239,68,68,0.3)]',
  info: 'group-hover:shadow-[0_0_20px_rgba(56,189,248,0.3)]',
};

export function StatusCard({ title, value, subtitle, icon: Icon, status = 'info', delay = 0 }: StatusCardProps) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, delay, ease: [0.4, 0, 0.2, 1] }}
      className="group"
    >
      <div
        className={`
          relative overflow-hidden rounded-2xl bg-card p-6
          border border-border/50
          transition-all duration-300
          hover:border-sky-500/50 hover:-translate-y-1
          ${glowColors[status]}
        `}
      >
        {/* Background Gradient */}
        <div className="absolute inset-0 bg-gradient-to-br from-white/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-300" />
        
        <div className="relative flex items-start justify-between">
          <div className="flex-1">
            <p className="text-sm font-medium text-muted-foreground mb-1">{title}</p>
            <p className={`text-2xl font-bold ${statusColors[status].split(' ')[0]}`}>
              {value}
            </p>
            {subtitle && (
              <p className="text-xs text-muted-foreground mt-1">{subtitle}</p>
            )}
          </div>
          <div className={`p-3 rounded-xl ${statusColors[status]}`}>
            <Icon className="w-5 h-5" />
          </div>
        </div>
      </div>
    </motion.div>
  );
}
