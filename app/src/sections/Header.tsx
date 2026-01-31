import { motion } from 'framer-motion';
import { Database, Activity } from 'lucide-react';
import { StatusBadge } from '@/components/StatusBadge';

export function Header() {
  return (
    <motion.header
      initial={{ opacity: 0, y: -20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.6, ease: [0.4, 0, 0.2, 1] }}
      className="sticky top-0 z-50 w-full border-b border-border/50 bg-background/80 backdrop-blur-xl"
    >
      <div className="flex h-16 items-center justify-between px-6">
        <div className="flex items-center gap-4">
          <div className="flex items-center justify-center w-10 h-10 rounded-xl bg-gradient-to-br from-sky-400 to-blue-500 shadow-lg shadow-sky-500/25">
            <Database className="w-5 h-5 text-white" />
          </div>
          <div>
            <h1 className="text-xl font-bold text-gradient">ETL 控制台</h1>
            <p className="text-xs text-muted-foreground">数据同步管理系统</p>
          </div>
        </div>
        
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2 px-4 py-2 rounded-lg bg-card border border-border/50">
            <Activity className="w-4 h-4 text-emerald-400" />
            <span className="text-sm text-muted-foreground">系统状态</span>
            <StatusBadge status="success">在线</StatusBadge>
          </div>
        </div>
      </div>
    </motion.header>
  );
}
