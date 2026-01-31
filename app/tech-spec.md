# ETL 控制台 - 技术规划文档

---

## 1. 组件清单

### shadcn/ui 组件
| 组件 | 用途 |
|------|------|
| Card | 各功能区块容器 |
| Button | 操作按钮 |
| Input | 文本输入框 |
| Select | 下拉选择器 |
| Label | 表单标签 |
| Badge | 状态标签 |
| Table | 任务列表、执行记录 |
| Alert | 错误信息展示 |
| Separator | 分隔线 |
| Tooltip | 提示信息 |
| Skeleton | 加载占位 |

### 自定义组件
| 组件 | 用途 |
|------|------|
| StatusCard | 仪表盘状态卡片 |
| StatusBadge | 带动画的状态标签 |
| AnimatedNumber | 数字滚动动画 |
| GradientButton | 渐变背景按钮 |
| FormRow | 表单行布局 |
| TaskTable | 任务列表表格 |

---

## 2. 动画实现规划

| 动画 | 库 | 实现方式 | 复杂度 |
|------|-----|---------|--------|
| 页面入场动画 | Framer Motion | AnimatePresence + motion.div | 中 |
| 卡片悬停效果 | Framer Motion | whileHover | 低 |
| 数字滚动 | Framer Motion | useSpring + useMotionValue | 中 |
| 状态脉冲 | CSS | @keyframes pulse | 低 |
| 进度条动画 | Framer Motion | animate width | 低 |
| 表格行悬停 | CSS/Tailwind | hover:bg | 低 |
| 按钮点击反馈 | Framer Motion | whileTap | 低 |
| 输入框聚焦 | CSS | focus:ring | 低 |
| 滚动触发 | Framer Motion | whileInView | 中 |

---

## 3. 项目结构

```
app/
├── src/
│   ├── components/
│   │   ├── ui/              # shadcn/ui 组件
│   │   ├── StatusCard.tsx   # 状态卡片
│   │   ├── StatusBadge.tsx  # 状态标签
│   │   ├── GradientButton.tsx
│   │   └── TaskTable.tsx
│   ├── sections/
│   │   ├── Header.tsx       # 页面头部
│   │   ├── Dashboard.tsx    # 仪表盘
│   │   ├── ODSStatus.tsx    # ODS执行状态
│   │   ├── IndicatorSync.tsx # 技术指标同步
│   │   ├── ManualTrigger.tsx # 手动触发
│   │   ├── ScheduledTasks.tsx # 定时任务
│   │   ├── TaskHistory.tsx  # 任务执行历史
│   │   └── DataBrowser.tsx  # 数据浏览
│   ├── hooks/
│   │   └── useAnimatedNumber.ts
│   ├── lib/
│   │   └── utils.ts
│   ├── App.tsx
│   ├── App.css
│   └── main.tsx
├── public/
├── index.html
├── tailwind.config.js
└── package.json
```

---

## 4. 依赖清单

### 核心依赖
- React 18
- TypeScript
- Vite
- Tailwind CSS
- shadcn/ui

### 动画库
- framer-motion

### 图标
- lucide-react

---

## 5. 颜色配置

```javascript
// tailwind.config.js 扩展
colors: {
  background: '#0f172a',
  foreground: '#f8fafc',
  card: '#1e293b',
  'card-hover': '#334155',
  muted: '#94a3b8',
  accent: '#38bdf8',
  success: '#22c55e',
  warning: '#f59e0b',
  error: '#ef4444',
}
```

---

## 6. 开发顺序

1. 初始化项目
2. 配置 Tailwind 颜色主题
3. 创建基础组件（StatusCard, StatusBadge, GradientButton）
4. 开发 Header 区块
5. 开发 Dashboard 区块
6. 开发 ODSStatus 区块
7. 开发 IndicatorSync 区块
8. 开发 ManualTrigger 区块
9. 开发 ScheduledTasks 区块
10. 开发 TaskHistory 区块
11. 开发 DataBrowser 区块
12. 整合 App.tsx
13. 添加动画效果
14. 构建测试
15. 部署
