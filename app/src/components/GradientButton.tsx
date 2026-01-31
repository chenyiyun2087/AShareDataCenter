import { motion } from 'framer-motion';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { Loader2 } from 'lucide-react';

interface GradientButtonProps {
  children: React.ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  loading?: boolean;
  variant?: 'primary' | 'success' | 'danger';
  size?: 'sm' | 'md' | 'lg';
  className?: string;
  type?: 'button' | 'submit' | 'reset';
}

const variantStyles = {
  primary: 'from-sky-400 to-blue-500 hover:from-sky-300 hover:to-blue-400 shadow-sky-500/25',
  success: 'from-emerald-400 to-emerald-500 hover:from-emerald-300 hover:to-emerald-400 shadow-emerald-500/25',
  danger: 'from-red-400 to-red-500 hover:from-red-300 hover:to-red-400 shadow-red-500/25',
};

const sizeStyles = {
  sm: 'px-4 py-2 text-xs',
  md: 'px-6 py-2.5 text-sm',
  lg: 'px-8 py-3 text-base',
};

export function GradientButton({
  children,
  onClick,
  disabled = false,
  loading = false,
  variant = 'primary',
  size = 'md',
  className,
  type = 'button',
}: GradientButtonProps) {
  return (
    <motion.div
      whileHover={{ scale: disabled || loading ? 1 : 1.02 }}
      whileTap={{ scale: disabled || loading ? 1 : 0.98 }}
    >
      <Button
        type={type}
        onClick={onClick}
        disabled={disabled || loading}
        className={cn(
          'relative overflow-hidden font-semibold text-white',
          'bg-gradient-to-r transition-all duration-300',
          'shadow-lg hover:shadow-xl hover:brightness-110',
          variantStyles[variant],
          sizeStyles[size],
          (disabled || loading) && 'opacity-60 cursor-not-allowed',
          className
        )}
      >
        {loading && (
          <Loader2 className="w-4 h-4 mr-2 animate-spin" />
        )}
        {children}
      </Button>
    </motion.div>
  );
}
