export function SiteDisclaimer({ className = '' }: { className?: string }) {
  return (
    <div className={`text-center text-[9px] sm:text-[10px] text-[#949ba5]/45 tracking-wide ${className}`.trim()}>
      Not financial advice. For educational purposes only.
    </div>
  )
}
