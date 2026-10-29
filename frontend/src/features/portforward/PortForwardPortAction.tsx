import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { ExternalLink, Square } from 'lucide-react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import { api } from '@/lib/api'
import { BrowserOpenURL } from '@/lib/wails/wailsjs/runtime/runtime'
import { usePortForwards } from '@/store/portForwards'
import { PortForwardDialog } from './PortForwardButton'
import { findForward, forwardURL, isRunning, type ForwardTarget } from './portForwards'

type Props = {
  contextName: string | null
  target: ForwardTarget
  port: number
  protocol?: string
}

// PortForwardPortAction is the Forward cell of a ports table: a button to
// start a forward to this port, or the running forward's link and Stop.
export function PortForwardPortAction({ contextName, target, port, protocol }: Props) {
  const forwards = usePortForwards((s) => s.list)
  const [open, setOpen] = useState(false)
  const stop = useMutation({
    mutationFn: (id: string) => api.stopPortForward(id),
    onError: (e) => toast.error(`Stop failed: ${String(e)}`),
  })

  if (protocol && protocol !== 'TCP') {
    return <span className="text-muted-foreground" title="Port-forward supports TCP only">—</span>
  }
  const pf = findForward(forwards, contextName, target, port)
  const running = pf && isRunning(pf)

  return (
    <span className="inline-flex items-center gap-1">
      {running && pf.status === 'active' ? (
        <button
          type="button"
          onClick={() => BrowserOpenURL(forwardURL(pf))}
          title={`Open ${forwardURL(pf)}`}
          className="inline-flex items-center gap-1 font-mono text-primary hover:underline"
        >
          localhost:{pf.boundPort}
          <ExternalLink className="size-3" />
        </button>
      ) : running ? (
        <span className="text-amber-600 dark:text-amber-400" title={pf.error || undefined}>
          {pf.status === 'starting' ? 'Starting…' : 'Reconnecting…'}
        </span>
      ) : (
        <Button
          size="xs"
          variant="outline"
          onClick={() => setOpen(true)}
          title={pf?.error || undefined}
          aria-label={`Forward port ${port}`}
        >
          Forward
        </Button>
      )}
      {running && (
        <Button
          size="icon-xs"
          variant="ghost"
          aria-label={`Stop forwarding port ${port}`}
          title="Stop port-forward"
          disabled={stop.isPending}
          onClick={() => stop.mutate(pf.id)}
        >
          <Square />
        </Button>
      )}
      <PortForwardDialog
        contextName={contextName}
        resource={{ kind: target.kind, namespace: target.namespace, name: target.name }}
        initialPort={port}
        open={open}
        onOpenChange={setOpen}
      />
    </span>
  )
}
