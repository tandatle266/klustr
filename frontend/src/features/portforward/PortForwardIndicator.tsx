import { useMutation } from '@tanstack/react-query'
import { ExternalLink, Network, Settings2, Square, Unplug } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { api } from '@/lib/api'
import { BrowserOpenURL } from '@/lib/wails/wailsjs/runtime/runtime'
import { usePortForwards } from '@/store/portForwards'
import { useUIStore } from '@/store/ui'
import { forwardURL, isRunning, STATUS_CLASS, STATUS_LABEL } from './portForwards'

export function PortForwardIndicator() {
  const all = usePortForwards((s) => s.list)
  const list = all.filter(isRunning)
  const setSelectedView = useUIStore((s) => s.setSelectedView)
  const stop = useMutation({
    mutationFn: async (id: string) => {
      await api.stopPortForward(id)
    },
  })
  const stopAll = useMutation({
    mutationFn: async (ids: string[]) => {
      await Promise.all(ids.map((id) => api.stopPortForward(id)))
    },
  })

  const active = list.length > 0
  const hasError = all.some((pf) => pf.status === 'error' || pf.status === 'reconnecting')
  const busy = stop.isPending || stopAll.isPending

  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button variant="ghost" size="sm" aria-label="Port-forwards">
          <Network
            className={
              hasError ? 'text-destructive' : active ? 'text-emerald-500' : 'text-muted-foreground'
            }
          />
          <span className="text-xs">
            {active ? `${list.length} forward${list.length === 1 ? '' : 's'}` : 'No forwards'}
          </span>
        </Button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-96 p-0">
        <div className="flex items-center justify-between gap-2 border-b border-border px-3 py-2">
          <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Running port-forwards
          </span>
          {list.length > 1 && (
            <Button
              size="xs"
              variant="ghost"
              disabled={busy}
              onClick={() => stopAll.mutate(list.map((pf) => pf.id))}
              className="-my-1 h-auto gap-1 py-1 text-destructive hover:text-destructive"
            >
              <Unplug />
              Stop all
            </Button>
          )}
        </div>
        {!active && (
          <div className="px-3 py-4 text-xs text-muted-foreground">
            No running port-forwards. Start one from the Ports table of a Pod or Service.
          </div>
        )}
        <ul className="max-h-80 overflow-auto">
          {list.map((pf) => (
            <li key={pf.id} className="flex items-center gap-2 border-b border-border px-3 py-2 text-xs last:border-b-0">
              <button
                type="button"
                disabled={pf.status !== 'active'}
                onClick={() => BrowserOpenURL(forwardURL(pf))}
                title={pf.status === 'active' ? `Open ${forwardURL(pf)} in browser` : undefined}
                className="group flex min-w-0 flex-1 flex-col items-start gap-0.5 text-left enabled:hover:text-primary"
              >
                <span className="flex w-full min-w-0 items-center gap-1 font-mono">
                  <span className="min-w-0 flex-1 truncate">
                    localhost:{pf.boundPort || pf.localPort || '…'} → {pf.kind === 'Pod' ? 'pod' : 'svc'}/{pf.name}:{pf.port}
                  </span>
                  <ExternalLink className="size-3 shrink-0 opacity-0 transition-opacity group-enabled:group-hover:opacity-100" />
                </span>
                <span className="max-w-full truncate text-[10px] text-muted-foreground">
                  {pf.namespace} · {pf.context}
                  {pf.status !== 'active' && (
                    <span className={`ml-1 ${STATUS_CLASS[pf.status] ?? ''}`}>· {STATUS_LABEL[pf.status] ?? pf.status}</span>
                  )}
                </span>
                {pf.error && (
                  <span className="max-w-full truncate text-[10px] text-destructive" title={pf.error}>
                    {pf.error}
                  </span>
                )}
              </button>
              <Button
                size="icon-xs"
                variant="ghost"
                aria-label="Stop port-forward"
                disabled={busy}
                onClick={() => stop.mutate(pf.id)}
                className="shrink-0"
              >
                <Square />
              </Button>
            </li>
          ))}
        </ul>
        <div className="border-t border-border px-1 py-1">
          <Button
            size="xs"
            variant="ghost"
            className="w-full justify-start"
            onClick={() => setSelectedView('portforwards')}
          >
            <Settings2 />
            Manage port-forwards
            {all.length > list.length && (
              <span className="ml-auto text-[10px] text-muted-foreground">{all.length - list.length} stopped</span>
            )}
          </Button>
        </div>
      </PopoverContent>
    </Popover>
  )
}
