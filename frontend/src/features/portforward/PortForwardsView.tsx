import { useMemo, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { ExternalLink, Pencil, Play, Search as SearchIcon, Square, Trash2 } from 'lucide-react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { api, type PortForwardInfo } from '@/lib/api'
import { BrowserOpenURL } from '@/lib/wails/wailsjs/runtime/runtime'
import { usePortForwards } from '@/store/portForwards'
import { forwardURL, isRunning, STATUS_CLASS, STATUS_LABEL, type ForwardProtocol } from './portForwards'

function matches(pf: PortForwardInfo, query: string): boolean {
  if (!query) return true
  const q = query.toLowerCase()
  return [pf.name, pf.namespace, pf.context, pf.podName, String(pf.port), String(pf.boundPort || pf.localPort)].some((v) =>
    v.toLowerCase().includes(q),
  )
}

// PortForwardsView lists every saved forward across contexts. Forwards are
// saved by Klustr, not read from a cluster, so the view works with no
// context connected; one whose context is not connected waits as idle.
export function PortForwardsView() {
  const forwards = usePortForwards((s) => s.list)
  const [query, setQuery] = useState('')
  const [editing, setEditing] = useState<PortForwardInfo | null>(null)
  const shown = useMemo(() => forwards.filter((pf) => matches(pf, query)), [forwards, query])

  const action = useMutation({
    mutationFn: async ({ kind, pf }: { kind: 'start' | 'stop' | 'remove'; pf: PortForwardInfo }) => {
      if (kind === 'start') return api.startPortForward(pf.id).then(() => undefined)
      if (kind === 'stop') return api.stopPortForward(pf.id)
      return api.removePortForward(pf.id)
    },
    onError: (e) => toast.error(String(e)),
  })

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex items-center gap-3 border-b border-border px-4 py-2">
        <h2 className="text-sm font-medium">Port Forwarding</h2>
        <div className="flex min-w-0 flex-1 items-center gap-2 rounded border border-border px-2 py-1">
          <SearchIcon className="size-3 shrink-0 text-muted-foreground" />
          <input
            type="text"
            spellCheck={false}
            autoCorrect="off"
            autoCapitalize="off"
            aria-label="Filter port-forwards"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Filter by name, namespace, context or port"
            className="min-w-0 flex-1 bg-transparent text-xs outline-none"
          />
        </div>
        <span className="text-[10px] text-muted-foreground">
          {forwards.filter(isRunning).length} running · {forwards.length} saved
        </span>
      </div>
      {forwards.length === 0 ? (
        <div className="p-6 text-xs text-muted-foreground">
          No port-forwards. Start one from the Ports table of a Pod or Service, or from its row menu.
          Forwards are saved and resume when their context connects.
        </div>
      ) : (
        <div className="min-h-0 flex-1 overflow-auto">
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-muted/60 text-left text-muted-foreground backdrop-blur">
              <tr>
                <th className="px-3 py-1.5 font-medium">Name</th>
                <th className="px-3 py-1.5 font-medium">Namespace</th>
                <th className="px-3 py-1.5 font-medium">Context</th>
                <th className="px-3 py-1.5 font-medium">Port</th>
                <th className="px-3 py-1.5 font-medium">Local</th>
                <th className="px-3 py-1.5 font-medium">Status</th>
                <th className="px-3 py-1.5 font-medium sr-only">Actions</th>
              </tr>
            </thead>
            <tbody>
              {shown.map((pf) => {
                const running = isRunning(pf)
                const label = `${pf.kind.toLowerCase()}/${pf.name}:${pf.port}`
                return (
                  <tr key={pf.id} className="border-t border-border align-top">
                    <td className="px-3 py-1.5">
                      <div className="font-mono">
                        <span className="text-muted-foreground">{pf.kind === 'Pod' ? 'pod/' : 'svc/'}</span>
                        {pf.name}
                      </div>
                      {pf.podName && pf.kind === 'Service' && (
                        <div className="text-[10px] text-muted-foreground">via {pf.podName}</div>
                      )}
                    </td>
                    <td className="px-3 py-1.5">{pf.namespace}</td>
                    <td className="px-3 py-1.5 text-muted-foreground">{pf.context}</td>
                    <td className="px-3 py-1.5 font-mono">
                      {pf.port}
                      {pf.targetPort > 0 && pf.targetPort !== pf.port && (
                        <span className="text-muted-foreground"> → {pf.targetPort}</span>
                      )}
                    </td>
                    <td className="px-3 py-1.5 font-mono">
                      {pf.status === 'active' ? (
                        <button
                          type="button"
                          onClick={() => BrowserOpenURL(forwardURL(pf))}
                          className="inline-flex items-center gap-1 text-primary hover:underline"
                          title={`Open ${forwardURL(pf)}`}
                        >
                          {pf.protocol}://localhost:{pf.boundPort}
                          <ExternalLink className="size-3" />
                        </button>
                      ) : (
                        <span className="text-muted-foreground">
                          {pf.protocol}://localhost:{pf.localPort || 'auto'}
                        </span>
                      )}
                    </td>
                    <td className="px-3 py-1.5">
                      <div className={STATUS_CLASS[pf.status] ?? 'text-muted-foreground'}>
                        {STATUS_LABEL[pf.status] ?? pf.status}
                      </div>
                      {pf.error && (
                        <div className="max-w-80 break-words text-[10px] text-destructive" title={pf.error}>
                          {pf.error}
                        </div>
                      )}
                    </td>
                    <td className="px-3 py-1.5">
                      <div className="flex justify-end gap-1">
                        {running ? (
                          <Button
                            size="icon-xs"
                            variant="ghost"
                            aria-label={`Stop ${label}`}
                            title="Stop"
                            disabled={action.isPending}
                            onClick={() => action.mutate({ kind: 'stop', pf })}
                          >
                            <Square />
                          </Button>
                        ) : (
                          <Button
                            size="icon-xs"
                            variant="ghost"
                            aria-label={`Start ${label}`}
                            title="Start"
                            disabled={action.isPending}
                            onClick={() => action.mutate({ kind: 'start', pf })}
                          >
                            <Play />
                          </Button>
                        )}
                        <Button
                          size="icon-xs"
                          variant="ghost"
                          aria-label={`Edit ${label}`}
                          title="Edit"
                          onClick={() => setEditing(pf)}
                        >
                          <Pencil />
                        </Button>
                        <Button
                          size="icon-xs"
                          variant="ghost"
                          aria-label={`Delete ${label}`}
                          title="Delete"
                          disabled={action.isPending}
                          onClick={() => action.mutate({ kind: 'remove', pf })}
                          className="hover:text-destructive"
                        >
                          <Trash2 />
                        </Button>
                      </div>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
      {editing && <EditPortForwardDialog forward={editing} onClose={() => setEditing(null)} />}
    </div>
  )
}

function EditPortForwardDialog({ forward, onClose }: { forward: PortForwardInfo; onClose: () => void }) {
  const [localPort, setLocalPort] = useState(forward.localPort)
  const [protocol, setProtocol] = useState<ForwardProtocol>(forward.protocol === 'https' ? 'https' : 'http')
  const save = useMutation({
    mutationFn: () => api.editPortForward(forward.id, localPort, protocol),
    onSuccess: onClose,
  })
  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="sm:max-w-sm" onOpenAutoFocus={(e) => e.preventDefault()}>
        <DialogHeader>
          <DialogTitle>Edit port-forward</DialogTitle>
          <DialogDescription asChild>
            <div className="space-y-2">
              <p>
                <span className="font-mono text-xs">
                  {forward.kind === 'Pod' ? 'pod' : 'svc'}/{forward.name}:{forward.port}
                </span>{' '}
                in <span className="font-mono text-xs">{forward.namespace}</span>. A running forward
                restarts on the new local port.
              </p>
              {save.error && (
                <p className="rounded border border-destructive/40 bg-destructive/10 p-2 font-mono text-xs text-destructive break-words">
                  {String(save.error)}
                </p>
              )}
            </div>
          </DialogDescription>
        </DialogHeader>
        <div className="flex items-center gap-3">
          <label htmlFor="pf-edit-local" className="w-24 text-sm text-muted-foreground">Local port</label>
          <input
            id="pf-edit-local"
            type="number"
            min={0}
            max={65535}
            value={localPort}
            onChange={(e) => setLocalPort(Math.min(65535, Math.max(0, Number.parseInt(e.target.value, 10) || 0)))}
            className="w-28 rounded border border-border bg-background px-2 py-1 text-sm"
          />
        </div>
        <div className="flex items-center gap-3">
          <span className="w-24 text-sm text-muted-foreground">Open as</span>
          <div className="inline-flex rounded border border-border p-0.5" role="group" aria-label="Protocol">
            {(['http', 'https'] as const).map((p) => (
              <button
                key={p}
                type="button"
                aria-pressed={protocol === p}
                onClick={() => setProtocol(p)}
                className={[
                  'rounded px-2 py-0.5 font-mono text-xs',
                  protocol === p ? 'bg-primary text-primary-foreground' : 'text-foreground hover:bg-muted',
                ].join(' ')}
              >
                {p}
              </button>
            ))}
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose} disabled={save.isPending}>
            Cancel
          </Button>
          <Button onClick={() => save.mutate()} disabled={save.isPending}>
            {save.isPending ? 'Saving…' : 'Save'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
