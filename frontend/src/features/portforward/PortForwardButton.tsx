import { useEffect, useMemo, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { Network } from 'lucide-react'
import { toast } from 'sonner'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { api } from '@/lib/api'
import { usePortForwards } from '@/store/portForwards'
import type { SelectedResource } from '@/store/ui'
import { BrowserOpenURL } from '@/lib/wails/wailsjs/runtime/runtime'
import {
  claimedLocalPorts,
  findForward,
  forwardURL,
  isForwardable,
  suggestLocalPort,
  type ForwardProtocol,
  type ForwardTarget,
} from './portForwards'

type PortOption = { port: number; label: string; hint: string }

// Port-forward tunnels TCP only, so UDP and SCTP ports are not offered.
function isTCP(protocol: string): boolean {
  return !protocol || protocol === 'TCP'
}

function usePortOptions(contextName: string | null, target: ForwardTarget, open: boolean): PortOption[] {
  const [options, setOptions] = useState<{ key: string; list: PortOption[] } | null>(null)
  const key = JSON.stringify([contextName, target.kind, target.namespace, target.name])
  useEffect(() => {
    if (!open || !contextName) return
    let cancelled = false
    const load =
      target.kind === 'Pod'
        ? api.getPod(contextName, target.namespace, target.name).then((d) =>
            d.containers.flatMap((c) =>
              (c.ports ?? [])
                .filter((p) => isTCP(p.protocol))
                .map((p) => ({ port: p.containerPort, label: p.name ? `${p.name} ${p.containerPort}` : String(p.containerPort), hint: c.name })),
            ),
          )
        : api.getService(contextName, target.namespace, target.name).then((d) =>
            d.ports
              .filter((p) => isTCP(p.protocol))
              .map((p) => ({ port: p.port, label: p.name ? `${p.name} ${p.port}` : String(p.port), hint: `→ ${p.targetPort}` })),
          )
    load
      .then((list) => {
        if (!cancelled) setOptions({ key, list })
      })
      .catch(() => {
        /* fall back to manual entry */
      })
    return () => {
      cancelled = true
    }
  }, [open, contextName, target.kind, target.namespace, target.name, key])
  return options?.key === key ? options.list : []
}

type DialogProps = {
  contextName: string | null
  resource: SelectedResource
  // Preselects a port, as the per-port Forward links do.
  initialPort?: number
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function PortForwardDialog({ contextName, resource, initialPort, open, onOpenChange }: DialogProps) {
  // Mounted only while open, so each open starts from the target's current
  // forward instead of the last session's edits.
  if (!open || !isForwardable(resource.kind)) return null
  return (
    <PortForwardDialogBody
      contextName={contextName}
      target={{ kind: resource.kind, namespace: resource.namespace, name: resource.name }}
      initialPort={initialPort}
      open={open}
      onOpenChange={onOpenChange}
    />
  )
}

function PortForwardDialogBody({
  contextName,
  target,
  initialPort,
  open,
  onOpenChange,
}: Omit<DialogProps, 'resource'> & { target: ForwardTarget }) {
  const forwards = usePortForwards((s) => s.list)
  const options = usePortOptions(contextName, target, open)
  const [pickedPort, setPickedPort] = useState<number | null>(initialPort ?? null)
  const port = pickedPort ?? options[0]?.port ?? 80
  const existing = findForward(forwards, contextName, target, port)
  const [localOverride, setLocalOverride] = useState<number | null>(null)
  const [protocolOverride, setProtocolOverride] = useState<ForwardProtocol | null>(null)
  const [openInBrowser, setOpenInBrowser] = useState(true)

  // Ports claimed by other forwards; this target's own saved port stays
  // available so reopening the dialog suggests it again.
  const used = useMemo(
    () => claimedLocalPorts(forwards.filter((pf) => pf.id !== existing?.id)),
    [forwards, existing?.id],
  )
  const localPort = localOverride ?? (existing ? existing.localPort || existing.boundPort : suggestLocalPort(port, used))
  const protocol: ForwardProtocol =
    protocolOverride ?? (existing?.protocol === 'https' || port === 443 || port === 8443 ? 'https' : 'http')

  const start = useMutation({
    mutationFn: async () => {
      if (!contextName) throw new Error('no context')
      return api.addPortForward(contextName, target.namespace, target.kind, target.name, port, localPort, protocol)
    },
    onSuccess: (info) => {
      toast.success(`Forwarding localhost:${info.boundPort} → ${target.kind.toLowerCase()}/${target.name}:${info.port}`)
      if (openInBrowser) BrowserOpenURL(forwardURL(info))
      onOpenChange(false)
    },
  })

  const kindLabel = target.kind === 'Pod' ? 'pod' : 'svc'

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md" onOpenAutoFocus={(e) => e.preventDefault()}>
        <DialogHeader>
          <DialogTitle>Port-forward</DialogTitle>
          <DialogDescription asChild>
            <div className="space-y-2">
              <p>
                Forward a port of{' '}
                <span className="font-mono text-xs">{kindLabel}/{target.name}</span> in{' '}
                <span className="font-mono text-xs">{target.namespace}</span> to your machine. Set
                the local port to <span className="font-mono text-xs">0</span> to let the OS pick
                one.
                {target.kind === 'Service' && ' Traffic goes to one running pod behind the service, and moves to another if that pod goes away.'}
              </p>
              {start.error && (
                <p className="rounded border border-destructive/40 bg-destructive/10 p-2 font-mono text-xs text-destructive break-words">
                  {String(start.error)}
                </p>
              )}
            </div>
          </DialogDescription>
        </DialogHeader>

        {options.length > 0 && (
          <div className="space-y-1">
            <div className="text-xs text-muted-foreground">{target.kind === 'Pod' ? 'Container ports' : 'Service ports'}</div>
            <div className="flex flex-wrap gap-1.5">
              {options.map((o, i) => (
                <button
                  key={`${o.port}-${i}`}
                  type="button"
                  onClick={() => {
                    setPickedPort(o.port)
                    setLocalOverride(null)
                    setProtocolOverride(null)
                  }}
                  className={[
                    'rounded border px-2 py-0.5 font-mono text-xs',
                    port === o.port
                      ? 'border-primary bg-primary text-primary-foreground'
                      : 'border-border bg-background text-foreground hover:bg-muted',
                  ].join(' ')}
                >
                  {o.label}
                  <span className="ml-1 text-[10px] opacity-70">{o.hint}</span>
                </button>
              ))}
            </div>
          </div>
        )}

        <div className="flex items-center gap-3">
          <label htmlFor="pf-local-port" className="w-28 text-sm text-muted-foreground">Local port</label>
          <input
            id="pf-local-port"
            type="number"
            spellCheck={false}
            autoCorrect="off"
            autoCapitalize="off"
            min={0}
            max={65535}
            value={localPort}
            onChange={(e) => setLocalOverride(Math.min(65535, Math.max(0, Number.parseInt(e.target.value, 10) || 0)))}
            className="w-28 rounded border border-border bg-background px-2 py-1 text-sm"
          />
        </div>
        <div className="flex items-center gap-3">
          <label htmlFor="pf-remote-port" className="w-28 text-sm text-muted-foreground">
            {target.kind === 'Pod' ? 'Container port' : 'Service port'}
          </label>
          <input
            id="pf-remote-port"
            type="number"
            spellCheck={false}
            autoCorrect="off"
            autoCapitalize="off"
            min={1}
            max={65535}
            value={port}
            onChange={(e) => {
              setPickedPort(Math.min(65535, Math.max(1, Number.parseInt(e.target.value, 10) || 1)))
              setLocalOverride(null)
            }}
            className="w-28 rounded border border-border bg-background px-2 py-1 text-sm"
          />
        </div>
        <div className="flex items-center gap-3">
          <span className="w-28 text-sm text-muted-foreground">Open as</span>
          <div className="inline-flex rounded border border-border p-0.5" role="group" aria-label="Protocol">
            {(['http', 'https'] as const).map((p) => (
              <button
                key={p}
                type="button"
                aria-pressed={protocol === p}
                onClick={() => setProtocolOverride(p)}
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
        <label className="flex cursor-pointer items-center gap-2 text-sm text-foreground">
          <Checkbox checked={openInBrowser} onChange={(e) => setOpenInBrowser(e.target.checked)} />
          Open in browser after starting
        </label>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={start.isPending}>
            Cancel
          </Button>
          <Button onClick={() => start.mutate()} disabled={start.isPending}>
            {start.isPending ? 'Starting…' : 'Start'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

type ButtonProps = {
  contextName: string | null
  resource: SelectedResource
}

export function PortForwardButton({ contextName, resource }: ButtonProps) {
  const [open, setOpen] = useState(false)

  return (
    <>
      <Tooltip>
        <TooltipTrigger asChild>
          <Button size="xs" variant="outline" onClick={() => setOpen(true)}>
            <Network />
            Forward
          </Button>
        </TooltipTrigger>
        <TooltipContent>Forward a port to your machine</TooltipContent>
      </Tooltip>
      <PortForwardDialog contextName={contextName} resource={resource} open={open} onOpenChange={setOpen} />
    </>
  )
}
