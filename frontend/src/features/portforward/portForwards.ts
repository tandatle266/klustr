import type { PortForwardInfo } from '@/lib/api'

export type ForwardKind = 'Pod' | 'Service'

export type ForwardTarget = {
  kind: ForwardKind
  namespace: string
  name: string
}

export type ForwardProtocol = 'http' | 'https'

export function isForwardable(kind: string): kind is ForwardKind {
  return kind === 'Pod' || kind === 'Service'
}

// A running forward has a supervisor: it is listening, connecting, or
// retrying after losing its pod.
export function isRunning(pf: PortForwardInfo): boolean {
  return pf.status === 'active' || pf.status === 'starting' || pf.status === 'reconnecting'
}

export function forwardURL(pf: PortForwardInfo): string {
  return `${pf.protocol === 'https' ? 'https' : 'http'}://localhost:${pf.boundPort}`
}

export function findForward(
  list: PortForwardInfo[],
  contextName: string | null,
  target: ForwardTarget,
  port: number,
): PortForwardInfo | undefined {
  return list.find(
    (pf) =>
      pf.context === contextName &&
      pf.kind === target.kind &&
      pf.namespace === target.namespace &&
      pf.name === target.name &&
      pf.port === port,
  )
}

// Local ports already claimed by other forwards, running or saved, so a
// suggestion does not collide with one that resumes later.
export function claimedLocalPorts(list: PortForwardInfo[]): Set<number> {
  const used = new Set<number>()
  for (const pf of list) {
    if (pf.localPort) used.add(pf.localPort)
    if (pf.boundPort) used.add(pf.boundPort)
  }
  return used
}

export function suggestLocalPort(remote: number, used: Set<number>): number {
  const candidates: number[] = []
  if (remote >= 1024) candidates.push(remote)
  candidates.push(remote + 8000)
  candidates.push(remote + 10000)
  for (const p of candidates) {
    if (p >= 1024 && p <= 65535 && !used.has(p)) return p
  }
  for (let p = 30000; p < 60000; p++) {
    if (!used.has(p)) return p
  }
  return 0
}

export const STATUS_LABEL: Record<string, string> = {
  active: 'Active',
  starting: 'Starting',
  reconnecting: 'Reconnecting',
  idle: 'Waiting for context',
  error: 'Error',
  stopped: 'Stopped',
}

export const STATUS_CLASS: Record<string, string> = {
  active: 'text-emerald-600 dark:text-emerald-400',
  starting: 'text-muted-foreground',
  reconnecting: 'text-amber-600 dark:text-amber-400',
  idle: 'text-muted-foreground',
  error: 'text-destructive',
  stopped: 'text-muted-foreground',
}
